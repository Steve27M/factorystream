# The lake bucket. One bucket, three prefixes: bronze/, quarantine/, manifests/.

resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket

  # The teardown/rebuild acceptance test is `destroy && apply && replay`. A
  # bucket that survives destroy would make that test a lie, and the data is
  # regenerable from a seed anyway.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id

  versioning_configuration {
    # Deliberately off. Idempotent object naming means a replayed batch
    # overwrites itself by design — with versioning on, every replay would
    # silently accumulate a new version and pay storage for it forever. The
    # replay-safety guarantee and object versioning want opposite things.
    status = "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-bronze"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    expiration {
      days = var.bronze_retention_days
    }
  }

  rule {
    id     = "expire-quarantine"
    status = "Enabled"

    filter {
      prefix = "quarantine/"
    }

    expiration {
      days = var.bronze_retention_days
    }
  }

  # manifests/ is deliberately NOT expired here. It is the ground truth the
  # completeness ledger is judged against; losing it retroactively would make
  # every historical reconciliation unverifiable.

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    # A failed multipart upload bills storage forever and is invisible in the
    # console. The single cheapest lifecycle rule anyone can add.
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}
