# Shared Athena workgroup, with the cost guardrail that matters most.
#
# Athena bills per terabyte scanned. The failure mode is not a slow drip — it is
# one unpartitioned `SELECT *` against a large table turning a $5 month into a
# $60 one in a single query. The bytes-scanned cutoff below makes that query
# fail instead of succeed, which is the correct outcome.

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.name_prefix}-athena-results-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    # Query results are scratch. Keeping them costs storage and proves nothing;
    # a week is more than enough to debug a failed run.
    expiration {
      days = 7
    }
  }
}

resource "aws_athena_workgroup" "main" {
  name        = var.name_prefix
  description = "Shared workgroup for FactoryStream and WearWatch, with a per-query scan cap."

  # Without this, destroying the workgroup fails while any query history exists.
  force_destroy = true

  configuration {
    # The guardrail. A query that would scan more than this is cancelled.
    bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff

    # Deliberately NOT enforced, and the reason is subtle enough to record.
    #
    # Enforcement forces the workgroup's ResultConfiguration onto every query,
    # which ALSO overrides the `external_location` dbt sets on a CTAS. Every
    # materialised table therefore landed in this results bucket — which
    # carries a 7-day expiration rule, because query results are scratch.
    # Silver, gold and the ledger would have silently vanished after a week,
    # and queries would have started returning nothing with no error.
    #
    # Nothing is lost by disabling it:
    #   * `bytes_scanned_cutoff_per_query` is a workgroup-level limit that no
    #     client can supply, so there is nothing for enforcement to protect.
    #   * Encryption is set on the BUCKET, not just here, so it holds regardless.
    #
    # See docs/decisions.md 23.
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
