# Cost and Usage Report — the raw evidence source for the cost postmortem.
#
# CUR only records from the moment it is enabled. There is no backfill. Every
# day it is off is billing detail that will not exist later, which is why this
# is in the first module applied rather than added when the postmortem is
# written and the data is needed.

resource "aws_s3_bucket" "cur" {
  bucket = local.cur_bucket_name

  # This bucket is disposable evidence storage, not a data product. Letting
  # terraform destroy it keeps the teardown honest — a bucket that survives
  # `destroy` is a resource the acceptance test silently does not cover.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "cur" {
  bucket = aws_s3_bucket.cur.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cur" {
  bucket = aws_s3_bucket.cur.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "cur" {
  bucket = aws_s3_bucket.cur.id

  rule {
    id     = "expire-cur"
    status = "Enabled"

    filter {}

    # Longer than the plan window, so nothing expires before the postmortem is
    # written, and short enough that it cannot quietly accumulate cost after.
    expiration {
      days = 180
    }
  }
}

# CUR writes as the billing service, not as us, so the bucket policy has to
# grant it explicitly. Conditions pin the source account and ARN — without them
# this is a confused-deputy opening, however unlikely.
data "aws_iam_policy_document" "cur_bucket" {
  statement {
    sid    = "AllowBillingReportsGetAcl"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["billingreports.amazonaws.com"]
    }

    actions   = ["s3:GetBucketAcl", "s3:GetBucketPolicy"]
    resources = [aws_s3_bucket.cur.arn]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }

  statement {
    sid    = "AllowBillingReportsPutObject"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["billingreports.amazonaws.com"]
    }

    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.cur.arn}/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_s3_bucket_policy" "cur" {
  bucket = aws_s3_bucket.cur.id
  policy = data.aws_iam_policy_document.cur_bucket.json
}

resource "aws_cur_report_definition" "main" {
  provider = aws.billing

  report_name = "${var.name_prefix}-cur"
  time_unit   = "DAILY"
  format      = "Parquet"
  compression = "Parquet"

  # RESOURCES gives per-resource-id detail. Without it the report says "you
  # spent $4 on S3" rather than "this bucket cost $4", and the postmortem
  # cannot attribute spend to a project.
  additional_schema_elements = ["RESOURCES"]

  s3_bucket = aws_s3_bucket.cur.id
  s3_prefix = "cur"
  s3_region = "us-east-1"

  additional_artifacts   = ["ATHENA"]
  report_versioning      = "OVERWRITE_REPORT"
  refresh_closed_reports = true

  depends_on = [aws_s3_bucket_policy.cur]
}
