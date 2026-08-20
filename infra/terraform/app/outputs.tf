output "lake_bucket" {
  description = "One bucket, three prefixes: bronze/, quarantine/, manifests/."
  value       = aws_s3_bucket.lake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.main.name
}

output "bronze_location" {
  value = "s3://${aws_s3_bucket.lake.bucket}/bronze/"
}

output "quarantine_location" {
  value = "s3://${aws_s3_bucket.lake.bucket}/quarantine/"
}

output "manifests_location" {
  value = "s3://${aws_s3_bucket.lake.bucket}/manifests/"
}

output "evidence_summary" {
  description = "Paste into evidence/ after apply."
  value       = <<-EOT
    lake_bucket    = ${aws_s3_bucket.lake.bucket}
    glue_database  = ${aws_glue_catalog_database.main.name}
    tables         = bronze_events, quarantine, manifests
    partitioning   = bronze dt/hr on INGEST time (append-only, replay-safe)
    projection     = enabled on all three (no MSCK REPAIR, no silent empty results)
    retention      = bronze/quarantine ${var.bronze_retention_days}d; manifests never expire
  EOT
}
