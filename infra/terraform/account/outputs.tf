# Consumed by the project modules and pasted into the evidence ledger.

output "account_id" {
  description = "AWS account id. Also the disambiguator in every bucket name."
  value       = local.account_id
}

output "region" {
  value = var.region
}

output "github_deploy_role_arn" {
  description = <<-EOT
    Put this in each repo's GitHub Actions workflow as role-to-assume. It is
    not a secret — it is an ARN, and it is useless without a token from a
    permitted repo and ref.
  EOT
  value       = aws_iam_role.github_deploy.arn
}

output "athena_workgroup" {
  description = "Workgroup name for both projects' dbt profiles."
  value       = aws_athena_workgroup.main.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.bucket
}

output "cur_bucket" {
  description = "Cost and Usage Report destination — the cost postmortem's raw source."
  value       = aws_s3_bucket.cur.bucket
}

output "cur_report_name" {
  value = aws_cur_report_definition.main.report_name
}

output "evidence_summary" {
  description = "One block to paste into evidence/ after apply."
  value       = <<-EOT
    account_id        = ${local.account_id}
    region            = ${var.region}
    deploy_role_arn   = ${aws_iam_role.github_deploy.arn}
    athena_workgroup  = ${aws_athena_workgroup.main.name}
    athena_scan_cap   = ${var.athena_bytes_scanned_cutoff} bytes
    cur_bucket        = ${aws_s3_bucket.cur.bucket}
    budget_monthly    = ${var.monthly_budget_usd} USD (alarms at 50/90% actual, 100% forecast)
    budget_credit     = ${var.credit_budget_usd} USD (alarms at 50/80% actual)
  EOT
}
