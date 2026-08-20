variable "region" {
  description = "Primary region for project resources."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for shared account-level resources."
  type        = string
  default     = "plant-platform"
}

variable "budget_notification_email" {
  description = <<-EOT
    Where budget alarms go. Required — a budget that notifies nobody is
    decoration, and the whole reason this module is applied first is so the
    alarm exists before the spend does.
  EOT
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.budget_notification_email))
    error_message = "budget_notification_email must be a valid email address."
  }
}

variable "monthly_budget_usd" {
  description = <<-EOT
    Monthly spend that triggers alarms. Deliberately well under the credit
    balance: the point is to hear about an unexpected charge in week one, not
    to discover it when the credits run out.
  EOT
  type        = number
  default     = 25
}

variable "credit_budget_usd" {
  description = "Total credit budget across the whole plan window."
  type        = number
  default     = 100
}

variable "github_owner" {
  description = "GitHub account or org that CI runs under."
  type        = string
  default     = "Steve27M"
}

variable "github_repos" {
  description = <<-EOT
    Repositories permitted to assume the deploy role via OIDC. Scoped by
    repo AND ref — a wildcard here would let any fork or any branch of any
    repo in the org assume the role, which is the whole failure mode OIDC is
    meant to remove.
  EOT
  type        = list(string)
  default     = ["factorystream", "wearwatch"]
}

variable "athena_bytes_scanned_cutoff" {
  description = <<-EOT
    Per-query bytes-scanned cap for the shared Athena workgroup, in bytes.
    Athena bills per TB scanned, so an accidental unpartitioned full scan is
    the single most likely way to turn a $5 month into a $60 one. 2 GiB is far
    above anything a partitioned query on this data should need, and far below
    anything that costs real money.
  EOT
  type        = number
  default     = 2 * 1024 * 1024 * 1024
}
