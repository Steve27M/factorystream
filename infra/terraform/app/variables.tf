variable "region" {
  description = "Must match the account module."
  type        = string
  default     = "us-east-1"
}

variable "glue_database" {
  description = "Glue catalog database for the lakehouse."
  type        = string
  default     = "factorystream"
}

variable "bronze_retention_days" {
  description = <<-EOT
    Days before bronze and quarantine objects expire.

    Short on purpose. Every byte is synthetic and regenerable from a seed, so
    there is nothing here worth paying to keep — and an unbounded lake on a
    time-boxed credit budget is how storage cost sneaks up. Manifests are
    exempt: they are the ground truth the ledger is judged against.
  EOT
  type        = number
  default     = 30
}
