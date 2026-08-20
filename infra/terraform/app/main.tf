# FactoryStream's own resources: the lake bucket and the Glue catalog.
#
# Account-level things (budgets, CUR, OIDC, Athena workgroup) live in
# ../account/ and are shared with WearWatch. This module reads what it needs
# from there by data source rather than duplicating it.

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "factorystream"
      ManagedBy   = "terraform"
      Environment = "portfolio"
      Module      = "app"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  lake_bucket = "factorystream-lake-${local.account_id}"

  # Bronze partitions on INGEST time, not event time.
  #
  # This matters and is easy to get backwards. The disorder injector produces
  # events whose publish_time trails event_time by up to six hours — partition
  # bronze by event time and a late arrival has to be written into a partition
  # that was closed hours ago, which breaks both immutability and the
  # idempotent object naming that makes replay safe.
  #
  # Partitioning by arrival keeps bronze append-only and replay-safe. Silver
  # re-keys to event time, which is where that logic belongs.
  bronze_partitions = [
    { name = "dt", type = "string" },
    { name = "hr", type = "string" },
  ]
}
