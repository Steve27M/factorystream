# Account-level foundation. Applied ONCE, shared by FactoryStream and WearWatch.
#
# Everything here is account-scoped rather than project-scoped: there can only
# be one GitHub OIDC provider per account, budgets and CUR are account-wide, and
# duplicating any of it per project would mean two things fighting over one
# resource.
#
# Project resources live in their own modules and reference this one by data
# source. See ../app/ (FactoryStream) and ../../../wearwatch/terraform/.
#
# ORDERING MATTERS: apply this before any billable resource exists anywhere. A
# budget alarm added after the spend is a postmortem, and CUR only records from
# the moment it is switched on — every day it is off is billing evidence that
# will not exist later.

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # State stays local and gitignored to start. It contains resource identifiers
  # and is not worth an S3 backend for a single operator on one machine — the
  # remote-backend upgrade is a decisions entry when a second machine appears,
  # not speculative work now.
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "plant-platform"
      ManagedBy   = "terraform"
      Environment = "portfolio"
      # Makes an orphaned resource traceable to the module that made it, which
      # is the difference between a five-minute cleanup and an archaeology dig.
      Module = "account"
    }
  }
}

# Cost Explorer, Budgets and CUR are only available in us-east-1 regardless of
# where the data lives.
provider "aws" {
  alias  = "billing"
  region = "us-east-1"

  default_tags {
    tags = {
      Project   = "plant-platform"
      ManagedBy = "terraform"
      Module    = "account"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # Bucket names are globally unique across all of AWS, so the account id is
  # appended rather than hoping "plant-platform-cur" is free.
  cur_bucket_name = "${var.name_prefix}-cur-${local.account_id}"
}
