# Budgets — the first thing that exists, before anything that can spend.
#
# Names are prefixed rather than generic because the account already carries a
# signup-created "My Monthly Cost Budget" at $10. Colliding with it would either
# fail the apply or, worse, silently adopt and rewrite something Terraform does
# not own.

# Monthly actual spend. The everyday guardrail.
resource "aws_budgets_budget" "monthly" {
  provider = aws.billing

  name         = "${var.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Credits mask real usage: with $199 of credit applied, "actual spend" reads
  # near zero right up until the credits run out and the bill becomes real.
  # Excluding them means these alarms track what the workload genuinely costs,
  # which is the number the README publishes and the number that matters after
  # the plan ends.
  cost_types {
    include_credit             = false
    include_refund             = false
    include_discount           = true
    include_other_subscription = true
    include_recurring          = true
    include_subscription       = true
    include_support            = true
    include_tax                = true
    include_upfront            = true
    use_amortized              = false
    use_blended                = false
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 90
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }

  # Forecast, not actual. This is the one that catches a NAT Gateway on day two
  # rather than on day twenty-eight — by the time *actual* crosses 90%, the
  # month is already spent.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_notification_email]
  }
}

# Whole-plan credit budget. The monthly alarm cannot see a slow burn that never
# trips a single month but still empties the balance.
resource "aws_budgets_budget" "credit_total" {
  provider = aws.billing

  name         = "${var.name_prefix}-credit-total"
  budget_type  = "COST"
  limit_amount = tostring(var.credit_budget_usd)
  limit_unit   = "USD"
  time_unit    = "ANNUALLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }
}
