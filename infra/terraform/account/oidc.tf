# GitHub OIDC — how CI authenticates without a stored AWS key.
#
# The alternative is an access key pair in GitHub repository secrets, and key
# leakage is the top cloud failure mode. OIDC removes the class of bug entirely:
# GitHub presents a short-lived signed token, AWS verifies it against the
# provider below, and issues credentials that expire in an hour. There is no
# secret to leak, rotate, or forget about.
#
# Both specs bind this. Nothing in either repository should ever contain an AWS
# access key, and the .gitignore files block the shapes anyway as a backstop.

resource "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = ["sts.amazonaws.com"]

  # AWS has verified GitHub's certificate chain natively since 2023, so this
  # list is no longer load-bearing — but the argument is still required, and a
  # stale pinned thumbprint that nobody notices is worse than a placeholder
  # that is documented as unused.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Scoped to specific repos AND specific refs.
    #
    # `repo:owner/*:*` would be the easy version and is a real vulnerability:
    # it lets ANY repository in the account — including one created later, or a
    # pull request from a fork running untrusted code — assume this role. The
    # narrow subject claim is most of the security value of OIDC.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = flatten([
        for repo in var.github_repos : [
          "repo:${var.github_owner}/${repo}:ref:refs/heads/main",
          "repo:${var.github_owner}/${repo}:ref:refs/tags/*",
          "repo:${var.github_owner}/${repo}:environment:production",
        ]
      ])
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name               = "${var.name_prefix}-github-deploy"
  description        = "Assumed by GitHub Actions via OIDC. No long-lived keys exist."
  assume_role_policy = data.aws_iam_policy_document.github_assume.json

  # One hour. CI jobs here are minutes; a longer window is only exposure.
  max_session_duration = 3600
}

# Deliberately broad for now, and deliberately not permanent.
#
# Terraform in CI creates S3 buckets, Glue databases, IAM roles, Athena
# workgroups and SageMaker jobs. Guessing a least-privilege policy up front
# produces a morning of AccessDenied and a policy that gets widened in
# frustration until it is Administrator anyway. Narrowing this from observed
# CloudTrail usage is a genuine Phase 6 exercise with a real before/after.
#
# The scoping that matters today is on WHO can assume the role, above — not on
# what it can do once assumed.
resource "aws_iam_role_policy_attachment" "github_deploy_admin" {
  role       = aws_iam_role.github_deploy.name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

# PowerUserAccess cannot manage IAM, which Terraform needs for project roles.
# Granting IAM narrowly beside it is much better than reaching for
# AdministratorAccess and giving CI the ability to rewrite its own trust policy.
data "aws_iam_policy_document" "github_deploy_iam" {
  statement {
    effect = "Allow"
    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:ListRoles",
      "iam:PassRole",
      "iam:TagRole",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:GetRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:CreatePolicy",
      "iam:DeletePolicy",
      "iam:GetPolicy",
      "iam:GetPolicyVersion",
      "iam:ListPolicyVersions",
    ]
    resources = ["*"]
  }

  # CI must never be able to alter the trust relationship that lets it in, nor
  # touch the account-level provider. Without this, "who can assume the role"
  # is a setting CI could edit — and the careful subject scoping above would be
  # advisory rather than enforced.
  statement {
    effect = "Deny"
    actions = [
      "iam:UpdateAssumeRolePolicy",
      "iam:DeleteOpenIDConnectProvider",
      "iam:UpdateOpenIDConnectProviderThumbprint",
      "iam:CreateUser",
      "iam:CreateAccessKey",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "github_deploy_iam" {
  name   = "${var.name_prefix}-github-deploy-iam"
  role   = aws_iam_role.github_deploy.id
  policy = data.aws_iam_policy_document.github_deploy_iam.json
}
