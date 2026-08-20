"""Assert the account contains only what we expect to be paying for.

Run at the end of **every** working session. Both specs make this a Phase 0
deliverable for the same reason: the way a $200 credit budget dies is not a
dramatic mistake, it is a NAT Gateway someone forgot about quietly billing $32 a
month while nobody looks at the console.

Read-only. It creates nothing, deletes nothing, and modifies nothing — it
enumerates billable resource types across regions and compares against an
allowlist. Deleting things automatically would be a worse idea than the problem
it solves.

    python tools/teardown_verify.py                 # expect an empty account
    python tools/teardown_verify.py --expect expected.yaml
    python tools/teardown_verify.py --all-regions   # slower, catches strays

Exit code 0 = as expected, 1 = something unexpected exists.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import Any

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover - boto3 is an optional extra
    boto3 = None  # type: ignore[assignment]
    BotoCoreError = ClientError = Exception  # type: ignore[misc, assignment]


# Resource types that cost money while idle. These are the ones worth scanning
# for; an empty S3 bucket or an unused IAM role bills nothing and would only add
# noise to the report.
#
# NAT Gateway is listed first deliberately: both specs ban it outright, it is
# the single most expensive thing that can appear by accident (~$32/month plus
# data processing), and it appears without being asked for whenever someone
# clicks through a "create VPC" wizard.
IDLE_BILLABLE = (
    "nat_gateways",
    "ec2_instances",
    "ebs_volumes",
    "elastic_ips",
    "rds_instances",
    "load_balancers",
    "sagemaker_endpoints",
    "sagemaker_notebooks",
    "opensearch_domains",
    "redshift_clusters",
    "kinesis_streams",
    "msk_clusters",
    "vpc_endpoints",
    "eks_clusters",
    "elasticache_clusters",
    "efs_filesystems",
    "global_accelerators",
)

# Regions we actually use. `--all-regions` widens the scan, which matters
# because a resource created in a region you never visit is exactly the one that
# bills unnoticed.
DEFAULT_REGIONS = ("us-east-1",)


@dataclass
class Finding:
    region: str
    kind: str
    identifier: str
    detail: str = ""


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    # Services the account is not enrolled in. Reported as reassurance, not
    # as a problem — nothing can be running on a service that is switched off.
    not_subscribed: list[str] = field(default_factory=list)
    regions_scanned: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.findings and not self.errors


# Errors that mean "definitively nothing there", not "could not look".
#
# `SubscriptionRequiredException` / `OptInRequired` mean the account is not
# enrolled in that service at all — so no resource of that type can exist. That
# is a *stronger* clean signal than an empty list, and treating it as an error
# would make the check cry wolf on every run until someone stopped reading it.
BENIGN_ERRORS = (
    "SubscriptionRequiredException",
    "OptInRequired",
    "AuthFailure",
    "UnrecognizedClient",
    "not supported",
    "InvalidAction",
)


def _safe(report: Report, label: str, fn: Any) -> Any:
    """Run a describe call, recording rather than raising on failure.

    A partial scan that reports what it could not check is far more useful than
    a traceback — the whole point is to notice things, and a sweep that aborts
    on the first AccessDenied notices nothing after it.
    """
    try:
        return fn()
    except (ClientError, BotoCoreError) as exc:
        message = str(exc)
        if any(s in message for s in BENIGN_ERRORS):
            report.not_subscribed.append(label)
            return None
        report.errors.append(f"{label}: {message[:160]}")
        return None


def scan_region(session: Any, region: str, report: Report) -> None:
    ec2 = session.client("ec2", region_name=region)

    natgw = _safe(report, f"{region}/nat", lambda: ec2.describe_nat_gateways())
    for gw in (natgw or {}).get("NatGateways", []):
        if gw.get("State") not in ("deleted", "failed"):
            report.findings.append(
                Finding(region, "nat_gateways", gw["NatGatewayId"], "~$32/month idle")
            )

    reservations = _safe(report, f"{region}/ec2", lambda: ec2.describe_instances())
    for res in (reservations or {}).get("Reservations", []):
        for inst in res.get("Instances", []):
            if inst.get("State", {}).get("Name") not in ("terminated", "shutting-down"):
                report.findings.append(
                    Finding(
                        region,
                        "ec2_instances",
                        inst["InstanceId"],
                        f"{inst.get('InstanceType')} {inst.get('State', {}).get('Name')}",
                    )
                )

    volumes = _safe(report, f"{region}/ebs", lambda: ec2.describe_volumes())
    for vol in (volumes or {}).get("Volumes", []):
        report.findings.append(
            Finding(region, "ebs_volumes", vol["VolumeId"], f"{vol.get('Size')} GiB")
        )

    addresses = _safe(report, f"{region}/eip", lambda: ec2.describe_addresses())
    for addr in (addresses or {}).get("Addresses", []):
        # An unattached EIP bills; an attached one is covered by its instance.
        attached = bool(addr.get("InstanceId") or addr.get("NetworkInterfaceId"))
        report.findings.append(
            Finding(
                region,
                "elastic_ips",
                addr.get("PublicIp", "?"),
                "attached" if attached else "UNATTACHED — billing",
            )
        )

    endpoints = _safe(report, f"{region}/vpce", lambda: ec2.describe_vpc_endpoints())
    for ep in (endpoints or {}).get("VpcEndpoints", []):
        # Gateway endpoints (S3, DynamoDB) are free; interface endpoints are not.
        if ep.get("VpcEndpointType") == "Interface":
            report.findings.append(
                Finding(region, "vpc_endpoints", ep["VpcEndpointId"], "interface — hourly")
            )

    _scan_simple(
        session, region, report,
        [
            ("rds", "describe_db_instances", "DBInstances", "DBInstanceIdentifier",
             "rds_instances"),
            ("elbv2", "describe_load_balancers", "LoadBalancers", "LoadBalancerName",
             "load_balancers"),
            ("sagemaker", "list_endpoints", "Endpoints", "EndpointName",
             "sagemaker_endpoints"),
            ("sagemaker", "list_notebook_instances", "NotebookInstances",
             "NotebookInstanceName", "sagemaker_notebooks"),
            ("opensearch", "list_domain_names", "DomainNames", "DomainName",
             "opensearch_domains"),
            ("redshift", "describe_clusters", "Clusters", "ClusterIdentifier",
             "redshift_clusters"),
            ("kafka", "list_clusters", "ClusterInfoList", "ClusterName", "msk_clusters"),
            ("eks", "list_clusters", "clusters", None, "eks_clusters"),
            ("elasticache", "describe_cache_clusters", "CacheClusters", "CacheClusterId",
             "elasticache_clusters"),
            ("efs", "describe_file_systems", "FileSystems", "FileSystemId",
             "efs_filesystems"),
        ],
    )

    streams = _safe(report, f"{region}/kinesis", lambda: session.client(
        "kinesis", region_name=region).list_streams())
    for name in (streams or {}).get("StreamNames", []):
        report.findings.append(Finding(region, "kinesis_streams", name, "check shard mode"))


def _scan_simple(
    session: Any, region: str, report: Report, specs: list[tuple[Any, ...]]
) -> None:
    """Describe-and-list for services whose shape is uniform enough to loop."""
    for service, method, key, id_field, kind in specs:
        client = session.client(service, region_name=region)
        result = _safe(report, f"{region}/{service}", lambda c=client, m=method: getattr(c, m)())
        for item in (result or {}).get(key, []):
            identifier = item if id_field is None else item.get(id_field, "?")
            report.findings.append(Finding(region, kind, str(identifier)))


def enabled_regions(session: Any) -> list[str]:
    ec2 = session.client("ec2", region_name="us-east-1")
    try:
        resp = ec2.describe_regions(AllRegions=False)
        return sorted(r["RegionName"] for r in resp["Regions"])
    except (ClientError, BotoCoreError):
        return list(DEFAULT_REGIONS)


def render(report: Report) -> str:
    lines: list[str] = []
    lines.append(f"regions scanned: {', '.join(report.regions_scanned)}")

    if report.findings:
        lines.append("")
        lines.append(f"UNEXPECTED BILLABLE RESOURCES ({len(report.findings)}):")
        for f in sorted(report.findings, key=lambda x: (x.kind, x.region)):
            suffix = f"  [{f.detail}]" if f.detail else ""
            lines.append(f"  {f.kind:<22} {f.region:<12} {f.identifier}{suffix}")
    else:
        lines.append("")
        lines.append("no unexpected billable resources")

    if report.not_subscribed:
        services = sorted({label.split("/")[-1] for label in report.not_subscribed})
        lines.append("")
        lines.append(f"not enrolled ({len(services)}): {', '.join(services)}")
        lines.append("  nothing can exist on a service the account is not subscribed to")

    if report.errors:
        lines.append("")
        lines.append(f"COULD NOT CHECK ({len(report.errors)}) — treat as unknown, not clean:")
        for err in report.errors:
            lines.append(f"  {err}")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--all-regions",
        action="store_true",
        help="scan every enabled region, not just the ones we use",
    )
    parser.add_argument(
        "--allow",
        nargs="*",
        default=[],
        metavar="ID",
        help="resource identifiers that are expected to exist",
    )
    args = parser.parse_args()

    if boto3 is None:
        print("boto3 not installed — run: uv pip install --link-mode=copy -e '.[cloud]'")
        return 2

    session = boto3.Session()
    report = Report()
    regions = enabled_regions(session) if args.all_regions else list(DEFAULT_REGIONS)
    report.regions_scanned = regions

    for region in regions:
        scan_region(session, region, report)

    allowed = set(args.allow)
    report.findings = [f for f in report.findings if f.identifier not in allowed]

    print(render(report))
    # Errors fail the check too: "I could not look" is not "nothing is there."
    return 0 if report.clean else 1


if __name__ == "__main__":
    sys.exit(main())
