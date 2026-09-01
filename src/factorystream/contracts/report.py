"""Print the contract compatibility matrix.

    python -m factorystream.contracts.report

Exits 0 always. This is a *report*, not a gate: a breaking change between
versions is a fact about the contract's history, not a defect - v1 -> v2 broke
compatibility on purpose, and that is the condition the pipeline exists to
handle. The gate is `producer --contract-check-only`, which checks live data
against the version it declares.
"""

from __future__ import annotations

from factorystream.contracts.registry import report


def main() -> int:
    print(report())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
