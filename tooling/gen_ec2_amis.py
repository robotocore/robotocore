"""Generate the amazon-owned AMI roster the EC2 backend serves.

Mirrors moto's `_load_amis`: resources/amis.json.gz for every region plus
resources/latest_amis/<region>.json.gz and resources/ecs/optimized_amis/<region>.json.gz
(owner_alias=amazon). Only us-east-1 gets the regional extras (that is the
only region the compat suites use); other regions serve the base list.

Regenerate with:
    uv run python tooling/gen_ec2_amis.py
"""

import gzip
import json
import pathlib
import sys

RESOURCES = (
    pathlib.Path(sys.argv[1])
    if len(sys.argv) > 1
    else pathlib.Path(
        "/home/termag/termag/projects/robotocore/.venv/lib/python3.13/site-packages/moto/ec2/resources"
    )
)
OUT = pathlib.Path("rotocore/src/services/ec2/amis/amis-us-east-1.json")


def load(path: pathlib.Path) -> list[dict]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as fh:
        return json.load(fh)


def main() -> int:
    rows = []
    for ami in load(RESOURCES / "amis.json.gz"):
        ami.setdefault("owner_alias", "amazon")
        rows.append(ami)
    us = {"ami_id" for _ in ()}
    for sub in ("latest_amis/us-east-1.json", "ecs/optimized_amis/us-east-1.json"):
        for ami in load(RESOURCES / f"{sub}.gz"):
            ami.setdefault("owner_alias", "amazon")
            ami.setdefault("owner_id", "801119661308")
            ami.setdefault("hypervisor", "xen")
            ami.setdefault("state", "available")
            ami.setdefault("public", True)
            ami.setdefault("image_type", "machine")
            ami.setdefault("root_device_type", "ebs")
            ami.setdefault("root_device_name", "/dev/sda1")
            ami.setdefault("platform", "linux")
            ami.setdefault("architecture", "x86_64")
            ami.setdefault("virtualization_type", "hvm")
            ami.setdefault("sriov", "simple")
            ami.setdefault("usage_operation", "RunInstances")
            ami.setdefault("name", "")
            ami.setdefault("description", "")
            ami.setdefault("image_location", "")
            rows.append(ami)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(rows))
    print(f"wrote {len(rows)} amis -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
