#!/usr/bin/env python3
"""Discover and compare AWS service coverage between LocalStack community, Moto, and Robotocore.

Usage:
    uv run python scripts/discover_services.py
"""

import importlib
import json
import os
import sys

# LocalStack community tier services (from their docs)
LOCALSTACK_COMMUNITY_SERVICES = {
    "acm",
    "apigateway",
    "cloudformation",
    "cloudwatch",
    "config",
    "dynamodb",
    "dynamodbstreams",
    "ec2",
    "es",
    "events",
    "firehose",
    "iam",
    "kinesis",
    "kms",
    "lambda",
    "logs",
    "opensearch",
    "redshift",
    "resource-groups",
    "resourcegroupstaggingapi",
    "route53",
    "route53resolver",
    "s3",
    "s3control",
    "scheduler",
    "secretsmanager",
    "ses",
    "sns",
    "sqs",
    "ssm",
    "stepfunctions",
    "sts",
    "support",
    "swf",
    "transcribe",
}


def get_moto_services() -> set[str]:
    """Get the list of services Moto supports."""
    try:
        from moto.backends import BACKENDS

        return set(BACKENDS.keys())
    except ImportError:
        # Try reading from vendor submodule
        backend_index = os.path.join(
            os.path.dirname(__file__), "..", "vendor", "moto", "moto", "backend_index.py"
        )
        if os.path.exists(backend_index):
            # Parse the backend_index to extract service names
            services = set()
            with open(backend_index) as f:
                content = f.read()
                # Look for the backend_url_patterns dict
                for line in content.splitlines():
                    line = line.strip()
                    if line.startswith('"') and '": ' in line:
                        service = line.split('"')[1]
                        services.add(service)
            return services
    return set()


def main():
    moto_services = get_moto_services()

    print("=" * 70)
    print("AWS Service Coverage Report")
    print("=" * 70)

    print(f"\nLocalStack Community services: {len(LOCALSTACK_COMMUNITY_SERVICES)}")
    print(f"Moto services:                {len(moto_services)}")

    covered = LOCALSTACK_COMMUNITY_SERVICES & moto_services
    gaps = LOCALSTACK_COMMUNITY_SERVICES - moto_services

    print(f"\nLocalStack services covered by Moto: {len(covered)}/{len(LOCALSTACK_COMMUNITY_SERVICES)}")

    if gaps:
        print(f"\nGaps (in LocalStack but not in Moto):")
        for s in sorted(gaps):
            print(f"  - {s}")
    else:
        print("\nNo gaps! Moto covers all LocalStack community services.")

    print(f"\nAdditional Moto services (not in LocalStack community):")
    extra = moto_services - LOCALSTACK_COMMUNITY_SERVICES
    for s in sorted(extra):
        print(f"  + {s}")


if __name__ == "__main__":
    main()
