"""Upload the Glue job code to S3 and create or update the Glue job.

    python scripts/deploy_glue_job.py --bucket my-lake --role-arn arn:aws:iam::123:role/glue-job
    python scripts/deploy_glue_job.py --dry-run   # print the job definition only

Reads LAKE_BUCKET, GLUE_JOB_NAME, GLUE_ROLE_ARN and AWS_REGION from the
environment when the flags are not given. Terraform for the AWS side is a
stretch goal; until then this script is the single place the job is defined.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
JOB_SCRIPT = REPO / "glue" / "jobs" / "bronze_to_silver.py"
LIB_DIR = REPO / "glue" / "silver_lib"


def build_lib_zip(target: Path) -> Path:
    """Zip silver_lib so Glue can import it through --extra-py-files."""
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in sorted(LIB_DIR.rglob("*.py")):
            zf.write(file, file.relative_to(LIB_DIR.parent).as_posix())
    return target


def job_definition(args: argparse.Namespace) -> dict:
    base = f"s3://{args.bucket}"
    return {
        "Role": args.role_arn,
        "Description": "Home Credit raw CSV to silver Parquet (schema, types, quarantine)",
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": f"{base}/glue/scripts/bronze_to_silver.py",
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--job-bookmark-option": "job-bookmark-enable",
            "--extra-py-files": f"{base}/glue/libs/silver_lib.zip",
            "--raw_root": f"{base}/raw",
            "--silver_root": f"{base}/silver",
            "--quarantine_root": f"{base}/quarantine",
            "--TempDir": f"{base}/glue/temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-observability-metrics": "true",
        },
        "GlueVersion": args.glue_version,
        "WorkerType": args.worker_type,
        "NumberOfWorkers": args.workers,
        # Flex runs on spare capacity: cheaper per DPU-hour, may start later.
        # Fine for a daily batch that nobody waits on.
        "ExecutionClass": "FLEX" if args.flex else "STANDARD",
        "Timeout": args.timeout_minutes,
        "MaxRetries": 0,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
    }


def deploy(args: argparse.Namespace) -> None:
    import boto3

    session = boto3.Session(region_name=args.region)
    s3 = session.client("s3")
    glue = session.client("glue")

    with tempfile.TemporaryDirectory() as tmp:
        lib_zip = build_lib_zip(Path(tmp) / "silver_lib.zip")
        s3.upload_file(str(JOB_SCRIPT), args.bucket, "glue/scripts/bronze_to_silver.py")
        s3.upload_file(str(lib_zip), args.bucket, "glue/libs/silver_lib.zip")
    print(f"Uploaded job script and silver_lib.zip to s3://{args.bucket}/glue/")

    definition = job_definition(args)
    try:
        glue.get_job(JobName=args.job_name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(Name=args.job_name, **definition)
        print(f"Created Glue job {args.job_name}")
    else:
        glue.update_job(JobName=args.job_name, JobUpdate=definition)
        print(f"Updated Glue job {args.job_name}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--bucket", default=os.getenv("LAKE_BUCKET", "<BUCKET>"))
    p.add_argument("--job-name", default=os.getenv("GLUE_JOB_NAME", "credit_risk_bronze_to_silver"))
    p.add_argument("--role-arn", default=os.getenv("GLUE_ROLE_ARN", "<GLUE_ROLE_ARN>"))
    p.add_argument("--region", default=os.getenv("AWS_REGION"))
    p.add_argument("--glue-version", default="5.1")
    p.add_argument("--worker-type", default="G.1X")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--timeout-minutes", type=int, default=30)
    p.add_argument("--no-flex", dest="flex", action="store_false")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.dry_run:
        print(json.dumps({"Name": args.job_name, **job_definition(args)}, indent=2))
        return 0
    if "<" in args.bucket or "<" in args.role_arn:
        print("Set --bucket and --role-arn (or LAKE_BUCKET and GLUE_ROLE_ARN).", file=sys.stderr)
        return 2
    deploy(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
