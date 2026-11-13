# Documentation

Start with the [project README](../README.md) for the architecture, the data and the known
limitations. These pages go deeper:

| Page | Covers |
|---|---|
| [design-decisions.md](design-decisions.md) | Why each layer works the way it does: replayer slicing, Glue bookmarks and quarantine, how silver reaches Databricks, dbt keys/SCD2/galaxy schema, data quality and orphan policy, orchestration, plus the dated decision log |
| [running.md](running.md) | Full setup on AWS + Databricks + Airflow, running the Glue job against S3 without managed Glue, running real data through dbt without AWS, and cost notes |
| [testing.md](testing.md) | Every test layer, how to run it, and what has actually been run against real services |

Component guides live next to their code:

| Guide | Covers |
|---|---|
| [glue/README.md](../glue/README.md) | What the Glue job does per table, and how to run its Spark tests |
| [dbt/README.md](../dbt/README.md) | Schemas, the gold layer, and how a Databricks job run works |
| [databricks/s3_access_test/README.md](../databricks/s3_access_test/README.md) | Runbook for the Free Edition S3 access question |
