# Free Edition S3 access test

**Question:** can a Unity Catalog external location on Databricks Free Edition read my own S3 bucket?

The answer decides how Databricks gets Glue's silver Parquet:

| Result | `SILVER_ACCESS_MODE` | What happens |
|---|---|---|
| Yes | `external_location` | dbt reads `s3://<bucket>/silver/` directly |
| No | `volume` | Airflow copies each new silver partition into the Unity Catalog Volume `<catalog>/landing/silver` with the Databricks SDK, and dbt reads from there |

The Free Edition limitations page does not list external locations either way, and community forum answers say custom external locations are not available. Run the test anyway so the README can report what actually happened.

Time needed: about 20 minutes. Cost: zero (one small file in S3).

## Files

- `trust_policy_initial.json`: IAM trust policy with the placeholder external ID `0000`
- `trust_policy_final.json`: trust policy after Databricks gives you the real external ID (also trusts itself, which AWS now requires)
- `permissions_policy.json`: S3 access for the role, scoped to the lake bucket
- `probe.sql`: the SQL to run in the Databricks SQL editor

## Steps

### 0. Catalog check

Open the SQL editor in Databricks and run the Step 0 block of `probe.sql`.

- `CREATE CATALOG` works: keep `DATABRICKS_CATALOG=credit_risk`.
- It fails: use the built-in `workspace` catalog. Set `DATABRICKS_CATALOG=workspace` in `.env` (read by the local dbt profile) and deploy the bundle with `--var catalog=workspace` (the Databricks job passes it to dbt as the target catalog). Replace `credit_risk` with `workspace` in the rest of `probe.sql`.

### 1. Put a probe file in the bucket

```bash
aws s3 cp tests/fixtures/home_credit/bureau.csv s3://<BUCKET>/silver/_probe/bureau.csv
```

### 2. Create the IAM role

1. IAM, Roles, Create role, Custom trust policy. Paste `trust_policy_initial.json`.
2. Name it `credit-risk-uc-silver-reader`. Skip permissions for now and create it.
3. Create a policy from `permissions_policy.json` (fill in `<BUCKET>` and `<AWS-ACCOUNT-ID>`) and attach it to the role.

### 3. Create the storage credential in Databricks

1. In **Catalog**, open **Connect**, then **Credentials**, then **Create credential**.
   - If there is no way to create a credential, stop here: the answer is **No**. Go to "Record the result".
2. Credential type **AWS IAM Role**, name `credit_risk_s3`, role ARN `arn:aws:iam::<AWS-ACCOUNT-ID>:role/credit-risk-uc-silver-reader`.
3. Copy the **External ID** from the confirmation dialog.
4. Back in IAM, replace the role's trust policy with `trust_policy_final.json`, filling in the account id and the external ID.
5. In Databricks, open the credential and click **Validate Configuration**. Fix any failed check before moving on.

### 4. Create the external location

In **Catalog**, open **Connect**, then **External Locations**, then **Create external location** (Manual):

- Name `credit_risk_silver`, storage type S3, URL `s3://<BUCKET>/silver/`, credential `credit_risk_s3`.

Or in SQL:

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS credit_risk_silver
URL 's3://<BUCKET>/silver/'
WITH (STORAGE CREDENTIAL credit_risk_s3);
```

### 5. Read the probe file

Run the `LIST` and `read_files` blocks of `probe.sql`. Seeing the bureau rows means the answer is **Yes**.

### 6. Check the fallback either way

Run the Volume block of `probe.sql` after uploading `bureau.csv` to the Volume in Catalog Explorer. This confirms the fallback path works before the pipeline depends on it.

## Record the result

Fill this in, set `SILVER_ACCESS_MODE` in `.env`, and copy the outcome into the main README under "Decision log".

```
Date tested: 2026-10-20
Catalog creation allowed (yes/no): yes
Storage credential creation available (yes/no): yes
External location created (yes/no): yes
read_files on s3:// worked (yes/no): yes
Volume fallback worked (yes/no): yes
Error messages (verbatim): none
Chosen SILVER_ACCESS_MODE: external_location
```

Notes: the test was run with the Databricks SDK instead of the UI. The credential was created with `skip_validation=True`, then the trust policy was updated with the real external ID and the validation call returned PASS for READ, LIST, WRITE, DELETE and PATH_EXISTS on the first try. The Unity Catalog IAM ARN returned by Databricks matched the one hardcoded in `trust_policy_initial.json`. `LIST` and `read_files` on `s3://<bucket>/silver/_probe/` returned the 7 probe rows. `LIST` on the Volume `/Volumes/credit_risk/landing/silver/` also worked. The probe file was deleted afterwards, and the IAM role, storage credential and external location were kept.

## Clean up

Delete the probe file (`aws s3 rm s3://<BUCKET>/silver/_probe/ --recursive`). Keep the IAM role only if the answer was Yes.
