# Pipe1 Data Pipeline – Cloud Run Jobs + GCS Notebook Execution  
*(Final, corrected edition)*

This pipeline executes a sequence of Jupyter notebooks stored in Google Cloud Storage using a Cloud Run Job that runs Spark + Papermill inside a Docker container.  
It performs ETL using Spark (reading from GCS) and writes final tables to BigQuery via the Python BigQuery client.

---

## 1. Architecture Overview

The pipeline runs five notebooks located in:

```
gs://<bucket>/pipe1/notebooks/
```

Executed versions are written to:

```
gs://<bucket>/pipe1/executed/
```

### Execution Flow
1. Cloud Run Job starts container  
2. Container downloads notebooks from GCS  
3. Each notebook is executed using Papermill  
4. Spark reads CSV files from GCS using the GCS connector  
5. Notebooks write results to BigQuery via Python BigQuery Client  
6. Executed notebooks are uploaded to GCS  
7. Job exits successfully

---

## 2. Files Inside the Docker Image

```
Dockerfile
requirements.txt
run_pipeline1.py
```

Notebooks are not baked into the image—they are downloaded from GCS.

---

## 3. Python Dependencies

Your requirements.txt must contain:

```
pyspark
papermill
google-cloud-storage
google-cloud-bigquery
db-dtypes
nbformat
nbclient
pandas
numpy
pyarrow
ipykernel
```

---

## 4. Environment Variables

Passed into Cloud Run Job:

```
PIPELINE_NOTEBOOK_BUCKET=<gcs-bucket>
PIPELINE_NOTEBOOK_PREFIX=pipe1/notebooks
PIPELINE_EXECUTED_PREFIX=pipe1/executed
```

Defined inside the notebooks:

```
project_id = "dejadsgl"
bq_dataset = "netflix"
```

---

## 5. IAM Requirements

The Cloud Run Job service account must have:

```
roles/storage.objectAdmin
roles/artifactregistry.reader
roles/bigquery.dataEditor
roles/bigquery.jobUser
roles/bigquery.user
```

---

## 6. Deployment Variables

```
PROJECT_ID="dejadsgl"
REGION="us-central1"
REPO="de-pipelines"
IMAGE="us-central1-docker.pkg.dev/$PROJECT_ID/$REPO/pipeline1:latest"
SA="pipe1-job-sa@$PROJECT_ID.iam.gserviceaccount.com"
```

---

## 7. Create Artifact Registry Repo

```
gcloud artifacts repositories create de-pipelines     --repository-format=docker     --location=us-central1
```

---

## 8. Build & Push Image

```
sudo gcloud auth configure-docker us-central1-docker.pkg.dev

sudo docker build -t $IMAGE .
sudo docker push $IMAGE
```

---

## 9. Deploy Cloud Run Job

```
gcloud run jobs create pipe1-job   --image=$IMAGE   --region=$REGION   --service-account=$SA   --cpu=2   --memory=4Gi   --max-retries=1   --task-timeout=3600s   --set-env-vars PIPELINE_NOTEBOOK_BUCKET=$PIPELINE_NOTEBOOK_BUCKET   --set-env-vars PIPELINE_NOTEBOOK_PREFIX=$PIPELINE_NOTEBOOK_PREFIX   --set-env-vars PIPELINE_EXECUTED_PREFIX=$PIPELINE_EXECUTED_PREFIX
```

---

## 10. Execute Pipeline

```
gcloud run jobs executions run pipe1-job --region=us-central1
```

---

## 11. Scheduling (Cloud Scheduler)

```
0 3 * * *
```

---

## 12. Updating the Pipeline

### Notebook-only changes  
→ Upload new notebook to GCS  
→ No rebuild needed  

### Code or dependency changes  
→ Rebuild container  
→ Push new image  
→ Update Cloud Run Job

---

## 13. Spark & BigQuery Integration

Spark does not use a BigQuery connector.  
The container includes only the GCS connector.

BigQuery is accessed via:

- google-cloud-bigquery  
- db-dtypes  
- load_table_from_dataframe()  
- list_rows().to_dataframe()

Spark is used only for transformations.

---

## 14. Static External IP for VM

```
gcloud compute addresses create my-static-ip --region=us-central1
gcloud compute addresses describe my-static-ip --region=us-central1 --format="get(address)"
```

---

## 15. Final Notes

- Pipeline is fully working end‑to‑end  
- BigQuery operations rely on Python, not Spark  
- Updating notebooks requires no rebuild  
- Updating code requires rebuild + redeploy  

---

# 16. Troubleshooting Summary / Lessons Learned

During development several issues were encountered, primarily around Spark compatibility, dependency conflicts, and Cloud Run execution. These adjustments were required to achieve a stable, fully working pipeline:

### 1. Removed Spark → BigQuery connector
All versions of the Spark BigQuery connector caused Java/Scala incompatibility errors (NoClassDefFoundError, NoSuchMethodError, ServiceConfigurationError).  
The connector was removed entirely. Spark now uses **only the GCS connector**, not a BigQuery connector.

### 2. Replaced all BigQuery I/O with the Python BigQuery Client
All Spark-based BigQuery reads/writes:

```
spark.read.format("bigquery")
df.write.format("bigquery")
```

were replaced with:

- `bq_client.list_rows(...).to_dataframe()`  
- `spark.createDataFrame(...)`  
- `bq_client.load_table_from_dataframe(...)`

This eliminated all connector issues and made BigQuery I/O stable.

### 3. Updated Python dependencies
To avoid runtime crashes, the following libraries were required:

```
pyspark
google-cloud-bigquery
db-dtypes
pyarrow
papermill
pandas
```

`db-dtypes` fixed the BigQuery → pandas conversion error (`NO_DB_TYPES_ERROR`).

### 4. Stabilized Docker image with only Spark + GCS connector
The final Docker image contains:

- Java  
- PySpark  
- Papermill  
- The GCS Hadoop connector  
- No BigQuery Spark connector  

This ensures Spark reliably reads `gs://` paths without conflicts.

### 5. Corrected GCP configuration (region, repo, IAM roles)
- Region standardized to **us-central1**  
- Artifact Registry repository: **de-pipelines**  
- Service account roles fixed:  
  - storage.objectAdmin  
  - artifactregistry.reader  
  - bigquery.dataEditor  
  - bigquery.jobUser  
  - bigquery.user  

This resolved image pull errors, GCS access errors, and BigQuery permission failures.

---

✔ **Result:**  
The pipeline now runs fully end‑to‑end in Cloud Run Jobs, reading notebooks from GCS, executing Spark transformations, and writing all tables to BigQuery using the Python client.
