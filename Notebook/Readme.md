# Pipe1 Data Pipeline – Cloud Run + GCS Notebook Execution (Updated Deployment Guide)

This guide describes how to deploy and run the Pipe1 data pipeline using **Google Cloud Run Jobs**, with all notebooks stored in **Google Cloud Storage (GCS)** instead of inside the container image.

This enables:
- Updating notebooks **without rebuilding** the container image  
- Clean separation between pipeline code and notebook logic  
- Full automation via Cloud Run + Cloud Scheduler  

---

# 1. Pipeline Architecture (GCS-Based Notebooks)

The pipeline executes the following notebooks in sequence:

1. `Pipe1_1_CleanDataset.ipynb`  
2. `Pipe1_2_IntegrationTables.ipynb`  
3. `Pipe1_3_FeatureTables.ipynb`  
4. `Pipe1_4_AggregationsAnalytics.ipynb`  
5. `Pipe1_5_Output_to_Serving_layer.ipynb`  

**Important:**  
These notebooks are **not part of the container image**.  
They live in a GCS bucket, for example:

```
gs://dejadsgl-pipe1/notebooks/Pipe1_1_CleanDataset.ipynb
gs://dejadsgl-pipe1/notebooks/Pipe1_2_IntegrationTables.ipynb
```

The Cloud Run Job downloads, executes, and uploads results dynamically.

---

# 2. Required Files in the Image

Only the following files must be inside `/notebooks/project` when building the container:

```
Dockerfile
requirements.txt
run_pipeline.py
```

Notebooks are **not stored in the image** but inside GCS.

---

# 3. requirements.txt (Minimal Runtime Dependencies)

```
pyspark
papermill
google-cloud-storage
google-cloud-bigquery
nbformat
nbclient
pandas
numpy
pyarrow
ipykernel
```

---

# 4. Environment Variables (Must Be Set for Cloud Run Job)

These control where notebooks are loaded from and where executed versions are saved.

```
PIPELINE_NOTEBOOK_BUCKET=<your-bucket-name>
PIPELINE_NOTEBOOK_PREFIX=pipe1/notebooks
PIPELINE_EXECUTED_PREFIX=pipe1/executed
```

Example:

```
PIPELINE_NOTEBOOK_BUCKET=dejadsgl-pipe1
PIPELINE_NOTEBOOK_PREFIX=pipe1/notebooks
PIPELINE_EXECUTED_PREFIX=pipe1/executed
```

---

# 5. IAM Roles Required

## 5.1 Service Account for Cloud Run Job (`pipe1-job-sa`)

This account must have:

```
roles/artifactregistry.reader
roles/storage.objectAdmin
roles/bigquery.jobUser
roles/bigquery.dataEditor      <-- REQUIRED for BigQuery writes
```

## 5.2 Service Account for Scheduler (`pipe1-scheduler-sa`)

```
roles/run.invoker
```

---

# 6. Set Deployment Variables

```
PROJECT_ID="dejadsgl"
REGION="us-central1"
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/de-pipelines/pipe1-job"
SA="pipe1-job-sa@$PROJECT_ID.iam.gserviceaccount.com"
```

---

# 7. Create Artifact Registry Repository (One-Time Setup)

```
gcloud artifacts repositories create de-pipelines   --repository-format=docker   --location="$REGION"   --description="Pipeline images for Pipe1"
```

---

# 8. Build and Push Container Image

Run inside `/notebooks/project`:

```
gcloud builds submit . --tag "$IMAGE"
```

---

# 9. Deploy the Cloud Run Job (Pipeline)

```
gcloud run jobs create pipe1-job   --image="$IMAGE"   --region="$REGION"   --service-account="$SA"   --cpu=2   --memory=4Gi   --max-retries=1   --task-timeout=3600s   --set-env-vars PIPELINE_NOTEBOOK_BUCKET=$PIPELINE_NOTEBOOK_BUCKET   --set-env-vars PIPELINE_NOTEBOOK_PREFIX=$PIPELINE_NOTEBOOK_PREFIX   --set-env-vars PIPELINE_EXECUTED_PREFIX=$PIPELINE_EXECUTED_PREFIX
```

---

# 10. Execute the Pipeline Manually

```
gcloud run jobs execute pipe1-job --region="$REGION"
```

Check logs under:

Cloud Run → Jobs → `pipe1-job` → Executions → Logs

---

# 11. Schedule the Pipeline (Optional)

In the Cloud Console:

1. Cloud Run → Jobs → `pipe1-job`  
2. Click **Schedule job**  
3. Choose cron, e.g.

```
0 3 * * *
Timezone: Europe/Amsterdam
```

4. Select scheduler service account (`pipe1-scheduler-sa`)

---

# 12. Updating the Pipeline

### Updating notebooks (no rebuild required)
- Upload new version of notebook to GCS  
- Next job execution uses the updated version  

### Updating pipeline logic (Python / Dockerfile)
```
gcloud builds submit . --tag "$IMAGE"
gcloud run jobs update pipe1-job --image="$IMAGE" --region="$REGION"
```

---

# 13. Troubleshooting

### Notebook Not Found
```
FileNotFoundError: gs://<bucket>/<prefix>/<file>
```

### BigQuery Write Permission Error
```
PERMISSION_DENIED: Access Denied: Table ...
```

### pyspark Missing
Add to `requirements.txt` and rebuild.

### Cloud Run cannot read bucket
Check service account roles + env vars.

---

# 14. Summary

This pipeline:

- Loads notebooks dynamically from GCS  
- Executes them sequentially using Papermill  
- Uploads executed notebooks back to GCS  
- Runs serverlessly via Cloud Run Jobs  
- Can be automated using Cloud Scheduler  
- Allows updating notebooks **without container rebuilds**  
