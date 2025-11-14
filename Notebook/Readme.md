# Pipe1 Data Pipeline – Deployment Guide (Google Cloud)

This guide explains how to build, deploy, and execute the Pipe1 data-processing pipeline on Google Cloud using:

- Artifact Registry (stores the container image)
- Cloud Run Jobs (runs the full notebook pipeline)
- Cloud Scheduler (optional automation)

The pipeline executes the following notebooks in sequence:

1. Pipe1_1_CleanDataset.ipynb
2. Pipe1_2_IntegrationTables.ipynb
3. Pipe1_3_FeatureTables.ipynb
4. Pipe1_4_AggregationsAnalytics.ipynb
5. Pipe1_5_Output_to_Serving_layer.ipynb

All commands must be executed on the VM via SSH, inside:
/notebooks/project

## 0. Required Files

Your directory must contain:

- Dockerfile
- requirements.txt
- run_pipeline.py
- Pipe1_1_CleanDataset.ipynb
- Pipe1_2_IntegrationTables.ipynb
- Pipe1_3_FeatureTables.ipynb
- Pipe1_4_AggregationsAnalytics.ipynb
- Pipe1_5_Output_to_Serving_layer.ipynb

### requirements.txt must include:

- pyspark
- papermill
- nbformat
- nbclient
- pandas
- numpy
- google-cloud-bigquery
- google-cloud-storage
- pyarrow
- ipykernel

## 1. Set Environment Variables

```
PROJECT_ID="dejadsgl"
REGION="us-central1"
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/de-pipelines/pipe1-job"
SA="966685993851-compute@developer.gserviceaccount.com"
```

## 2. Create Artifact Registry Repository (One Time)

```
gcloud artifacts repositories create de-pipelines   --repository-format=docker   --location="$REGION"   --description="Pipeline images for Pipe1"
```

If the repository already exists, continue.

## 3. Build and Push Container Image

```
gcloud builds submit . --tag "$IMAGE"
```

## 4. Deploy Cloud Run Job

```
gcloud run jobs create pipe1-job   --image="$IMAGE"   --region="$REGION"   --service-account="$SA"   --cpu=2   --memory=4Gi   --max-retries=1   --task-timeout=3600s
```

## 5. Execute the Pipeline (Manual Run)

```
gcloud run jobs execute pipe1-job --region="$REGION"
```

Then check logs in:
Cloud Run → Jobs → pipe1-job → Executions → Logs

## 6. (Optional) Schedule the Pipeline

In Cloud Run UI:
Cloud Run → Jobs → pipe1-job → **Schedule job**

## 7. Updating the Pipeline

### Rebuild image:
```
gcloud builds submit . --tag "$IMAGE"
```

### Update job:
```
gcloud run jobs update pipe1-job   --image="$IMAGE"   --region="$REGION"   --service-account="$SA"
```

### Test again:
```
gcloud run jobs execute pipe1-job --region="$REGION"
```

## 8. Troubleshooting

### pyspark missing  
Add `pyspark` to requirements.txt and rebuild.

### BigQuery temp bucket  
Inside notebooks:
```
spark.conf.set("temporaryGcsBucket", "<YOUR-BUCKET>")
```

### Cloud Run job fails  
Check logs under: Cloud Run → Jobs → Executions.
