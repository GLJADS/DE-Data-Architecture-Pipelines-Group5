# Pipe1 Data Pipeline – Cloud Run Jobs + GCS-Based Notebook Execution (Final Updated Guide)

This README has been fully reviewed and corrected. All commands, concepts, and deployment steps now match the actual working setup in your project:
- Project ID: dejadsgl
- Region: us-central1
- Artifact Registry Repository: de-pipelines
- Cloud Run Job Name: pipe1-job
- Container Image: us-central1-docker.pkg.dev/dejadsgl/de-pipelines/pipeline-image:latest

This version includes a validated and simplified flow for:
- building and pushing Docker images,
- running the pipeline via Cloud Run Jobs,
- updating notebooks without rebuilding images,
- and updating the Docker image when pipeline logic changes.

---

# 1. Pipeline Architecture (Notebook Execution from GCS)

The pipeline runs a sequence of Jupyter notebooks stored outside the image, in Google Cloud Storage:

gs://<bucket>/pipe1/notebooks/Pipe1_1_CleanDataset.ipynb
gs://<bucket>/pipe1/notebooks/Pipe1_2_IntegrationTables.ipynb
gs://<bucket>/pipe1/notebooks/Pipe1_3_FeatureTables.ipynb
gs://<bucket>/pipe1/notebooks/Pipe1_4_AggregationsAnalytics.ipynb
gs://<bucket>/pipe1/notebooks/Pipe1_5_Output_to_Serving_layer.ipynb

The job downloads the notebooks, executes them using Papermill + PySpark, and writes executed notebooks back to GCS.

This design ensures that notebook updates do not require Docker rebuilds.

---

# 2. Required Files in the Docker Image

Only these files must exist in your build directory (/notebooks/project):

Dockerfile
requirements.txt
run_pipeline1.py

Notebooks must NOT be included inside the image.

---

# 3. requirements.txt (Verified)

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

---

# 4. Environment Variables (Used by run_pipeline1.py)

PIPELINE_NOTEBOOK_BUCKET=<your-bucket>
PIPELINE_NOTEBOOK_PREFIX=pipe1/notebooks
PIPELINE_EXECUTED_PREFIX=pipe1/executed

Example:
PIPELINE_NOTEBOOK_BUCKET=netflix-group5-notebooks_gl
PIPELINE_NOTEBOOK_PREFIX=pipe1/notebooks
PIPELINE_EXECUTED_PREFIX=pipe1/executed

---

# 5. IAM Requirements

## Cloud Run Job service account (pipe1-job-sa) needs:

roles/artifactregistry.reader
roles/storage.objectAdmin
roles/bigquery.jobUser
roles/bigquery.dataEditor

## Cloud Scheduler service account (optional):

roles/run.invoker

---

# 6. Deployment Variables (Corrected)

PROJECT_ID="dejadsgl"
REGION="us-central1"
REPO="de-pipelines"
IMAGE="us-central1-docker.pkg.dev/$PROJECT_ID/$REPO/pipeline-image:latest"
SA="pipe1-job-sa@$PROJECT_ID.iam.gserviceaccount.com"

---

# 7. Create Artifact Registry Repository (One-Time)

gcloud artifacts repositories create de-pipelines   --repository-format=docker   --location=us-central1   --description="Pipeline images for Pipe1"

If it already exists, this step can be skipped.

---

# 8. Build and Push Docker Image (Correct + Simple)

From inside the directory containing your Dockerfile:

cd ~/notebooks/project

Enable authentication (required when using sudo):
sudo gcloud auth configure-docker us-central1-docker.pkg.dev

Build the image:
sudo docker build -t $IMAGE .

Push it:
sudo docker push $IMAGE

This is the only correct and tested path for your setup.

---

# 9. Deploy Cloud Run Job (Corrected)

You only need to create the job once:

gcloud run jobs create pipe1-job   --image=$IMAGE   --region=$REGION   --service-account=$SA   --cpu=2   --memory=4Gi   --max-retries=1   --task-timeout=3600s   --set-env-vars PIPELINE_NOTEBOOK_BUCKET=$PIPELINE_NOTEBOOK_BUCKET   --set-env-vars PIPELINE_NOTEBOOK_PREFIX=$PIPELINE_NOTEBOOK_PREFIX   --set-env-vars PIPELINE_EXECUTED_PREFIX=$PIPELINE_EXECUTED_PREFIX

---

# 10. Execute the Pipeline Manually (Correct)

gcloud run jobs execute pipe1-job --region=us-central1

Execution logs are available under:
Cloud Run → Jobs → pipe1-job → Executions → Logs

---

# 11. Optional — Schedule the Pipeline

Cron example:

0 3 * * *

Runs daily at 03:00 (Amsterdam timezone).

---

# 12. Updating the Pipeline (Corrected + Complete)

There are two update paths depending on what changes.

---

## A. Updating Notebooks (NO Docker build required)

If you edit a notebook:
- upload it to the same GCS location
- re-run the Cloud Run Job

That's it. Nothing else.

---

## B. Updating Docker Image Logic (YES rebuild required)

This applies when you change:
- run_pipeline1.py
- the Dockerfile
- requirements.txt
- Spark configuration
- GCS-connector logic

Step 1 — Build new image
cd ~/notebooks/project
sudo docker build -t $IMAGE .

Step 2 — Push to Artifact Registry
sudo docker push $IMAGE

Step 3 — Update the Cloud Run Job to use the new image
gcloud beta run jobs update pipe1-job   --image=$IMAGE   --region=us-central1

Step 4 — Run the updated job
gcloud beta run jobs execute pipe1-job --region=us-central1

This makes the job pick up the updated container.

---

# 13. Troubleshooting (Validated)

Notebook not found  
Check PIPELINE_NOTEBOOK_BUCKET and prefixes.

BigQuery write errors  
Service account missing roles/bigquery.dataEditor.

Spark cannot find GCS filesystem  
Ensure Dockerfile includes:

RUN apt-get update && apt-get install -y curl  && mkdir -p /opt/spark/jars  && curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar

Cloud Run deployment failed  
You attempted to deploy as a service instead of a job.
Use:

gcloud run jobs execute ...

---

# 14. Summary

- Cloud Run Jobs are the correct approach (not services)
- Notebooks live fully in GCS
- Docker image only contains pipeline code and Spark runtime
- Notebook updates require no rebuild
- Code changes require rebuild + job update
- Logs are fully visible in Cloud Run Jobs Executions
