# Pipe1 Data Pipeline – Cloud Run Jobs + GCS-Based Notebook Execution (Final Updated Guide)

This README has been fully reviewed and corrected. All commands, concepts, and deployment steps now match the actual working setup in your project:
- Project ID: dejadsgl
- Region: us-central1
- Artifact Registry Repository: de-pipelines
- Cloud Run Job Name: pipe1-job
- Container Image: us-central1-docker.pkg.dev/dejadsgl/de-pipelines/pipeline-image:latest

This version includes a validated and simplified flow for building and pushing Docker images, running the pipeline via Cloud Run Jobs, updating notebooks without rebuilding images, and updating the Docker image when pipeline logic changes.

# 1. Pipeline Architecture (Notebook Execution from GCS)

The pipeline runs a sequence of Jupyter notebooks stored outside the image, in Google Cloud Storage:

gs://<bucket>/pipe1/notebooks/Pipe1_1_CleanDataset.ipynb
gs://<bucket>/pipe1/notebooks/Pipe1_2_IntegrationTables.ipynb
gs://<bucket>/pipe1/notebooks/Pipe1_3_FeatureTables.ipynb
gs://<bucket>/pipe1/notebooks/Pipe1_4_AggregationsAnalytics.ipynb
gs://<bucket>/pipe1/notebooks/Pipe1_5_Output_to_Serving_layer.ipynb

The job downloads the notebooks, executes them using Papermill + PySpark, and writes executed notebooks back to GCS.

# 2. Required Files in the Docker Image

Dockerfile  
requirements.txt  
run_pipeline1.py  

# 3. requirements.txt

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

# 4. Environment Variables

PIPELINE_NOTEBOOK_BUCKET=<your-bucket>  
PIPELINE_NOTEBOOK_PREFIX=pipe1/notebooks  
PIPELINE_EXECUTED_PREFIX=pipe1/executed  

# 5. IAM Requirements

roles/artifactregistry.reader  
roles/storage.objectAdmin  
roles/bigquery.jobUser  
roles/bigquery.dataEditor  

# 6. Deployment Variables

PROJECT_ID="dejadsgl"  
REGION="us-central1"  
REPO="de-pipelines"  
IMAGE="us-central1-docker.pkg.dev/$PROJECT_ID/$REPO/pipeline-image:latest"  
SA="pipe1-job-sa@$PROJECT_ID.iam.gserviceaccount.com"  

# 7. Create Artifact Registry Repo

gcloud artifacts repositories create de-pipelines --repository-format=docker --location=us-central1 

# 8. Build & Push Image

cd ~/notebooks/project  
sudo gcloud auth configure-docker us-central1-docker.pkg.dev  
sudo docker build -t $IMAGE .  
sudo docker push $IMAGE  

# 9. Deploy Cloud Run Job

gcloud run jobs create pipe1-job --image=$IMAGE --region=$REGION --service-account=$SA --cpu=2 --memory=4Gi --max-retries=1 --task-timeout=3600s --set-env-vars PIPELINE_NOTEBOOK_BUCKET=$PIPELINE_NOTEBOOK_BUCKET --set-env-vars PIPELINE_NOTEBOOK_PREFIX=$PIPELINE_NOTEBOOK_PREFIX --set-env-vars PIPELINE_EXECUTED_PREFIX=$PIPELINE_EXECUTED_PREFIX

# 10. Execute Pipeline

gcloud run jobs execute pipe1-job --region=us-central1

# 11. Scheduling

0 3 * * *

# 12. Updating Pipeline

Notebook updates → upload new notebook to GCS  
Code updates → rebuild + push + update job  

# 13. Static External IP for VM

Reserve static IP:  
gcloud compute addresses create my-static-ip --region=us-central1  

Find IP:  
gcloud compute addresses describe my-static-ip --region=us-central1 --format="get(address)"  

Remove ephemeral:  
gcloud compute instances delete-access-config instance-20251029-143900 --zone=us-central1-c --network-interface=nic0 --access-config-name="External NAT"  

Add static IP:  
gcloud compute instances add-access-config instance-20251029-143900 --zone=us-central1-c --network-interface=nic0 --access-config-name="External NAT" --address=<STATIC-IP>  

Verify:  
gcloud compute instances describe instance-20251029-143900 --zone=us-central1-c --format="get(networkInterfaces[0].accessConfigs[0].natIP)"

# 14. Summary

- Cloud Run Jobs are correct for this workflow  
- Notebooks remain in GCS  
- Image only contains Spark + pipeline logic  
- Notebook updates need no rebuild  
- Code updates require rebuild + update job  
