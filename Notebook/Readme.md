#=============== stap 0 ====================
# klaarzetten bestanden
#===========================================

Dockerfile
requirements.txt
run_pipeline.py
Pipe1_1_CleanDataset.ipynb
...
#=============== stap 1 ====================
# variables
#===========================================

PROJECT_ID="dejadsq1"
SA_NAME="pipe1-job-sa"

#=============== stap 2 ====================
# Artifact Registry repository aanmaken in us-central1
#===========================================

PROJECT_ID="dejadsgl"
REGION="us-central1"

gcloud artifacts repositories create de-pipelines \
  --repository-format=docker \
  --location=$REGION \
  --description="Pipeline images in US region"

#=============== stap 3 ====================
# Cloud Run Job aanmaken in us-central1
#===========================================
PROJECT_ID="dejadsgl"
REGION="us-central1"
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/de-pipelines/pipe1-job"
SA="966685993851-compute@developer.gserviceaccount.com"

gcloud run jobs create pipe1-job \
  --image="$IMAGE" \
  --region="$REGION" \
  --service-account="$SA" \
  --cpu=2 \
  --memory=4Gi \
  --max-retries=1 \
  --task-timeout=3600s


#=============== stap 4 ====================
# Testen in us-central1
#===========================================
gcloud run jobs execute pipe1-job --region="us-central1"


#=============== stap 5  ====================
# Scheduler aanpassen
#===========================================
