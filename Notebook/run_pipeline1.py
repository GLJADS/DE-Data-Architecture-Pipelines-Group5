"""
run_pipeline.py

Pipeline runner for Pipe1 notebooks using Google Cloud Storage (GCS).

Concept:
- Source notebooks are stored in a GCS bucket (not baked into the Docker image).
- For each step, the notebook is downloaded to /tmp,
  executed with papermill, and the executed version is uploaded back to GCS.
- This allows you to update the notebooks in GCS without rebuilding the image.

Required:
- The Cloud Run Job service account must have:
  - Storage Object Viewer (to read source notebooks)
  - Storage Object Admin or Writer (to upload executed notebooks)

Configuration via environment variables (recommended):
- PIPELINE_NOTEBOOK_BUCKET: GCS bucket that contains the notebooks
- PIPELINE_NOTEBOOK_PREFIX: prefix/folder for the source notebooks (optional)
- PIPELINE_EXECUTED_PREFIX: prefix/folder for executed notebooks (optional)
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import papermill as pm
from google.cloud import storage


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# GCS bucket where the source notebooks live
PIPELINE_NOTEBOOK_BUCKET = os.environ.get("PIPELINE_NOTEBOOK_BUCKET", "netflix-group5-notebooks_gl")

# Prefix for source notebooks in the bucket, for example: "pipe1/notebooks"
# Leave empty "" if notebooks are stored at the root of the bucket.
PIPELINE_NOTEBOOK_PREFIX = os.environ.get("PIPELINE_NOTEBOOK_PREFIX", "pipe1/notebooks")

# Prefix for executed notebooks (logs) in the same bucket
PIPELINE_EXECUTED_PREFIX = os.environ.get("PIPELINE_EXECUTED_PREFIX", "pipe1/executed")

# Local directory in the container used for temporary storage
LOCAL_WORK_DIR = Path("/tmp/pipe1_pipeline")

# Ordered list of notebooks that form the pipeline
PIPELINE_STEPS = [
    "Pipe1_1_CleanDataset.ipynb",
    "Pipe1_2_IntegrationTables.ipynb",
    "Pipe1_3_FeatureTables.ipynb",
    "Pipe1_4_AggregationsAnalytics.ipynb",
    "Pipe1_5_Output_to_Serving_layer.ipynb",
]


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def build_blob_path(prefix: str, filename: str) -> str:
    """Build a GCS blob path from an optional prefix and a filename."""
    if prefix and not prefix.endswith("/"):
        prefix_with_slash = prefix + "/"
    else:
        prefix_with_slash = prefix
    return f"{prefix_with_slash}{filename}" if prefix_with_slash else filename


def download_blob_to_file(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    local_path: Path,
) -> None:
    """Download a single GCS blob to a local file."""
    logger.info(f"Downloading gs://{bucket_name}/{blob_name} to {local_path}")
    LOCAL_WORK_DIR.mkdir(parents=True, exist_ok=True)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"Source notebook not found: gs://{bucket_name}/{blob_name}")

    blob.download_to_filename(str(local_path))
    logger.info("Download complete.")


def upload_file_to_blob(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    local_path: Path,
) -> None:
    """Upload a local file to a GCS blob."""
    logger.info(f"Uploading {local_path} to gs://{bucket_name}/{blob_name}")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))
    logger.info("Upload complete.")


def run_step(
    client: storage.Client,
    notebook_name: str,
    parameters: Optional[dict] = None,
) -> None:
    """
    Run a single notebook step:
    - download from GCS
    - execute with papermill
    - upload executed notebook back to GCS
    """
    if parameters is None:
        parameters = {}

    # Paths in GCS
    source_blob = build_blob_path(PIPELINE_NOTEBOOK_PREFIX, notebook_name)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    executed_name = notebook_name.replace(".ipynb", f"_executed_{timestamp}.ipynb")
    executed_blob = build_blob_path(PIPELINE_EXECUTED_PREFIX, executed_name)

    # Local paths
    local_input = LOCAL_WORK_DIR / notebook_name
    local_output = LOCAL_WORK_DIR / executed_name

    logger.info(f"=== Starting step: {notebook_name} ===")

    # 1. Download notebook from GCS
    download_blob_to_file(client, PIPELINE_NOTEBOOK_BUCKET, source_blob, local_input)

    # 2. Execute notebook with papermill
    logger.info(f"Executing notebook: {local_input}")
    pm.execute_notebook(
        input_path=str(local_input),
        output_path=str(local_output),
        parameters=parameters,
    )
    logger.info(f"Execution complete: {local_output}")

    # 3. Upload executed notebook back to GCS
    upload_file_to_blob(client, PIPELINE_NOTEBOOK_BUCKET, executed_blob, local_output)

    logger.info(f"=== Finished step: {notebook_name} ===")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Starting Pipe1 pipeline using notebooks from GCS.")
    logger.info(f"Notebook bucket: {PIPELINE_NOTEBOOK_BUCKET}")
    logger.info(f"Notebook prefix: {PIPELINE_NOTEBOOK_PREFIX}")
    logger.info(f"Executed prefix: {PIPELINE_EXECUTED_PREFIX}")

    if PIPELINE_NOTEBOOK_BUCKET == "YOUR-NOTEBOOK-BUCKET":
        logger.warning(
            "PIPELINE_NOTEBOOK_BUCKET is still set to the placeholder "
            "'YOUR-NOTEBOOK-BUCKET'. Please set the correct bucket name via "
            "environment variable or in this file."
        )

    storage_client = storage.Client()

    for notebook in PIPELINE_STEPS:
        run_step(storage_client, notebook)

    logger.info("Pipe1 pipeline finished successfully.")


if __name__ == "__main__":
    main()
