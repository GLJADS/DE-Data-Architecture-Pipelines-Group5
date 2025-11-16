FROM python:3.11-slim

# Java + curl installeren
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# JAVA_HOME instellen
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

# Python settings
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Werkdirectory in de container
WORKDIR /app

# Python dependencies installeren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# GCS connector in de pyspark/jars map zetten
RUN set -eux; \
    jars_dir=$(python -c "import pyspark, pathlib; print(pathlib.Path(pyspark.__file__).with_name('jars'))"); \
    mkdir -p "$jars_dir" && \
    curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
         -o "$jars_dir/gcs-connector-hadoop3-latest.jar"

# Pipeline code kopiëren (notebooks komen uit GCS)
COPY . .

# Start de pipeline runner (voor Cloud Run Job)
CMD ["python", "run_pipeline1.py"]