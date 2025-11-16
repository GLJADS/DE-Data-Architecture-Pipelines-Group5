FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*


# Set JAVA_HOME to the default Java installation
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

# Python settings
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Google Cloud Storage connector jar into PySpark's jars folder
# Install Google Cloud Storage connector JAR in Spark's jars folder
RUN mkdir -p /opt/spark/jars \
 && curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
      -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar

# Copy pipeline code (notebooks are loaded from GCS at runtime)
COPY . .

# Start the pipeline runner
CMD ["python", "run_pipeline1.py"]
