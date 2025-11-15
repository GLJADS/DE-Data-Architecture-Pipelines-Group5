FROM python:3.11-slim

# Install Java (needed for Spark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
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

# Copy pipeline code (notebooks are loaded from GCS at runtime)
COPY . .

# Start the pipeline runner
CMD ["python", "run_pipeline1.py"]
