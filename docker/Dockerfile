FROM python:3.14-slim

WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code
COPY ingestion/ ingestion/
COPY transformations/ transformations/
COPY validation/ validation/
COPY run_pipeline.py .

# Run pipeline
CMD ["python", "run_pipeline.py"]
