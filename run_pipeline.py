from ingestion.ingest import main as run_ingestion
from transformations.transform import run_transformation
from validation.validate import run_validation

def run_pipeline():
    print("Starting pipeline...")

    print("Step 1: Running ingestion...")
    run_ingestion()

    print("Step 2: Running transformation...")
    run_transformation()

    print("Step 3: Running validation...")
    run_validation()

    print("Pipeline completed.")

if __name__ == "__main__":
    run_pipeline()
