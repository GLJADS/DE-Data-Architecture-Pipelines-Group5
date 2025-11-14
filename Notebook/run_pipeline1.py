import os
from pathlib import Path
import papermill as pm

# # to be executed before running this script
# pip install papermill nbformat nbclient

# Pad naar de map waar je notebooks staan
BASE_DIR = Path(__file__).parent

# Volgorde van je pipeline
PIPELINE_STEPS = [
    "Pipe1_1_CleanDataset.ipynb",
    "Pipe1_2_IntegrationTables.ipynb",
    "Pipe1_3_FeatureTables.ipynb",
    "Pipe1_4_AggregationsAnalytics.ipynb",
    "Pipe1_5_Output_to_Serving_layer.ipynb",
]

# Map om de “executed” versies in op te slaan (voor logging / debugging)
OUTPUT_DIR = BASE_DIR / "executed_notebooks"
OUTPUT_DIR.mkdir(exist_ok=True)

def run_step(notebook_name: str):
    input_path = BASE_DIR / notebook_name
    output_path = OUTPUT_DIR / notebook_name.replace(".ipynb", "_executed.ipynb")

    print(f"=== Running {input_path} ===")
    pm.execute_notebook(
        input_path,
        output_path,
        parameters={}  # hier kun je evt. parameters meegeven
    )
    print(f"=== Finished {input_path}, output: {output_path} ===")

def main():
    for step in PIPELINE_STEPS:
        run_step(step)

if __name__ == "__main__":
    main()