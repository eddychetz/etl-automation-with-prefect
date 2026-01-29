#  eR2m DAILY DATA ETL AUTOMATION PORJECT
# **********************************************************************************

## Business Problem:
# - Importing daily data from FTP server with Prefect

## Task:
# - Build an automated data import system

## Steps:
# 1. Get data from FTP server
# 2. Clean and Store data on a CSV file
# 3. Run ETL process on every morning at 8:30 AM
# 4. Run CRON job on MySQL server to import data on a daily basis

## GOALS
# - Add `prefect` and examine default cli logging

### RUN COMMAND

# Run this command `python main.py` on command line.

## GOALS
# - Make a Deployment YAML file
# - Expose Scheduling to run the flow on an "Internet Schedule"
# - **IMPORTANT**: Interval Scheduler must be 60 seconds or greater (it must be this minimum for it to work)
# - Can also do `cron` schedule [MOST Common Automation]
#
#
# ## RESOURCES
#  https://docs.prefect.io/concepts/schedules/

# # LIBRARIES
import os
from typing import Tuple, Dict, List, Optional
from utils import ensure_grouped_dependencies

# Ensure everything is present; choose ONE of the two strategies below:
# * Strategy A) Install missing individually (faster, minimal install)
# libs = ensure_grouped_dependencies(GROUPS, install_missing_from_requirements=False)

# * Strategy B) Install missing using requirements.txt once, then import
# ---------------------------
# 2) Group your dependencies
# ---------------------------
GROUPS: Dict[str, List[Tuple[str, Optional[str]]]] = {
    # (import_name, pip_name or None if same)
    "stdlib": [
        ("os", None),
        ("sys", None),
        ("subprocess", None),
        ("warnings", None),
        ("glob", None),
        ("zipfile", None),
        ("tempfile", None),
        ("time", None),
        ("getpass", None),
        # You also use these below via from-imports
        ("pathlib", None),
        ("io", None),
        ("datetime", None),
        ("typing", None),
    ],
    "core_data": [
        ("pandas", None),
        ("numpy", None),
    ],
    "sftp": [
        ("paramiko", None),
        ("pysftp", None),  # note: unmaintained; keep only if you must
    ],
    "orchestration": [
        ("prefect", None),
    ],
    "validation": [
        ("pandera", None),
        ("pandera.typing.pandas", None),
    ],
    "ux": [
        ("tqdm", None),
        ("tqdm.notebook", None),
    ],
}
libs = ensure_grouped_dependencies(GROUPS, install_missing_from_requirements=True, requirements_file="requirements.txt")

# You’ll then have imports available; plus you can reference them from libs if you want:
# - pd = libs.get("pandas")    # optional convenience — you can also use normal 'import pandas as pd'

# Function to import libraries (stdlib examples & from-imports)

os.getcwd()
from pathlib import Path
from io import BytesIO
from glob import glob
from datetime import date, timedelta
from prefect import task, flow, get_run_logger
from tqdm.notebook import tqdm
from tqdm.version import __version__ as tqdm__version__
# from pandera.typing.pandas import Index, DataFrame, Series

import warnings
warnings.simplefilter('ignore')
pa = libs.get("pandera")
pd = libs.get("pandas")
# Extract Data Task
@task(name="Extract Data", retries=3, retry_delay_seconds=[2, 5, 15])
def extract_data(zip_file_path: str) -> pd.DataFrame:

    # Open the ZIP file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # List all files in the ZIP archive
        file_list = zip_ref.namelist()
        print("Files in the ZIP archive:", file_list)

        # Find the CSV file in the ZIP archive (assuming there's only one CSV file)
        csv_file_name = next((file for file in file_list if file.endswith('.csv')), None)
        
        if csv_file_name:
            print(f"Found CSV file: {csv_file_name}")
            
            # Extract the CSV file to a temporary location (optional)
            zip_ref.extract(csv_file_name, path="extracted_files")
            
            # Read the CSV file directly from the ZIP archive
            with zip_ref.open(csv_file_name) as csv_file:
                df = pd.read_csv(csv_file)
                print("CSV file content:")
        else:
            print("No CSV file found in the ZIP archive.")
            
        return df