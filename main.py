# ================================================================================================================
#  eR2m DAILY DATA ETL AUTOMATION PORJECT
# ================================================================================================================

## Business Problem:
# -------------------
# - Importing daily data from FTP server with Prefect

## Task:
# ----------------
# - Build an automated data import system

## Steps:
# ----------------
# 1. Get data from FTP server
# 2. Clean and Store data on a CSV file
# 3. Run ETL process on every morning at 8:30 AM
# 4. Run CRON job on MySQL server to import data on a daily basis

## GOALS
# -------
# - Build a Prefect Flow
# - Add `prefect` and examine default cli logging

### RUN COMMAND
# -----------------

# Run this command `python main.py` on command line.

## GOALS
# -------
# - Make a Deployment YAML file
# - Expose Scheduling to run the flow on an "Internet Schedule"
# - **IMPORTANT**: Interval Scheduler must be 60 seconds or greater (it must be this minimum for it to work)
# - Can also do `cron` schedule [MOST Common Automation]
#
#
# ## RESOURCES
# --------------
#  https://docs.prefect.io/concepts/schedules/

# ================================================================================================================
# # LIBRARIES
# ================================================================================================================
import os
from typing import Tuple, Dict, List, Optional
from utils import ensure_grouped_dependencies

# Ensure everything is present; choose ONE of the two strategies below:
# *** Strategy A) Install missing individually (faster, minimal install)
# libs = ensure_grouped_dependencies(GROUPS, install_missing_from_requirements=False)

# *** Strategy B) Install missing using requirements.txt once, then import

# * Group your dependencies
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
libs = ensure_grouped_dependencies(
    GROUPS,
    install_missing_from_requirements=True,
    requirements_file="requirements.txt"
    )

# You’ll then have imports available; plus you can reference them from libs if you want:
# - pd = libs.get("pandas")    # optional convenience — you can also use normal 'import pandas as pd'

# * Function to import libraries (stdlib examples & from-imports)
# ----------------------------------------------------------------

os.getcwd()
from pathlib import Path
from io import BytesIO
from glob import glob
from datetime import date, timedelta
from prefect import task, flow, get_run_logger
from tqdm.notebook import tqdm
from tqdm.version import __version__ as tqdm__version__
from pandera.typing.pandas import Index, DataFrame, Series

import warnings
warnings.simplefilter('ignore')

pa = libs.get("pandera")
pd = libs.get("pandas")

# ===============================================================================================================
# # Connect to FTP data sources and Download Data
# ================================================================================================================
@task(name="Connect to FTP and Download Data", retries=3, retry_delay_seconds=5)
def connect_and_download():
    sftpHost = os.getenv('ftp_host')
    sftpPort = int(os.getenv('ftp_port'))
    uname = os.getenv('ftp_user')
    pwd = os.getenv('ftp_pass')

    current_date = datetime.now().strftime('%Y%m%d')

    # ---- PARAMIKO CLIENT SETUP (replaces pysftp.CnOpts) ----
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # equivalent to cnopts.hostkeys=None

    # ---- CONNECT ----
    client.connect(
        hostname=sftpHost,
        port=sftpPort,
        username=uname,
        password=pwd,
        allow_agent=False,
        look_for_keys=False,
    )

    sftp = client.open_sftp()
    print("Connected to SFTP Server!!!")

    # ---- DELETE LOCAL Vilbev FILES ----
    for filename in os.listdir('.'):
        if filename.startswith('Vilbev-') and filename.endswith('.zip'):
            try:
                os.remove(filename)
                print(f'Deleted existing file: {filename}')
            except Exception as e:
                print(f'Error deleting {filename}: {e}')

    # ---- REMOTE & LOCAL PATHS ----
    remote_file = f"/home/viljoenbev/Vilbev-{current_date}.zip"
    local_file = f"./data/Vilbev-{current_date}.zip"

    # ---- DOWNLOAD ----
    try:
        sftp.get(
            remotepath=remote_file,
            localpath=local_file,
            callback=None  # optionally add progress callback
        )
        print(f'Download is Complete!!! File saved as {local_file}')
    except FileNotFoundError:
        print(f"❌ Remote file not found: {remote_file}")
    except Exception as e:
        print(f"❌ Error downloading file: {e}")

    # ---- CLEAN UP ----
    sftp.close()
    client.close()

# ===============================================================================================================
# Extract Data Task
# ===============================================================================================================
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

# ===============================================================================================================
# Main Flow
# ===============================================================================================================
@flow(name="Main Flow")
def main_flow(
    zip_file_path: str
    ):
    """
    Main Flow:
    - Connect to FTP site 
    - Download the zip file
    - Transform the data
    - Validate the data
    - Generate Data Quality Report
    - Run importation when the data is ready
    - Send email notification when the importation is done or else send an alert to the user
    """
    # For Logging the flow process
    logger = get_run_logger()

    # Connect to FTP server and download the zip file
    logger.info(f"***************************** Connecting to FTP server and downloading {zip_file_path}*****************************")
    raw = connect_and_download()
    # Extract Data from FTP site
    logger.info(f"***************************** Extracting data from {zip_file_path}*****************************")
    df = extract_data(zip_file_path)

    # Transform the data
    logger.info("***************************** Transforming the data ***************************")

    # Validate the data
    logger.info("***************************** Validating the data *****************************")

    # Generate Data Quality Report
    logger.info("***************************** Generating Data Quality Report *****************************")

    # Run importation when the data is ready
    logger.info("***************************** Running importation *****************************")

    # Send email notification when the importation is done or else send an alert to the user
    logger.info("***************************** Sending email notification *****************************")

    return df


# Run Flow
if __name__ == "__main__":
    file_path = glob('/Users/Eddie/OneDrive - eRoute2Market/eRoute2Market/Data Import/BoudeBlaaie/Boude2026*.zip')[-1]
    main_flow(zip_file_path=file_path)