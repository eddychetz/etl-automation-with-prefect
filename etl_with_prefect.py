#! C:\Users\Eddie\OneDrive - eRoute2Market\eRoute2Market\Agents\etl-automation-with-prefect\.venv\Scripts\python.exe
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
        ("paramiko", None)  # note: unmaintained; keep only if you must
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
        ("ipywidgets", None)
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
import paramiko
import warnings
warnings.simplefilter('ignore')

pa = libs.get("pandera")
pd = libs.get("pandas")

# File System & Utilities
# ----------------------
import os
import zipfile
import getpass
from glob import glob
from pathlib import Path
from dotenv import load_dotenv
from utils import get_latest_zip # utils.py
from typing import Tuple, Optional, Callable
# import ftplib
# import tempfile
# from io import BytesIO

# Data Manipulation & time
# -------------------------
import numpy as np
import pandas as pd
from datetime import timedelta, datetime

# FTP server communication
# -------------------------
import paramiko

# Data Validation
# ----------------
import pandera as pa
from pandera.typing.pandas import Series

# For workflow automation
# -----------------------
from prefect import task, flow, get_run_logger # type: ignore

# Ignore warnings
# ----------------
import warnings
warnings.simplefilter('ignore')

# ===============================================================================================================
# # Connect to FTP data sources and Download Data
# ================================================================================================================
@task(name="🔐 Connect to FTP and 📥 Download Data", retries=3, retry_delay_seconds=5)
def download_data():
    """
    Connects to SFTP and downloads Vilbev-{YYYYMMDD}.zip after removing
    any existing Vilbev-*.zip files in ./data/raw.
    """
    current_date = datetime.now().strftime('%Y%m%d')

    # ---- PATHS ----
    data_dir = Path("./data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)

    local_file = data_dir / f"Vilbev-{current_date}.zip"
    remote_file = f"/home/viljoenbev/Vilbev-{current_date}.zip"

    # ---- DELETE LOCAL Vilbev FILES FIRST ----
    print("🗑️ Deleting up existing Vilbev-*.zip files in ./data/raw ...")
    deleted_any = False
    for p in data_dir.glob("Vilbev-*.zip"):
        try:
            p.unlink()
            deleted_any = True
            print(f"✔️ Deleted: {p.name}")
        except Exception as e:
            print(f"❌ Error deleting {p.name}: {e}")
    if not deleted_any:
        print("ℹ️ No existing Vilbev-*.zip files found to delete.")

    # (Optional) ensure target file does not exist—even if name pattern changes in future
    if local_file.exists():
        try:
            local_file.unlink()
            print(f"🗑️ Removed pre-existing target file: {local_file.name}")
        except Exception as e:
            print(f"❌ Error deleting pre-existing target file {local_file.name}: {e}")

    # ---- PARAMIKO CLIENT SETUP ----
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    sftp = None
    try:
        # ---- CONNECT ----
        host = os.getenv('ftp_host')
        port = int(os.getenv('ftp_port'))  # ensure integer
        user = os.getenv('ftp_user')
        pwd  = os.getenv('ftp_pass')

        client.connect(
            hostname=host,
            port=port,
            username=user,
            password=pwd,
            allow_agent=False,
            look_for_keys=False,
            timeout=30,
        )
        sftp = client.open_sftp()
        print("🔐 Connected to SFTP server 🖥️")

        # ---- DOWNLOAD ----
        print(f"📥 Downloading: {remote_file} → {local_file}")
        sftp.get(
            remotepath=remote_file,
            localpath=str(local_file),
            callback=None  # add progress callback if you need it
        )
        print("✔️ Download complete!")

        print(f"📁 File saved on {str(local_file)}")

    except FileNotFoundError:
        print(f"❌ Remote file not found: {remote_file}")
        return None
    except Exception as e:
        print(f"❌ Error during SFTP operation: {e}")
        return None
    finally:
        # ---- CLEAN UP ----
        try:
            if sftp is not None:
                sftp.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass
# ===============================================================================================================
# Extract Data Task
# ===============================================================================================================
@task(name="🔍 Extract Data - UnZip the folder", retries=3, retry_delay_seconds=[2, 5, 15])
def extract_data() -> pd.DataFrame:
    """
    Extract the first CSV file from a ZIP archive and load it into a pandas DataFrame.
    Handles:
    - file existence checks
    - multiple CSV files (selects first match)
    - safe extraction into a temp folder
    - consistent return behavior
    """
    zip_file_path = get_latest_zip(os.getenv('BASE_DIR'))

    if not os.path.exists(zip_file_path):
        raise FileNotFoundError(f"❌ ZIP file does not exist: {zip_file_path}")

    print("📦 Reading ZIP archive!")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:

        # List all files
        file_list = zip_ref.namelist()
        print("📁 Files inside ZIP:", file_list)

        # find CSV file(s)
        csv_files = [f for f in file_list if f.lower().endswith(".csv")]

        if not csv_files:
            raise ValueError("❌ No CSV file found inside ZIP.")

        # Use the first CSV file found
        csv_file_name = csv_files[0]
        print(f"📄 Found CSV file: {csv_file_name}")

        # Ensure extraction directory exists
        extract_dir = "data"
        os.makedirs(extract_dir, exist_ok=True)

        # Extract file (optional but useful for debugging)
        extracted_path = zip_ref.extract(csv_file_name, path=extract_dir)
        print(f"📤 Extracted to: {extracted_path}")

        # Load CSV into pandas directly from ZIP
        with zip_ref.open(csv_file_name) as csv_file:
            try:
                df = pd.read_csv(csv_file)
                print(f"✔️ Loaded CSV: {csv_file_name}")
            except Exception as e:
                raise ValueError(f"❌ Failed to read CSV inside ZIP: {e}")

    return df


# ==============================================================================================================
# Transform Data Task
# ==============================================================================================================
@task(name="⏳ Transform data", retries=3, retry_delay_seconds=[2,5,15])
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to transform Viljoen Beverages data

    Args:
        df: Input dataframe to transform
        returns: Transformed dataframe
    """
    # Standard column layout
    columns=[
        'SellerID','GUID','Date','Reference','Customer_Code','Name','Physical_Address1',\
        'Physical_Address2','Physical_Address3','Physical_Address4','Telephone',\
        'Stock_Code','Description','Price_Ex_Vat','Quantity','RepCode','ProductBarCodeID'
        ]
    # Create an empty dataframe
    df1=pd.DataFrame(columns=columns)

    # Build the dataframe
    df1['Date']=df['Date']
    df1['SellerID']='VILJOEN'
    df1['GUID']=0
    df1['Reference']=df['Reference']
    df1['Customer_Code']=df['Customer code']
    df1['Name']=df['Customer name']
    df1['Physical_Address1']=df['Physical_Address1']
    df1['Physical_Address2']=df['Physical_Address2']
    df1['Physical_Address3']=df['Physical_Address3']
    df1['Physical_Address4']=(
        df['Deliver1'].fillna('').astype(str) +' '+
        df['Deliver2'].fillna('').astype(str) +' '+
        df['Deliver3'].fillna('').astype(str) +' '+
        df['Deliver4'].fillna('').astype(str)
        ).str.strip()

    df1['Telephone']=df['Telephone']
    df1['Stock_Code']=df['Product code']
    df1['Description']=df['Product description']
    df1['Price_Ex_Vat']=round(abs(df['Value']/df['Quantity']),2)
    df1['Quantity']=df['Quantity']
    df1['RepCode']=df['Rep']
    df1['ProductBarCodeID']=''

    print(f"ℹ️ Total quantity: {np.sum(df1['Quantity']):.0f}\n")

    df2=df1.copy()
    df2['Date']=pd.to_datetime(df2['Date'])
    df2['Date']=df2['Date'].apply(lambda x: x.strftime("%Y-%m-%d"))

    # Condition: Physical_Address1 contains the phrase AND Name is missing/empty
    mask = df1["Physical_Address1"].astype(str).str.contains("SPAR NORTHRAND (11691)", na=False)

    df1["Name"].fillna('SPAR NORTH RAND (11691)', inplace=True)
    #   DATE FORMAT CLEANING
    # -----------------------------
    print("✔️ Date format cleaned")
    df1['Date'] = pd.to_datetime(df1['Date'], errors="coerce").dt.strftime("%Y-%m-%d")
    print("✔️ Data transformation complete!")

    return df1


# =============================================================================================================
# Validate Data Task
# =============================================================================================================
@task(name="⏳ Validate data", retries=3, retry_delay_seconds=[2,5,15])
def validate_data(df: pd.DataFrame):
    """
    Function to validate data
    """
    # logger = get_run_logger()
    class Schema(pa.DataFrameModel):
        # 1. Check data types and uniqueness
        SellerID: Series[str] = pa.Field(nullable=False)  # seller IDs must be non-null
        GUID: Series[int] = pa.Field(ge=0, nullable=False)  # must be non-null

        # 2. Dates coerced to proper datetime
        Date: Series[pd.Timestamp] = pa.Field(coerce=False, nullable=False) # must be non-null

        # 3. Reference and customer codes
        Reference: Series[str] = pa.Field(nullable=False) # must be non-null
        Customer_Code: Series[str] = pa.Field(str_matches=r"^[A-Z0-9]+$", nullable=False)  # must be non-null

        # 4. Customer details
        Name: Series[str] = pa.Field(nullable=False) # must be non-null
        Physical_Address1: Series[str] = pa.Field(nullable=True)
        Physical_Address2: Series[str] = pa.Field(nullable=True)
        Physical_Address3: Series[str] = pa.Field(nullable=True)
        Physical_Address4: Series[str] = pa.Field(nullable=True)

        # 5. Telephone validation (basic regex for digits, spaces, +, -)
        Telephone: Series[str] = pa.Field(nullable=True)

        # 6. Product details
        Stock_Code: Series[str] = pa.Field(nullable=False) # must be non-null
        Description: Series[str] = pa.Field(nullable=False) # must be non-null
        Price_Ex_Vat: Series[float] = pa.Field(ge=0.0, nullable=False)  # must be non-null
        Quantity: Series[int] = pa.Field(nullable=False)  # must be non-null

        # 7. Rep and barcode
        RepCode: Series[str] = pa.Field(nullable=True)
        ProductBarCodeID: Series[str] = pa.Field(nullable=True)  # typical EAN/UPC

        class Config:
            strict = True  # enforce exact schema
            coerce = True  # auto-convert types where possible

    try:
        # lazy=True means "find all errors before crashing"
        Schema.validate(df, lazy=True)
        print("✔️ Data passed validation!\nℹ️ Proceeding to next step.")

    except pa.errors.SchemaErrors as err:
        print("⚠️ Data Contract Breached!.......\n")
        print(f"❌ Total errors found: {len(err.failure_cases)}")

        # Let's look at the specific failures
        print("\n*********⚠️Failure Report⚠️************\n")
        print(err.failure_cases[['column', 'check', 'failure_case']])

# ============================================================================================================
# Load Data Task
# ============================================================================================================
@task(name="📤 Load to Local Data Store")
def load_to_local(
    df: pd.DataFrame,
    create_dir_if_missing: bool = True,
    delete_existing_csvs: bool = True,        # ← new flag to control cleanup
    restrict_delete_to_prefix: str | None = None  # e.g., "Viljoenbev_" to only delete those CSVs
) -> Tuple[str, bool]:
    """
    Save cleaned data to a CSV inside the folder specified by OUTPUT_DIR in .env,
    only if:
    - the DataFrame's date range is entirely within the last 3 days, and
    - the latest date's month is the current month or the previous month.
    Skips save if a file with the same name already exists.

    Returns
    -------
    (full_path, saved) : Tuple[str, bool]
        full_path -> absolute path to the intended CSV
        saved     -> True if file was written, False if skipped (already existed)
    """
    # --- Resolve OUTPUT_DIR ---
    output_dir = os.getenv("OUTPUT_DIR")
    if not output_dir:
        raise ValueError("❌ Environment variable 'OUTPUT_DIR' is not set in your environment or .env file.")

    output_dir_path = Path(os.path.abspath(os.path.expanduser(output_dir)))
    if not output_dir_path.is_dir():
        if create_dir_if_missing:
            output_dir_path.mkdir(parents=True, exist_ok=True)
            print(f"🗂️ Created output directory: {output_dir_path}")
        else:
            raise FileNotFoundError(f"Output directory does not exist: {output_dir_path}")

    # ---- DELETE EXISTING CSVs IN CLEANED FOLDER (before saving) ----
    if delete_existing_csvs:
        # Choose the pattern:
        #   "*.csv" to delete ALL CSVs in the folder
        #   f"{restrict_delete_to_prefix}*.csv" to only delete those starting with a prefix
        if restrict_delete_to_prefix:
            pattern = f"{restrict_delete_to_prefix}*.csv"
        else:
            pattern = "*.csv"

        print(f"⏳ Cleaning up existing CSV file in:\n📁 {output_dir_path}.")
        deleted_any = False
        for p in output_dir_path.glob(pattern):
            try:
                p.unlink()
                deleted_any = True
                print(f"🗑️ Deleted CSV: {p.name}")
            except Exception as e:
                print(f"❌ Error deleting {p.name}: {e}")
        if not deleted_any:
            print("ℹ️ No matching CSV files found to delete.")

    # --- Prepare and validate dates ---
    if "Date" not in df.columns:
        raise KeyError("🔑 Input DataFrame must contain a 'Date' column.")

    data = df.copy()
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")

    if data["Date"].isna().all():
        raise ValueError("❌ All values in 'Date' are NaT after parsing. Check your input data.")

    min_date = data["Date"].dropna().min()
    max_date = data["Date"].dropna().max()

    # Validation per your rule:
    if today is None:
        today = datetime.now()

    # Normalize to date (drop time)
    today_d = today.date()
    window_start = today_d - timedelta(days=3) # lookback - 3 days

    min_d = min_date.date()
    max_d = max_date.date()

    # 1) Entire range must be within the last `lookback_days` days (inclusive)
    if not (window_start <= min_d <= today_d and window_start <= max_d <= today_d):
        raise ValueError(
            f"❌ Date range {min_d} to {max_d} is not fully within the last {lookback_days} days "
            f"({window_start}..{today_d})."
        )

    # 2) Month check on the latest date in the file (max_d)
    cur_month = today_d.month
    prev_month = 12 if cur_month == 1 else cur_month - 1
    file_month = max_d.month

    if file_month not in (cur_month, prev_month):
        raise ValueError(
            f"❌ Latest file month ({file_month}) is not the current month ({cur_month}) "
            f"or previous month ({prev_month})."
        )

    # --- Build deterministic filename and check for existence ---
    min_str = min_date.strftime("%Y-%m-%d")
    max_str = max_date.strftime("%Y-%m-%d")
    filename = f"Viljoenbev_{min_str}_to_{max_str}.csv"
    full_path = output_dir_path / filename

    if full_path.exists():
        print(f"🛑 File already exists, skipping save:\n📁 {full_path}")
        return str(full_path), False

    # --- Finalize and save ---
    data["Date"] = data["Date"].dt.strftime("%Y-%m-%d")
    data.to_csv(full_path, index=False)
    print(f"\n✔️ Data saved to:\n📁 {full_path}")
    return str(full_path), True

# =============================================================================================================
# Load to Server
# =============================================================================================================
@task(name="📤 Upload to remote server 🖥️")
def upload_to_server(csv_file_path: str = None,) -> Optional[str]:
    """
    Upload a specific CSV file to an SFTP server using only paramiko.

    Parameters:
    -----------
    csv_file_path : str
        Path to the local CSV file to upload. If None, it will pick the first
        file matching 'Viljoenbev_*.csv' in `output_dir`.

    Returns:
    --------
    str or None
        Remote path where the file was uploaded, or None if the upload failed.
    """
    # ---- CONNECT ----
    sftp_host = os.getenv('ftp_host')
    sftp_port = int(os.getenv('ftp_port'))  # ensure integer
    sftp_user = os.getenv('ftp_user')
    sftp_pass  = os.getenv('ftp_pass')

    remote_dir = os.getenv('ftp_dir')

    # Fallback to discover a file if not provided (mirrors your original default idea)
    if csv_file_path is None:
        # Adjust `output_dir` to your actual variable/scope if needed
        output_dir = os.getenv('OUTPUT_DIR')
        matches = glob(os.path.join(output_dir, 'Viljoenbev_*.csv'))
        if not matches:
            print("⚠️ No CSV file found matching 'Viljoenbev_*.csv'.")
            return None
        csv_file_path = matches[0]

    try:
        # Verify the local file exists
        if not os.path.exists(csv_file_path):
            print(f"⚠️ Local file not found: {csv_file_path}")
            return None

        filename = os.path.basename(csv_file_path)
        # Normalize remote path to POSIX style for SFTP
        remote_dir_posix = remote_dir.replace('\\', '/').rstrip('/') + '/'
        remote_path = (remote_dir_posix + filename)

        # Create SSH client and connect
        ssh = paramiko.SSHClient()

        # WARNING: Auto-adding host keys reduces security. Prefer loading known hosts in production.
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        print(f"🔐 Connecting to FTP server 🖥️ as {sftp_user}.")
        ssh.connect(
            hostname=sftp_host,
            port=sftp_port,
            username=sftp_user,
            password=sftp_pass,
            look_for_keys=False,
            allow_agent=False,
            timeout=30
        )

        try:
            # Open SFTP session
            sftp = ssh.open_sftp()

            # Upload with confirmation via file size/stat check
            print(f"📤 Uploading {filename} to {remote_path}.")
            sftp.put(csv_file_path, remote_path)

            # Optional: verify upload completed by checking remote file size
            local_size = os.path.getsize(csv_file_path)
            remote_stat = sftp.stat(remote_path)
            if remote_stat.st_size != local_size:
                print("⚠️ Size mismatch after upload. Upload may be incomplete.")
                return None

            print("✔️ Upload completed successfully!")
            return None

        finally:
            try:
                sftp.close()
            except Exception:
                pass
            ssh.close()

    except paramiko.AuthenticationException:
        print("🔴 Authentication failed. Please verify username/password (or key).")
        return None
    except paramiko.SSHException as e:
        print(f"❌ SSH/SFTP error: {e}")
        return None
    except Exception as e:
        print(f"❌ Error uploading file: {e}")
        return None

# =============================================================================================================
# Run Importation Task
# =============================================================================================================
@task(name="🚀 Running import script")
def run_import(timeout: int = 120):
    """
    Connect to a remote server via SSH and execute commands, first testing then running.

    Args:
        timeout (int): Timeout for command execution in seconds

    Returns:
        tuple: (stdout, stderr, exit_code) from the last command executed
    """
    # Access login credentials
    hostname = os.getenv('host_name')
    port = os.getenv('port')
    username = os.getenv('user_name')
    password = os.getenv('password')

    # Define commands
    commands = {
        'test': "/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1",
        'run': "/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1"
    }

    # If no password provided, prompt for it securely
    if password is None:
        password = getpass.getpass(f"🔑 Enter SSH password for {username}@{hostname}: ")

    # Create an SSH client instance
    client = paramiko.SSHClient()

    try:
        # Automatically add the server's host key
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the remote server
        print(f"🔐 Connecting to {hostname} on port {port}...")
        client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password
        )

        # First execute the test command
        print(f"🚀 Executing test command:>>>>>>>> {commands['test']}")
        stdin, stdout, stderr = client.exec_command(commands['test'], timeout=timeout)
        exit_status = stdout.channel.recv_exit_status()
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')

        if exit_status != 0:
            print(f"🔴 Test command failed with exit code: {exit_status}")
            print("🛑 Aborting - not running the main command.")
            return stdout_str, stderr_str, exit_status

        print("🧪 Test command succeeded.")

        # If test succeeded, execute the run command
        print(f"🚀 Executing run command: {commands['run']}")
        stdin, stdout, stderr = client.exec_command(commands['run'], timeout=timeout)
        exit_status = stdout.channel.recv_exit_status()
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')

        # Print status
        if exit_status == 0:
            print("🟢 Run command executed successfully.")
        else:
            print(f"❌ Run command failed with exit code: {exit_status}")

        # Return results from the run command
        return stdout_str, stderr_str, exit_status

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return "", str(e), -1

    finally:
        # Always close the connection
        client.close()
        print("🔌 SSH connection closed.")

# Helper Function
# -----------------
@task(name="📝 Log error report")
def parse_perl_output(stdout: str, stderr: str, exit_code: int) -> dict:
    working_messages = [line for line in stdout.splitlines() if line.strip()]

    stderr_lines = [line for line in stderr.splitlines() if line.strip()]
    warnings = [warn for warn in stderr_lines if "warning" in warn.lower()]
    errors = [err for err in stderr_lines if "error" in err.lower()]
    other_stderr = [serr for serr in stderr_lines if serr not in warnings + errors]

    return {
        "working_on": working_messages,
        "warnings": warnings,
        "errors": errors,
        "other_stderr": other_stderr,
        "exit_code": exit_code
    }


# ===============================================================================================================
# Main Flow
# ===============================================================================================================
@flow(name="🚀 Main ETL Flow")
def main():
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
    logger.info("********⚙️ Connecting to FTP server and downloading ⚙️*********")
    download_data()

    # Extract Data from FTP site
    logger.info("********⚙️ Extracting data ⚙️**********")
    # raw = extract_data()

    # Transform the data
    logger.info("*************⚙️ Transforming the data ⚙️*********")
    # df = transform_data(raw)

    # Validate the data
    logger.info("************ ⚙️ Validating the data ⚙️**********")
    # validate_data(df)

    # Generate Data Quality Report
    logger.info("**********⚙️ Generating Data Quality Report ⚙️************")
    # load_to_local(df)
    # upload_to_server()

    # Run importation when the data is ready
    logger.info("***********⚙️ Running importation ⚙️*************")
    # out, err, cod = run_import()
    # result = parse_perl_output(out, err, cod)
    # for key, value in result.items():
    #     print(f"{key}: {value}")
    # Send email notification when the importation is done or else send an alert to the user
    logger.info("************⚙️ Sending email notification ⚙️***************")

# Run Flow
if __name__ == "__main__":
    main()

# # TESTING
# To start local server, run `prefect server start`
# `python etl_with_prefect.py`
#
# # DEPLOYMENT STEPS & CLI COMMANDS:
###########################################

# 1. BUILD:
#     `prefect deployment build etl_with_prefect.py:etl_flow --name sp500_flow --interval 60`
#
# 2. PARAMETERS:
#     path: 
#
# 3. APPLY:
#     `prefect deployment apply etl_flow-deployment.yaml`
#
# 4. LIST DEPLOYMENTS:
#     `prefect deployment ls`
#
# 5. RUN:
#     `prefect deployment run "SP500 Stock Price Pipeline/etl_with_prefect"`

# 6. ORION GUI:
#     `prefect orion start`
#
# 7. AGENT START: (on a new terminal)
#     `prefect agent start --work-queue "default"`
#
# 8. Ctrl + C to exit
#