# [markdown]
# # Load Libraries

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

# Load environment variables
load_dotenv()

# Print current working directory
print(os.getcwd()) # os.getcwd()

# [markdown]
# # Download data

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
    print("🧹 Cleaning up existing Vilbev-*.zip files in ./data/raw ...")
    deleted_any = False
    for p in data_dir.glob("Vilbev-*.zip"):
        try:
            p.unlink()
            deleted_any = True
            print(f"🗑️ Deleted: {p.name}")
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
        print("🔐 Connected to SFTP server")

        # ---- DOWNLOAD ----
        print(f"📥 Downloading: {remote_file} → {local_file}")
        sftp.get(
            remotepath=remote_file,
            localpath=str(local_file),
            callback=None  # add progress callback if you need it
        )
        print("✅ Download complete!")

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

download_data()

# [markdown]
# # Unzip file

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
                print(f"✅ Loaded CSV: {csv_file_name}")
            except Exception as e:
                raise ValueError(f"❌ Failed to read CSV inside ZIP: {e}")

    return df

raw = extract_data()

# [markdown]
# # Transform data

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

    #   INTELLIGENT NAME BACKFILLING
    # -----------------------------------
    # 1) Use Customer_Code as primary matching key
    # -----------------------------
    # df1['Name'] = df1.groupby('Customer_Code')['Name'].transform(
    #     lambda x: x.fillna(x.mode().iloc[0]) if x.mode().size > 0 else x
    # )
    # # 2) Use Address fields as secondary matching key
    # # -----------------------------
    # df1['Name'] = df1.groupby(
    #     ['Physical_Address1', 'Physical_Address2', 'Physical_Address3', 'Physical_Address4']
    # )['Name'].transform(
    #     lambda x: x.fillna(x.mode().iloc[0]) if x.mode().size > 0 else x
    # )
    # # 3) Use telephone number as fallback
    # # -----------------------------
    # df1['Name'] = df1.groupby('Telephone')['Name'].transform(
    #     lambda x: x.fillna(x.mode().iloc[0]) if x.mode().size > 0 else x
    # )
    # # 4) Global fallback (only for final unresolved missing names)
    # # -----------------------------
    # df1['Name'].fillna('SPAR NORTH RAND (11691)', inplace=True)
    # print("✅ Missing buyer names fixed.")

    # Ensure you imported pandas as pd
    # Condition: Physical_Address1 contains the phrase AND Name is missing/empty
    mask = df1["Physical_Address1"].astype(str).str.contains("SPAR NORTHRAND (11691)", na=False)

    df1["Name"].fillna('SPAR NORTH RAND (11691)', inplace=True)
    #   DATE FORMAT CLEANING
    # -----------------------------
    print("✅ Date fomat cleaned")
    df1['Date'] = pd.to_datetime(df1['Date'], errors="coerce").dt.strftime("%Y-%m-%d")
    print("✅ Data transformation complete!")

    return df1

df = transform_data(raw)

df.iloc[13:17]

# [markdown]
# # Validate data

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
        print("✅ Data passed validation!\nℹ️ Proceeding to next step.")

    except pa.errors.SchemaErrors as err:
        print("⚠️ Data Contract Breached!.......\n")
        print(f"❌ Total errors found: {len(err.failure_cases)}")

        # Let's look at the specific failures
        print("\n*********⚠️Failure Report⚠️************\n")
        print(err.failure_cases[['column', 'check', 'failure_case']])

validate_data(df)

# [markdown]
#  Load Data

# -------------------------------------------------------------
# Validate date in cleaned file
# -------------------------------------------------------------
def validate_dates(
    min_date: pd.Timestamp,
    max_date: pd.Timestamp,
    today: datetime = None,
    lookback_days: int = 3) -> None:

    """
    Raises ValueError if the date range is not entirely within the last `lookback_days`
    and if the latest month is neither the current month nor the previous month.
    """

    if today is None:
        today = datetime.now()

    # Normalize to date (drop time)
    today_d = today.date()
    window_start = today_d - timedelta(days=lookback_days)

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
        raise ValueError("Environment variable 'OUTPUT_DIR' is not set in your environment or .env file.")

    output_dir_path = Path(os.path.abspath(os.path.expanduser(output_dir)))
    if not output_dir_path.is_dir():
        if create_dir_if_missing:
            output_dir_path.mkdir(parents=True, exist_ok=True)
            print(f"📁 Created output directory: {output_dir_path}")
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

        print(f"🧹 Cleaning up existing CSV file in:\n📁 {output_dir_path}.")
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
        raise KeyError("Input DataFrame must contain a 'Date' column.")

    data = df.copy()
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")

    if data["Date"].isna().all():
        raise ValueError("All values in 'Date' are NaT after parsing. Check your input data.")

    min_date = data["Date"].dropna().min()
    max_date = data["Date"].dropna().max()

    # Validation per your rule:
    validate_dates(min_date, max_date, lookback_days=3)

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
    print(f"\n✅ Data saved to:\n📁 {full_path}")
    return str(full_path), True

load_to_local(df)

# [markdown]
# # Upload to FTP server

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

        print(f"🔐 Connecting to FTP server as {sftp_user}.")
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

            # Ensure remote directory exists (create recursively if missing)
            _ensure_remote_dir(sftp, remote_dir_posix)

            # Upload with confirmation via file size/stat check
            print(f"🚀 Uploading {filename} to {remote_path}...")
            sftp.put(csv_file_path, remote_path)

            # Optional: verify upload completed by checking remote file size
            local_size = os.path.getsize(csv_file_path)
            remote_stat = sftp.stat(remote_path)
            if remote_stat.st_size != local_size:
                print("⚠️ Size mismatch after upload. Upload may be incomplete.")
                return None

            print("✅ Upload completed successfully!")
            return remote_path

        finally:
            try:
                sftp.close()
            except Exception:
                pass
            ssh.close()

    except paramiko.AuthenticationException:
        print("⚠️ Authentication failed. Please verify username/password (or key).")
        return None
    except paramiko.SSHException as e:
        print(f"❌ SSH/SFTP error: {e}")
        return None
    except Exception as e:
        print(f"❌ Error uploading file: {e}")
        return None

# Remote Directory Helpers
# ------------------------------
def _ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir_posix: str) -> None:
    """
    Recursively create remote directories if they do not exist.
    `remote_dir_posix` must be a POSIX-style path ending with '/'.
    """
    # Split path into components and build progressively
    # Handle absolute paths like '/home/user/data/'
    parts = [p for p in remote_dir_posix.split('/') if p]
    prefix = '/' if remote_dir_posix.startswith('/') else ''

    current = prefix
    for part in parts:
        current = (current.rstrip('/') + '/' + part)
        try:
            sftp.stat(current)  # Exists
        except FileNotFoundError:
            sftp.mkdir(current)

upload_to_server()

# [markdown]
# # Run import Script

# Helper Function
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
    port = int(os.getenv('port'))
    username = os.getenv('user_name')
    password = os.getenv('password')

    # Define commands
    commands = {
        'test': "/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1",
        'run': "/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1"
    }

    # If no password provided, prompt for it securely
    if password is None:
        password = getpass.getpass(f"Enter SSH password for {username}@{hostname}: ")

    # Create an SSH client instance
    client = paramiko.SSHClient()

    try:
        # Automatically add the server's host key
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the remote server
        print(f"Connecting to {hostname} on port {port}...")
        client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password
        )

        # First execute the test command
        print(f"Executing test command:>>>>>>>> {commands['test']}")
        stdin, stdout, stderr = client.exec_command(commands['test'], timeout=timeout)
        exit_status = stdout.channel.recv_exit_status()
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')

        if exit_status != 0:
            print(f"Test command failed with exit code: {exit_status}")
            print("Aborting - not running the main command.")
            return stdout_str, stderr_str, exit_status

        print("Test command succeeded. \nNow executing run command...")

        # If test succeeded, execute the run command
        print(f"Executing run command:>>>>>>>> {commands['run']}")
        stdin, stdout, stderr = client.exec_command(commands['run'], timeout=timeout)
        exit_status = stdout.channel.recv_exit_status()
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')

        # Print status
        if exit_status == 0:
            print("Run command executed successfully.")
        else:
            print(f"Run command failed with exit code: {exit_status}")

        # Return results from the run command
        return stdout_str, stderr_str, exit_status

    except Exception as e:
        print(f"Error: {str(e)}")
        return "", str(e), -1

    finally:
        # Always close the connection
        client.close()
        print("SSH connection closed.")

out, err, cod = run_import()
result = parse_perl_output(out, err, cod)
for key, value in result.items():
    print(f"{key}: {value}")





import smtplib
from email.message import EmailMessage

def email_notifier(body: str):
    msg = EmailMessage()
    msg["Subject"] = "ImportTxns Alert"
    msg["From"] = "eddychetz@gmail.com"
    msg["To"] = "eddwin@eroute2market.co.za"
    msg.set_content(body)

    with smtplib.SMTP("smtp.yourdomain", 587) as s:
        s.starttls()
        s.login("smtp_user", "smtp_pass")
        s.send_message(msg)


run_import(notifier=email_notifier)

import paramiko
from typing import Optional, Tuple, Callable

def run_import_commands(
    timeout: int = 120,
    notifier: Optional[Callable[[str], None]] = None,
) -> Tuple[str, str, int]:
    """
    SSH into the remote system and execute two commands sequentially:
      1) Test: /usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1
      2) Run : /usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data
    The second command runs only if the first exits with code 0.

    Args:
        hostname: SSH server hostname.
        port: SSH port (verify 28 is correct; SSH default is 22).
        username: SSH username ("toby").
        password: SSH password. If None, the connection will fail unless you adapt to prompt or key auth.
        timeout: Timeout for each command in seconds.
        notifier: Optional callback that receives a string message for alerts.

    Returns:
        (stdout, stderr, exit_code) of the last executed command:
          - If test fails, returns the test command's tuple.
          - If test succeeds, returns the run command's tuple.
          - On SSH-level error, returns ("", error_message, -1).
    """
    def default_notifier(msg: str) -> None:
        print(f"[ALERT] {msg}")

    if notifier is None:
        notifier = default_notifier

    commands = {
        "test": "/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1",
        "run":  "/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1",
    }

    # Access login credentials
    hostname: str = os.getenv('hostname')
    port = os.getenv('port')
    username = os.getenv('username')
    password = os.getenv('password')

    client = paramiko.SSHClient()
    try:
        # Production-hardening (preferred):
        # client.load_system_host_keys()
        # client.set_missing_host_key_policy(paramiko.RejectPolicy())

        # Convenience (less secure): auto-accept unknown hosts
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            look_for_keys=False,
            allow_agent=False,
            timeout=30,
        )

        # ---- Run TEST ----
        t_out, t_err, t_code = _exec(client, commands["test"], timeout)
        if t_code != 0:
            notifier(
                "Test command failed; main command not executed.\n"
                f"Exit code: {t_code}\n"
                f"STDERR:\n{(t_err or '').strip() or '(empty)'}\n"
                f"STDOUT:\n{(t_out or '').strip() or '(empty)'}"
            )
            return t_out, t_err, t_code

        # ---- Run MAIN ----
        r_out, r_err, r_code = _exec(client, commands["run"], timeout)
        if r_code != 0:
            notifier(
                "Run command failed even though test passed.\n"
                f"Exit code: {r_code}\n"
                f"STDERR:\n{(r_err or '').strip() or '(empty)'}\n"
                f"STDOUT:\n{(r_out or '').strip() or '(empty)'}"
            )
        return r_out, r_err, r_code

    except paramiko.AuthenticationException:
        msg = "Authentication failed. Check username/password or switch to key-based auth."
        notifier(msg)
        return "", msg, -1
    except paramiko.SSHException as e:
        msg = f"SSH error: {e}"
        notifier(msg)
        return "", msg, -1
    except Exception as e:
        msg = f"Unexpected error: {e}"
        notifier(msg)
        return "", msg, -1
    finally:
        try:
            client.close()
        except Exception:
            pass


def _exec(client: paramiko.SSHClient, cmd: str, timeout: int) -> Tuple[str, str, int]:
    """
    Execute a command via an existing SSH connection and return (stdout, stderr, exit_code).
    """
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    # Block until the command completes and get the exit status
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    return out, err, exit_code

# Simple console alerts (default)
stdout, stderr, code = run_import_commands()
print("Exit:", code)
print("STDOUT:\n", stdout)
print("STDERR:\n", stderr)

# Or send alerts to Microsoft Teams via an incoming webhook:
# import json, urllib.request
# def teams_notifier(body: str):
#     url = "https://outlook.office.com/webhook/..."  # your Teams incoming webhook
#     req = urllib.request.Request(
#         url,
#         data=json.dumps({"text": body}).encode("utf-8"),
#         headers={"Content-Type": "application/json"},
#         method="POST"
#     )
#     with urllib.request.urlopen(req) as resp: _ = resp.read()
#
# stdout, stderr, code = run_import_commands(
#     username="toby",
#     password="your_password_here",
#     notifier=teams_notifier
# )


