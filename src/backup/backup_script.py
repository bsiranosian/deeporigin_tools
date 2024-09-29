from deeporigin import auth
from deeporigin.data_hub import api
import pandas as pd
import os
import tqdm
import json
from backup import ELNBackup
from datetime import datetime
import concurrent.futures
import argparse
import shutil

# take in outdir as an argument
parser = argparse.ArgumentParser(
    description="Backup Deep Origin Data Hub, first to a local directory then to S3. Rquires s5cmd to be installed for final backup to S3."
)
parser.add_argument("--outdir", type=str, required=True, help="Backup directory. Sub directory will be created with current date/time.")
parser.add_argument("--s3dir", type=str, help="Backup directory will be mirrored to S3 when complete at this prefix.")
args = parser.parse_args()
outdir_base = args.outdir
s3dir = args.s3dir

# detect number of cpus
max_workers = os.cpu_count()
max_workers = min(8, max_workers)

# check if authenticated on this machine yet
if not auth.tokens_exist():
    auth.authenticate()

# get current date/time
now = datetime.now()
dt_string = now.strftime("%Y-%m-%d_%H-%M-%S")
outdir_base_date = os.path.join(outdir_base, dt_string)

print(f"Backing up to {outdir_base_date} ...")

workspace_dir = os.path.join(outdir_base_date, "workspaces")
database_dir = os.path.join(outdir_base_date, "databases")
notebook_dir = os.path.join(outdir_base_date, "notebooks")
file_dir = os.path.join(outdir_base, "files")

# Create directories
os.makedirs(workspace_dir, exist_ok=True)
os.makedirs(database_dir, exist_ok=True)
os.makedirs(notebook_dir, exist_ok=True)
os.makedirs(file_dir, exist_ok=True)

# 1) workspaces and associated notebooks
workspaces = api.list_rows(row_type="workspace")
workspaces_df = pd.DataFrame([dict(workspace) for workspace in workspaces])
workspaces_df.to_csv(os.path.join(workspace_dir, "workspaces.csv"), index=False)

with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_id = {executor.submit(ELNBackup.get_row_notebook_json, id): id for id in workspaces_df["id"]}

    for future in tqdm.tqdm(concurrent.futures.as_completed(future_to_id), desc="Backing up workspaces ...", total=len(future_to_id)):
        workspace_json = future.result()
        id = future_to_id[future]
        with open(os.path.join(workspace_dir, f"{id}.json"), "w") as f:
            json.dump(workspace_json, f)

# 2) databases and associated metadata
databases = api.list_rows(row_type="database")
databases_df = pd.DataFrame([dict(database) for database in databases])
databases_df.to_csv(os.path.join(database_dir, "databases.csv"), index=False)

with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_id = {executor.submit(ELNBackup.get_full_database_export, id): id for id in databases_df["id"]}

    for future in tqdm.tqdm(concurrent.futures.as_completed(future_to_id), desc="Backing up databases  ...", total=len(future_to_id)):
        this_database_df = future.result()
        id = future_to_id[future]
        this_database_df.to_csv(os.path.join(database_dir, f"{id}.csv"), index=False)

# 3) Notebook pages for each row
rows = api.list_rows(row_type="row")
row_df = pd.DataFrame([dict(row) for row in rows])
row_df.to_csv(os.path.join(notebook_dir, "notebooks.csv"), index=False)

with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_id = {executor.submit(ELNBackup.get_row_notebook_json, id): id for id in row_df["id"]}

    for future in tqdm.tqdm(concurrent.futures.as_completed(future_to_id), desc="Backing up notebooks  ...", total=len(future_to_id)):
        notebook_json = future.result()
        id = future_to_id[future]
        with open(os.path.join(notebook_dir, f"{id}.json"), "w") as f:
            json.dump(notebook_json, f)

# 4) Files
ELNBackup.download_all_files(file_dir)

# Finally, back up all to S3 using s5cmd
# check if s5cmd is available
if shutil.which("s5cmd") is not None:
    os.system(f"s5cmd sync {outdir_base} {s3dir}")
else:
    print("s5cmd not found. Skipping S3 backup.")
    exit(1)
