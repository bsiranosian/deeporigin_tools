# pylint: disable=no-member, broad-exception-raised, duplicate-code
import concurrent.futures
import os
import shutil
from typing import Any, Optional

import pandas as pd
import requests
import tqdm
from deeporigin import auth
from deeporigin.data_hub import api


class ELNBackup:
    """
    Class for backing up and exporting data from our ELN, the Deep Origin Data Hub.
    """

    @staticmethod
    def get_full_database_export(db_id: str) -> pd.DataFrame:
        """
        Get a full export of a Deep Origin database, including metadata about each row.

        :param db_id: The ID of the database to export
        :return: A pandas DataFrame with the database contents and metadata
        """
        assert auth.tokens_exist(), "No auth tokens, please authenticate."
        try:
            this_db = api.get_dataframe(db_id, return_type="dataframe")
        except TypeError:
            return pd.DataFrame()
        metadata_dict = {}
        for row_id in this_db.index:
            metadata_dict[row_id] = dict(api.describe_row(row_id=row_id))
        metadata_df = pd.DataFrame(metadata_dict).T

        final_df = pd.DataFrame(this_db).join(metadata_df)
        return final_df

    @staticmethod
    def get_row_notebook_json(row_id: str) -> Any:
        """
        Get the JSON representation of a Deep Origin database row, including the notebook content.

        :param row_id: The ID of the row to get
        :return: A dictionary with the row content
        """
        assert auth.tokens_exist(), "No auth tokens, please authenticate."
        url = "https://os.deeporigin.io/nucleus-api/api/DescribeRow"
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Content-Type": "application/json",
            "x-org-id": auth.get_config()["organization_id"],
            "authorization": f'Bearer {auth.get_tokens()["access"]}',
            "Origin": "https://os.deeporigin.io",
            "DNT": "1",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Priority": "u=4",
            "TE": "trailers",
        }
        payload = {"rowId": row_id, "fields": True}
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        if response.status_code != 200:
            raise ValueError(f"Failed to get row {row_id}: {response.text}")
        return response.json()["data"]

    @staticmethod
    def get_download_url(file_id: str) -> str:
        """
        Get a presigned download URL for a file in the Deep Origin OS.

        :param file_id: The ID of the file to download
        :return: The download URL
        """
        assert auth.tokens_exist(), "No auth tokens, please authenticate."
        url = "https://os.deeporigin.io/nucleus-api/api/CreateFileDownloadUrl"
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "content-type": "application/json",
            "x-org-id": auth.get_config()["organization_id"],
            "authorization": f'Bearer {auth.get_tokens()["access"]}',
            "Origin": "https://os.deeporigin.io",
            "DNT": "1",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Priority": "u=4",
            "TE": "trailers",
        }
        payload = {"fileId": file_id}

        response = requests.post(url, headers=headers, json=payload, timeout=60)
        if response.status_code != 200:
            raise ValueError(f"Failed to get download URL for file {file_id}: {response.text}")
        download_url = str(response.json()["data"]["downloadUrl"])
        return download_url

    # TODO: Add size and modification time checks, possibly to the next function
    @staticmethod
    def download_file(
        file_id: str,
        local_path: Optional[str] = None,
        skip_if_exists: bool = True,
    ) -> None:
        """
        Download a file from the Deep Origin OS.

        :param file_id: The ID of the file to download
        :param local_path: The local path to save the file to. If None, the file will be saved with the same name as the file ID.
        :param skip_if_exists: If True, skip the download if the file already exists locally
        """
        if local_path is None:
            local_path = file_id
        if skip_if_exists and os.path.exists(local_path):
            return

        download_url = ELNBackup.get_download_url(file_id)
        with requests.get(download_url, stream=True, timeout=60) as r:
            if r.status_code != 200:
                raise Exception(f"Failed to download file {file_id}")
            with open(local_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

    @staticmethod
    def download_all_files(local_dir: str, n_workers: int = 8) -> None:
        """
        Download all files from the Deep Origin OS to a local directory.

        :param local_dir: The local directory to save the files to
        :param n_workers: The number of parallel download workers to use
        """
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        all_files_metadata = api.list_files()
        file_metadata_df = pd.DataFrame([dict(dict(file)["file"]) for file in all_files_metadata])
        file_metadata_df.set_index("id", inplace=True)
        file_metadata_df.to_csv(os.path.join(local_dir, "file_metadata.csv"))

        # sort by size to download largest files first, so all threads stay busy
        file_metadata_df.sort_values("content_length", ascending=False, inplace=True)

        total_download_size_mb = int(file_metadata_df["content_length"].sum() / 1e6)
        print(f"Downloading {len(file_metadata_df)} files to {local_dir} (total size: {total_download_size_mb} MB)")
        print("(Skipping files that already exist in local_dir)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = []
            for file_id in file_metadata_df.index:
                futures.append(executor.submit(ELNBackup.download_file, file_id, os.path.join(local_dir, file_id)))

            for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                future.result()
