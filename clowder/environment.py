import io
import os
from typing import Any, Optional
import yaml
import warnings
from pathlib import Path
import gspread

warnings.filterwarnings("ignore", r"Blowfish")

from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive, GoogleDriveFile
from pydrive2.files import MediaIoReadable

GDRIVE_SCOPE = "https://www.googleapis.com/auth/drive"


class ClowderMeta:
    def __init__(self, meta_filepath: str) -> None:
        self.filepath = meta_filepath
        with open(meta_filepath, "r") as f:
            self.data: Any = yaml.safe_load(f)

    def flush(self):
        with open(self.filepath, "w") as f:
            yaml.safe_dump(self.data, f)
        with open(self.filepath, "r") as f:
            self.data: Any = yaml.safe_load(f)


class Environment:
    def __init__(self):
        self.meta = ClowderMeta("../.clowder/clowder.master.meta.yml")
        self.INVESTIGATIONS_GDRIVE_FOLDER = self.meta.data["current_root"]  # TODO
        self.GOOGLE_CREDENTIALS_FILE = (
            self._get_env_var("GOOGLE_CREDENTIALS_FILE")
            if os.environ.get("GOOGLE_CREDENTIALS_FILE") is not None
            else "../.clowder/"
            + list(filter(lambda p: "clowder" in p and ".json" in p, os.listdir("../.clowder/")))[0]  # TODO
        )
        self.EXPERIMENTS_S3_FOLDER = "clowder"  # self._get_env_var("EXPERIMENTS_S3_FOLDER")
        self._setup_google_drive()
        self._existing_files: dict[str, dict[str, GoogleDriveFile]] = {}
        self.gc = gspread.service_account(filename=Path(self.GOOGLE_CREDENTIALS_FILE))

    @property
    def root(self):
        return self.meta.data["current_root"]

    @root.setter
    def root(self, value):
        self.meta.data["current_root"] = value
        self.meta.flush()

    @property
    def current_meta(self):
        return self.meta.data[self.root]

    def _get_env_var(self, name: str) -> str:
        var = os.environ.get(name)
        assert var is not None, name + " needs to be set."
        return var

    def _setup_google_drive(self):
        gauth = GoogleAuth()
        gauth.auth_method = "service"
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.GOOGLE_CREDENTIALS_FILE, scopes=GDRIVE_SCOPE
        )
        self._google_drive = GoogleDrive(gauth)

    def dict_of_gdrive_files(self, folder_id: str) -> "dict[str, GoogleDriveFile]":
        if folder_id in self._existing_files:
            return self._existing_files[folder_id]
        else:
            files = self._google_drive.ListFile({"q": f"trashed=false and '{folder_id}' in parents"}).GetList()
            self._existing_files[folder_id] = {f["title"]: f for f in files}
            return self._existing_files[folder_id]

    def list_gdrive_files(self, folder_id: str) -> "list[GoogleDriveFile]":
        return list(self.dict_of_gdrive_files(folder_id).values())

    def read_gdrive_file_as_string(self, file_id: str) -> str:
        return self.read_gdrive_file_as_bytes(file_id).decode("utf-8")

    def read_gdrive_file_as_bytes(self, file_id: str) -> bytes:
        file = self._google_drive.CreateFile({"id": file_id})
        in_memory_file = io.BytesIO()
        buffer: MediaIoReadable = file.GetContentIOBuffer()
        return buffer.read()  # type: ignore

    def write_gdrive_file_in_folder(
        self, parent_folder_id: str, file_name: str, content: str, file_type: Optional[str] = None
    ) -> str:
        files = self.dict_of_gdrive_files(parent_folder_id)
        if file_name in files:
            # overwrite the existing folder
            fh = self._google_drive.CreateFile({"id": files[file_name]["id"]})
        else:
            # create a new folder
            fh = self._google_drive.CreateFile(
                {
                    "title": file_name,
                    "parents": [{"id": parent_folder_id}],
                    "mimeType": "text/plain" if file_type is None else file_type,
                }
            )
        fh.SetContentString(content)
        fh.Upload()
        return fh["id"]

    def delete_gdrive_folder(self, folder_id: str) -> str:
        fh = self._google_drive.CreateFile({"id": folder_id})
        fh.Delete()
        return fh["id"]

    def create_gdrive_folder(self, folder_name: str, parent_folder_id: str) -> str:
        files = self.dict_of_gdrive_files(parent_folder_id)
        if folder_name in files:
            # Do nothing - return the existing folder id
            return files[folder_name]["id"]
        # create a new folder
        fh = self._google_drive.CreateFile(
            {
                "title": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [{"id": parent_folder_id}],
            }
        )
        fh.Upload()
        return fh["id"]


ENV = Environment()
