import io
import os

from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive, GoogleDriveFile
from pydrive2.files import MediaIoReadable

GDRIVE_SCOPE = "https://www.googleapis.com/auth/drive"


class Environment:
    def __init__(self):
        self.INVESTIGATIONS_GDRIVE_FOLDER = self._get_env_var("INVESTIGATIONS_GDRIVE_FOLDER")
        self.GOOGLE_CREDENTIALS_FILE = self._get_env_var("GOOGLE_CREDENTIALS_FILE")
        self.EXPERIMENTS_S3_FOLDER = self._get_env_var("EXPERIMENTS_S3_FOLDER")
        self._setup_google_drive()
        self._existing_files: dict[str, dict[str, GoogleDriveFile]] = {}

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

    def dict_of_gdrive_files(self, folder_id: str) -> dict[str, GoogleDriveFile]:
        if folder_id in self._existing_files:
            return self._existing_files[folder_id]
        else:
            files = self._google_drive.ListFile({"q": f"trashed=false and '{folder_id}' in parents"}).GetList()
            self._existing_files[folder_id] = {f["title"]: f for f in files}
            return self._existing_files[folder_id]

    def list_gdrive_files(self, folder_id: str) -> list[GoogleDriveFile]:
        return list(self.dict_of_gdrive_files(folder_id).values())

    def read_gdrive_file_as_string(self, file_id: str) -> str:
        return self.read_gdrive_file_as_bytes(file_id).decode("utf-8")

    def read_gdrive_file_as_bytes(self, file_id: str) -> bytes:
        file = self._google_drive.CreateFile({"id": file_id})
        in_memory_file = io.BytesIO()
        buffer: MediaIoReadable = file.GetContentIOBuffer()
        return buffer.read()  # type: ignore

    def write_gdrive_file_in_folder(self, parent_folder_id: str, file_name: str, content: str) -> str:
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
                }
            )
        fh.SetContentString(content)
        fh.Upload()
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
