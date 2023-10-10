import os

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

GDRIVE_SCOPE = "https://www.googleapis.com/auth/drive"


class Environment:
    def __init__(self):
        self.INVESTIGATIONS_GDRIVE_FOLDER = self._get_env_var("INVESTIGATIONS_GDRIVE_FOLDER")
        self.GOOGLE_CREDENTIALS_FILE = self._get_env_var("GOOGLE_CREDENTIALS_FILE")
        self.EXPERIMENTS_S3_FOLDER = self._get_env_var("EXPERIMENTS_S3_FOLDER")
        self._setup_google_drive()

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
        self.GOOGLE_DRIVE = GoogleDrive(gauth)


ENV = Environment()
