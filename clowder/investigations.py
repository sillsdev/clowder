from pydrive2.drive import GoogleDriveFile

from clowder.investigation import Investigation
from clowder.environment import ENV


class Investigations:
    def list_investigations(self) -> list[GoogleDriveFile]:
        return ENV.GOOGLE_DRIVE.ListFile(
            {
                "q": f"mimeType='application/vnd.google-apps.folder' and trashed=false and '{ENV.INVESTIGATIONS_GDRIVE_FOLDER}' in parents"
            }
        ).GetList()

    def get_investigation(self, folder_id: str) -> Investigation:
        return Investigation(folder_id)
