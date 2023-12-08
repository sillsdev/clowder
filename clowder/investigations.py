# from pydrive2.drive import GoogleDriveFile

# from clowder.environment import ENV
# from clowder.investigation import Investigation


# class Investigations:
#     def list_investigations(self) -> "list[GoogleDriveFile]":
#         return ENV._google_drive.ListFile(
#             {
#                 "q": f"mimeType='application/vnd.google-apps.folder' and trashed=false and '{ENV.INVESTIGATIONS_GDRIVE_FOLDER}' in parents"
#             }
#         ).GetList()

#     def get_investigation(self, folder_id: str) -> Investigation:
#         return Investigation(folder_id)
