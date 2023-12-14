from pathlib import Path
import subprocess
import gspread
import pandas as pd
import s3path
import re
from jinja2 import BaseLoader, Environment

from clowder.environment import ENV

EXPERIMENT_PARAMETER_SPREADSHEET = "investigation"
EXPERIMENT_FOLDER = "experiments"
CLEARML_QUEUE = "lambert_24gb"


class MissingConfigurationFile(IOError):
    "Missing clowder configuration file"
    pass


class Investigation:
    def __init__(self, folder_id: str, name: str):
        self.folder_id = folder_id
        self.name = name
        self.gc = gspread.service_account(filename=Path(ENV.GOOGLE_CREDENTIALS_FILE))  # type: ignore
        self._import_experiments_spreadsheet()
        self._check_silnlp_jobs()
        self._setup_experiment_s3()

    def _import_experiments_spreadsheet(self):
        self.files = ENV.dict_of_gdrive_files(self.folder_id)
        if EXPERIMENT_PARAMETER_SPREADSHEET not in self.files:
            raise MissingConfigurationFile("Missing experiments file")
        if self.files[EXPERIMENT_PARAMETER_SPREADSHEET]["mimeType"] != "application/vnd.google-apps.spreadsheet":
            raise MissingConfigurationFile(
                "Experiments file is not a google spreadsheet iof type application/vnd.google-apps.spreadsheet"
            )
        worksheet: gspread.Spreadsheet = self.gc.open_by_key(self.files[EXPERIMENT_PARAMETER_SPREADSHEET]["id"])
        self.experiments_df: pd.DataFrame = pd.DataFrame(worksheet.sheet1.get_all_records())
        if "name" not in self.experiments_df.columns:
            raise MissingConfigurationFile("Missing name column on sheet1 of the experiments google sheet")
        self.experiments_df.set_index(self.experiments_df.name, inplace=True)
        if "type" not in self.experiments_df.columns:
            raise MissingConfigurationFile("Missing type column on sheet1 of the experiments google sheet")

    def _check_silnlp_jobs(self):
        self.silnlp_config_yml = ""
        # if (self.experiments_df["type"] == "silnlp").sum() == 0: #TODO support other broad types of jobs?
        #     return
        if self.experiments_df.index.duplicated().sum() > 0:
            raise MissingConfigurationFile(
                "Duplicate names in experiments google sheet.  Each name needs to be unique."
            )
        if "config.yml" not in self.files:
            raise MissingConfigurationFile("config.yml needed for silnlp jobs")
        self.silnlp_config_yml = ENV.read_gdrive_file_as_string(self.files["config.yml"]["id"])

    def _setup_experiment_s3(self):
        self.investigation_s3_path = s3path.S3Path(ENV.EXPERIMENTS_S3_FOLDER) / self.folder_id

    def setup_investigation(self):
        experiments_folder_id = ENV.create_gdrive_folder(EXPERIMENT_FOLDER, self.folder_id)
        for name, params in self.experiments_df.iterrows():
            experiment_folder_id = ENV.create_gdrive_folder(str(name), experiments_folder_id)
            self._setup_silnlp_experiment(
                str(name), params, experiment_folder_id
            )  # TODO only silnlp jobs for now - or assumes similar setup
        self._copy_gdrive_folder_to_s3(experiments_folder_id, self.investigation_s3_path)

    def _setup_silnlp_experiment(self, name: str, params: pd.Series, folder_id: str):
        rtemplate = Environment(loader=BaseLoader()).from_string(self.silnlp_config_yml)  # TODO iterate across types
        rendered_config = rtemplate.render(params.to_dict())
        ENV.write_gdrive_file_in_folder(folder_id, "config.yml", rendered_config)

    def _copy_gdrive_folder_to_s3(self, folder_id: str, s3_path: s3path.S3Path):
        # print(f"Copying folder {folder_id} to {s3_path}")
        for file in ENV.list_gdrive_files(folder_id):
            s3_file = s3_path / file["title"]
            if file["mimeType"] == "application/vnd.google-apps.folder":
                self._copy_gdrive_folder_to_s3(file["id"], s3_file)
            else:
                with s3_file.open("wb") as f:
                    f.write(ENV.read_gdrive_file_as_bytes(file["id"]))

    def start_investigation(self):
        if "experiments" not in ENV.current_meta["investigations"][self.name]:
            ENV.current_meta["investigations"][self.name]["experiments"] = {}
        worksheet: gspread.Spreadsheet = self.gc.open_by_key(self.files[EXPERIMENT_PARAMETER_SPREADSHEET]["id"])
        paramters_df: pd.DataFrame = pd.DataFrame(worksheet.sheet1.get_all_records())
        for _, row in paramters_df.iterrows():
            experiment_path: s3path.S3Path = self.investigation_s3_path / row["name"]
            result = subprocess.run(
                f"python -m {row['entrypoint']} --clearml-queue {CLEARML_QUEUE} {'/'.join(str(experiment_path.absolute()).split('/')[4:])}",
                shell=True,
                capture_output=True,
                text=True,
            )
            print(result.stdout)
            match = re.search(r"new task id=(.*)", result.stdout)
            clearml_id = match.group(1) if match is not None else "unknown"
            ENV.current_meta["investigations"][self.name]["experiments"][row["name"]] = {"clearml_id": clearml_id}
