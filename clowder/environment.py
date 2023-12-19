import warnings

from clowder.functions import DuplicateInvestigationException

warnings.filterwarnings("ignore", r"Blowfish")
import os
import datetime
from typing import Any, Optional
from pathlib import Path
import gspread
from clearml import Task
import s3path
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive, GoogleDriveFile
from pydrive2.files import MediaIoReadable
from io import StringIO
from pathlib import Path
import subprocess
import pandas as pd
import re
import jinja2
from clearml import Task
import yaml
import numpy as np
from gspread import Worksheet
import gspread_dataframe as gd
from status import Status

GDRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
EXPERIMENT_PARAMETER_SPREADSHEET = "investigation"
EXPERIMENT_FOLDER = "experiments"
CLEARML_QUEUE = "jobs_backlog"


class MissingConfigurationFile(IOError):
    "Missing clowder configuration file"


class DuplicateExperimentException(Exception):
    "Duplicate experiments within investigation"


class Investigation:
    def __init__(
        self,
        id: str,
        name: str,
        experiments_folder_id: str,
        meta_id: str,
        sheet_id: str,
        log_id: str,
        status: str,
    ):
        self.id = id
        self.name = name
        self.experiments_folder_id = experiments_folder_id
        self.meta_id = meta_id
        self.sheet_id = sheet_id
        self.log_id = log_id
        self._status: Status = Status(status)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, enum: Status):
        ENV.current_meta["investigations"][self.name]["status"] = enum.value
        ENV.meta.flush()
        self._status = enum

    def _import_experiments_spreadsheet(self):
        self.files = ENV.dict_of_gdrive_files(self.id)  # TODO refactor to be consistent
        if EXPERIMENT_PARAMETER_SPREADSHEET not in self.files:
            raise MissingConfigurationFile("Missing experiments file")
        if self.files[EXPERIMENT_PARAMETER_SPREADSHEET]["mimeType"] != "application/vnd.google-apps.spreadsheet":
            raise MissingConfigurationFile(
                "Experiments file is not a google spreadsheet iof type application/vnd.google-apps.spreadsheet"
            )
        worksheet: gspread.Spreadsheet = ENV.gc.open_by_key(self.files[EXPERIMENT_PARAMETER_SPREADSHEET]["id"])
        self.experiments_df: pd.DataFrame = pd.DataFrame(worksheet.sheet1.get_all_records())
        if "name" not in self.experiments_df.columns:
            raise MissingConfigurationFile("Missing name column on sheet1 of the experiments google sheet")
        self.experiments_df.set_index(self.experiments_df.name, inplace=True)
        if "type" not in self.experiments_df.columns:
            raise MissingConfigurationFile("Missing type column on sheet1 of the experiments google sheet")

    def setup(self):
        self._import_experiments_spreadsheet()
        self.investigation_s3_path = s3path.S3Path(ENV.EXPERIMENTS_S3_FOLDER) / self.id
        experiments_folder_id = ENV.create_gdrive_folder(EXPERIMENT_FOLDER, self.id)
        ENV.current_meta["investigations"][self.name]["experiments_folder_id"] = experiments_folder_id
        ENV.meta.flush()
        for name, params in self.experiments_df.iterrows():
            experiment_folder_id = ENV.create_gdrive_folder(str(name), experiments_folder_id)
            self._setup_experiment(str(name), params, experiment_folder_id)
        ENV.copy_gdrive_folder_to_s3(experiments_folder_id, self.investigation_s3_path)

    def _setup_experiment(self, name: str, params: pd.Series, folder_id: str):
        self.silnlp_config_yml = ""
        if self.experiments_df.index.duplicated().sum() > 0:
            raise MissingConfigurationFile(
                "Duplicate names in experiments google sheet.  Each name needs to be unique."
            )
        if "config.yml" not in self.files:
            raise MissingConfigurationFile("config.yml needed for silnlp jobs")
        self.silnlp_config_yml = ENV.read_gdrive_file_as_string(self.files["config.yml"]["id"])
        rtemplate = jinja2.Environment(loader=jinja2.BaseLoader()).from_string(self.silnlp_config_yml)
        rendered_config = rtemplate.render(params.to_dict())
        ENV.write_gdrive_file_in_folder(folder_id, "config.yml", rendered_config)

    def start_investigation(self):
        if "experiments" not in ENV.current_meta["investigations"][self.name]:
            ENV.current_meta["investigations"][self.name]["experiments"] = {}
        worksheet: gspread.Spreadsheet = ENV.gc.open_by_key(self.files[EXPERIMENT_PARAMETER_SPREADSHEET]["id"])
        paramters_df: pd.DataFrame = pd.DataFrame(worksheet.sheet1.get_all_records())
        for _, row in paramters_df.iterrows():
            experiment_path: s3path.S3Path = self.investigation_s3_path / row["name"]
            result = subprocess.run(
                f"python -m {row['entrypoint']} --clearml-queue {CLEARML_QUEUE} --save-checkpoints {'/'.join(str(experiment_path.absolute()).split('/')[4:])}",
                shell=True,
                capture_output=True,
                text=True,
            )
            print(result.stdout)
            match = re.search(r"new task id=(.*)", result.stdout)
            clearml_id = match.group(1) if match is not None else "unknown"
            ENV.current_meta["investigations"][self.name]["experiments"][row["name"]] = {"clearml_id": clearml_id}

    def sync(self):
        # Fetch info from clearml
        clearml_tasks_dict: dict[str, Task] = ENV.sync_clearml_tasks(self.name)
        # Update gdrive, fetch
        meta_folder_id = ENV.current_meta["investigations"][self.name]["clowder_meta_yml_id"]
        remote_meta_content = yaml.safe_load(ENV.read_gdrive_file_as_string(meta_folder_id))
        if len(clearml_tasks_dict) > 0:
            if "experiments" not in remote_meta_content:
                remote_meta_content["experiments"] = {}
            for name, task in clearml_tasks_dict.items():
                if name not in remote_meta_content["experiments"]:
                    remote_meta_content["experiments"][name] = {}
                remote_meta_content["experiments"][name]["clearml_id"] = task.id
                remote_meta_content["experiments"][name]["status"] = task.get_status().capitalize()
        ENV.write_gdrive_file_in_folder(
            self.id, "clowder.meta.yml", yaml.safe_dump(remote_meta_content), "application/x-yaml"
        )
        statuses = []

        # Update locally
        for exp in ENV.current_meta["investigations"][self.name]["experiments"].keys():
            ENV.current_meta["investigations"][self.name]["experiments"][exp]["clearml_id"] = remote_meta_content[
                "experiments"
            ][exp]["clearml_id"]
            ENV.current_meta["investigations"][self.name]["experiments"][exp]["status"] = remote_meta_content[
                "experiments"
            ][exp]["status"]
            statuses.append(remote_meta_content["experiments"][exp]["status"])
        ENV.meta.flush()  # TODO url
        self.status = Status.from_clearml_task_statuses(statuses, self.status)  # type: ignore
        if self.status == Status.Completed.value:
            self._generate_results()
        return True

    def _generate_results(self):
        spreadsheet = ENV.gc.open_by_key(self.sheet_id)
        worksheets = spreadsheet.worksheets()
        setup_sheet: Worksheet = list(filter(lambda s: s.title == "ExperimentsSetup", worksheets))[0]
        setup_df = pd.DataFrame(setup_sheet.get_all_records())
        results: dict[str, pd.DataFrame] = {}
        for _, row in setup_df.iterrows():
            for filename in row["results-csvs"].split(";"):
                s3_filepath: s3path.S3Path = s3path.S3Path(ENV.EXPERIMENTS_S3_FOLDER) / self.id / row["name"] / filename
                with s3_filepath.open() as f:
                    df = pd.read_csv(StringIO(f.read()))
                    if "scores" in filename:
                        filename = "scores"
                        df = self._process_scores_csv(df, row["name"])
                    if filename not in results:
                        results[filename] = pd.DataFrame()
                    results[filename] = pd.concat([results[filename], df], join="outer", ignore_index=True)

        for filename, df in results.items():
            for w in spreadsheet.worksheets():
                if w.title == filename:
                    spreadsheet.del_worksheet(w)
            s = spreadsheet.add_worksheet(filename, rows=0, cols=0)
            gd.set_with_dataframe(s, df)
            ranking_df = self._rank_fields(df)
            for row_index, row in ranking_df.iterrows():
                col_index = 0
                for col in df.columns:
                    if not np.issubdtype(df.dtypes[col], np.number):
                        continue
                    ref = s.cell(
                        row_index + 2, col_index + 2  # type: ignore
                    ).address  # +2 = 1 + 1 - 1 for zero- vs. one-indexed and 1 to skip column names
                    col: str
                    r, g, b = self._color_func(row[col] / ((len(ranking_df.index)) - 1))
                    s.format(f"{ref}", {"backgroundColor": {"red": r, "green": g, "blue": b}})

                    col_index += 1

    def _process_scores_csv(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        ret = df[["score"]]
        column_names = df[["scorer"]].values.flatten()
        ret = ret.transpose()
        ret.columns = pd.Index(column_names)
        ret["BLEU-details"] = ret["BLEU"]
        ret["BLEU"] = ret["BLEU"].apply(lambda x: x.split("/")[0])
        ret[["BLEU", "CHRF3", "WER", "TER", "spBLEU"]] = ret[["BLEU", "CHRF3", "WER", "TER", "spBLEU"]].apply(
            pd.to_numeric, axis=0
        )  # TODO more robust
        ret.insert(0, "name", [name])
        return ret

    def _rank_fields(self, df: pd.DataFrame):
        series = []
        df = df.select_dtypes(include="number")
        for col in df.columns:
            srtd = df.sort_values(by=col)
            s = srtd.index
            series.append(s)
        ret = pd.DataFrame(series).transpose()
        ret.columns = df.columns
        return ret

    def _color_func(self, x: float) -> tuple:
        if x > 0.5:
            return ((209 - (209 - 27) * (x - 0.5) / 0.5) / 255, 209 / 255, 27 / 255)
        return (209 / 255, (27 + (209 - 27) * x / 0.5) / 255, 27 / 255)

    def delete(self, delete_from_clearml: bool = True, delete_from_gdrive: bool = True):
        if delete_from_clearml:
            for _, obj in ENV.current_meta["investigations"][self.name]["experiments"].items():
                task: Optional[Task] = Task.get_task(task_id=obj["clearml_id"])
                if task is not None:
                    task.delete()
        if delete_from_gdrive:
            ENV.delete_gdrive_folder(self.id)
        del ENV.current_meta["investigations"][self.name]
        ENV.meta.flush()
        self = None

    @staticmethod
    def from_meta(data: dict):
        name = list(data.keys())[0]
        data = data[name]
        return Investigation(
            id=data["id"],
            name=name,
            experiments_folder_id=data["experiments_folder_id"],
            log_id=data["clowder_log_id"],
            sheet_id=data["sheet_id"],
            meta_id=data["clowder_meta_yml_id"],
            status=data["status"],
        )


# TODO copy results into gdrvie - link


class InvestigationNotFoundError(Exception):
    """No such investigation in the current context"""


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
        self.EXPERIMENTS_S3_FOLDER = (
            "/aqua-ml-data/MT/experiments/clowder/"  # self._get_env_var("EXPERIMENTS_S3_FOLDER")
        )
        self._setup_google_drive()
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

    @property
    def investigations(self) -> "list[Investigation]":
        return [self.get_investigation(inv_name) for inv_name in self.current_meta["investigations"].keys()]

    def get_investigation(self, investigation_name: str) -> Investigation:
        inv_data = self.current_meta["investigations"].get(investigation_name, None)
        if inv_data is None:
            raise InvestigationNotFoundError(
                f"Investigation {investigation_name} does not exist in the current context"
            )
        return Investigation.from_meta({investigation_name: inv_data})

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
        files = self._google_drive.ListFile({"q": f"trashed=false and '{folder_id}' in parents"}).GetList()
        return {f["title"]: f for f in files}

    def list_gdrive_files(self, folder_id: str) -> "list[GoogleDriveFile]":
        return list(self.dict_of_gdrive_files(folder_id).values())

    def read_gdrive_file_as_string(self, file_id: str) -> str:
        return self.read_gdrive_file_as_bytes(file_id).decode("utf-8")

    def read_gdrive_file_as_bytes(self, file_id: str) -> bytes:
        file = self._google_drive.CreateFile({"id": file_id})
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

    def sync_clearml_tasks(self, investigation_name: str):
        if "experiments" not in self.current_meta["investigations"][investigation_name]:
            self.current_meta["investigations"][investigation_name]["experiments"] = {}
        experiments = self.current_meta["investigations"][investigation_name]["experiments"]
        tasks = {}
        for experiment_name, obj in experiments.items():
            task: Optional[Task] = Task.get_task(task_id=obj["clearml_id"])
            tasks[experiment_name] = task
        return tasks

    def find_investigations(self, folder_id: str, files_acc: "set[str]" = set()) -> "set[str]":
        files = self.dict_of_gdrive_files(folder_id)
        for filename, file in files.items():
            if filename == "clowder.meta.yml":
                files_acc.add(folder_id)
            if file["mimeType"] == "application/vnd.google-apps.folder":
                files_acc = files_acc.union(self.find_investigations(file["id"], files_acc))
        return files_acc

    def track_investigation_in_folder(self, folder_id: str):
        files = self.dict_of_gdrive_files(folder_id)
        meta_file = files.get("clowder.meta.yml", None)
        if meta_file is None:
            raise MissingConfigurationFile(f"No clowder.meta.yml file could be found in folder with id {folder_id}")
        remote_meta = yaml.safe_load(self.read_gdrive_file_as_string(meta_file["id"]))
        if "experiments_folder_id" not in remote_meta:
            experiments_folder_id = self.create_gdrive_folder("experiments", folder_id)
        else:
            experiments_folder_id = remote_meta["experiments_folder_id"]

        if "sheet_id" not in remote_meta:
            sheet = self.gc.create("investigation", folder_id)
            sheet.sheet1.update_title("ExperimentsSetup")
            sheet_id = sheet.id
        else:
            sheet_id = remote_meta["sheet_id"]

        if "clowder_log_id" not in remote_meta:
            clowder_log_id = self.write_gdrive_file_in_folder(folder_id, "clowder.log", "")
        else:
            clowder_log_id = remote_meta["clowder_log_id"]

        if "clowder_config_yml_id" not in remote_meta:
            clowder_config_yml_id = ENV.write_gdrive_file_in_folder(folder_id, "config.yml", "", "application/x-yaml")
        else:
            clowder_config_yml_id = remote_meta["clowder_config_yml_id"]

        folder = self._google_drive.CreateFile({"id": folder_id})
        investigation_name = folder["id"]
        if self.investigation_exists(investigation_name):
            raise DuplicateInvestigationException(
                f"There is already an investigation with name {investigation_name} in this context"
            )
        self.add_investigation(
            investigation_name,
            {
                "id": folder_id,
                "status": remote_meta["status"],
                "experiments_folder_id": experiments_folder_id,
                "clowder_meta_yml_id": meta_file["id"],
                "clowder_log_id": clowder_log_id,
                "clowder_config_yml_id": clowder_config_yml_id,
                "sheet_id": sheet_id,
            },
        )

    def log(self, investigation_name: str, data: str):
        current_log = self.read_gdrive_file_as_string(
            self.current_meta["investigation"][investigation_name]["clowder_log_id"]
        )
        new_id = self.write_gdrive_file_in_folder(
            self.current_meta["investigation"][investigation_name]["id"],
            "clowder.log",
            current_log + "\n" + datetime.datetime.now().isoformat() + " | " + data,
        )
        self.current_meta["investigation"][investigation_name]["clowder_log_id"] = new_id
        self.meta.flush()

    def copy_gdrive_folder_to_s3(self, folder_id: str, s3_path: s3path.S3Path):
        # print(f"Copying folder {folder_id} to {s3_path}")
        for file in ENV.list_gdrive_files(folder_id):
            s3_file = s3_path / file["title"]
            if file["mimeType"] == "application/vnd.google-apps.folder":
                self.copy_gdrive_folder_to_s3(file["id"], s3_file)
            else:
                with s3_file.open("wb") as f:
                    f.write(ENV.read_gdrive_file_as_bytes(file["id"]))

    def investigation_exists(self, investigation_name: str):
        return investigation_name in self.current_meta["investigations"]

    def add_investigation(self, investigation_name: str, investigation_data: dict):
        ENV.current_meta["investigations"][investigation_name] = investigation_data
        ENV.meta.flush()


ENV = Environment()
