from typing import Optional
from clowder.environment import ENV
from clowder.investigation import Investigation
from clowder.status import Status
import yaml
from clearml import Task
import s3path
from io import StringIO

from gspread import Worksheet
import gspread_dataframe as gd
import pandas as pd

# TODO logging


def untrack(investigation_name: Optional[str], inactive_only: bool = False) -> bool:
    # search for investigation(s) # TODO
    # for name, data in results...
    if investigation_name is not None:
        investigation_name = _clean_name(investigation_name)
    del ENV.current_meta["investigations"][investigation_name]
    ENV.meta.flush()
    return True


def track(investigation_name: Optional[str], active_only: bool = False) -> bool:
    # search for investigation(s) # TODO
    # for name, data in results...
    if investigation_name is not None:
        investigation_name = _clean_name(investigation_name)
    if investigation_name in ENV.current_meta["investigations"]:
        raise Exception(
            f"There is already an investigation with name {investigation_name} in this context"
        )  # TODO custom exception
    investigation: dict = {"dirty_name": investigation_name, "id": "TBD", "status": str(Status.Created)}  # TODO
    ENV.current_meta["investigations"][investigation_name] = investigation
    ENV.meta.flush()
    return True


def duplicate(from_investigation_name: str, new_investigation_name: str) -> bool:
    create(new_investigation_name)
    # ... Copy contents in gdrive
    try:
        ENV.current_meta["investigations"][new_investigation_name] = ENV.meta.data[ENV.root]["investigations"][
            from_investigation_name
        ]
        ENV.meta.flush()
    except:
        delete(new_investigation_name)
    return True


def delete(investigation_name: str, delete_from_clearml: bool = True, delete_from_google_drive: bool = True) -> bool:
    if delete_from_clearml:
        pass  # TODO
    if delete_from_google_drive:
        id = idfor(investigation_name)
        ENV.delete_gdrive_folder(id)
    untrack(investigation_name)
    return True


def idfor(investigation_name: str) -> str:
    """Returns GDrive ID for investigation with name `investigation_name` in current context"""  # TODO throws...
    if investigation_name not in ENV.current_meta["investigations"]:
        raise Exception(f"Investigation {investigation_name} does not exist in the current context")
    return ENV.current_meta["investigations"][investigation_name]["id"]


def urlfor(investigation_name: str) -> str:
    """Returns url for investigation with name `investigation_name` in current context"""  # TODO throws...
    return f"https://drive.google.com/drive/u/0/folders/{idfor(investigation_name)}"


def cancel(investigation_name: Optional[str], only_if_pending: bool = False) -> bool:
    return True


def run(investigation_name: str, force_rerun: bool = False) -> bool:
    if investigation_name is not None:
        investigation_name = _clean_name(investigation_name)
        if ENV.current_meta["investigations"][investigation_name]["status"] == str(Status.Running):
            return False
        id = idfor(investigation_name)
        investigation = Investigation(id, investigation_name)
        investigation.setup_investigation()
        investigation.start_investigation()
        ENV.current_meta["investigations"][investigation_name]["status"] = str(Status.Running)
        ENV.meta.flush()
        sync(investigation_name)
    return True


def status(investigation_name: Optional[str], _sync: bool = True) -> str:  # TODO change return/format
    """Returns status of investigation with name `investigation_name` in the current context"""  # TODO throws...
    if _sync:
        sync(investigation_name)
    if investigation_name is not None:
        investigation_name = _clean_name(investigation_name)
        if investigation_name in ENV.current_meta["investigations"]:
            return ENV.current_meta["investigations"][investigation_name]["status"]
        raise Exception(f"Investigation {investigation_name} does not exist in the current context")
    return "\n".join([f"{inv_k}:\t{inv_v['status']}" for inv_k, inv_v in ENV.current_meta["investigations"].items()])


def sync(investigation_name: Optional[str]) -> bool:  # TODO , polling_interval_minutes: int = 0
    if investigation_name is not None:
        investigation_name = _clean_name(investigation_name)
        return _internal_sync(investigation_name)
    val = True
    for investigation in ENV.current_meta["investigations"]:
        val = val and _internal_sync(investigation)
    return val


def _internal_sync(investigation_name: str) -> bool:
    # Fetch info from clearml
    clearml_tasks_dict: dict[str, Task] = ENV.sync_clearml_tasks(investigation_name)
    # Update gdrive, fetch
    meta_folder_id = ENV.current_meta["investigations"][investigation_name]["clowder_meta_yml_id"]
    remote_meta_content = yaml.safe_load(ENV.read_gdrive_file_as_string(meta_folder_id))
    if len(clearml_tasks_dict) > 0:
        if "experiments" not in remote_meta_content:
            remote_meta_content["experiments"] = {}
        for name, task in clearml_tasks_dict.items():
            if name not in remote_meta_content["experiments"]:
                remote_meta_content["experiments"][name] = {}
            remote_meta_content["experiments"][name]["clearml_id"] = task.id
            remote_meta_content["experiments"][name]["status"] = task.get_status().capitalize()  # TODO map status
    ENV.write_gdrive_file_in_folder(
        idfor(investigation_name), "clowder.meta.yml", yaml.dump(remote_meta_content), "application/x-yaml"
    )

    statuses = []

    # Update locally
    for exp in ENV.current_meta["investigations"][investigation_name]["experiments"].keys():
        ENV.current_meta["investigations"][investigation_name]["experiments"][exp]["clearml_id"] = remote_meta_content[
            "experiments"
        ][exp]["clearml_id"]
        ENV.current_meta["investigations"][investigation_name]["experiments"][exp]["status"] = remote_meta_content[
            "experiments"
        ][exp]["status"]
        statuses.append(remote_meta_content["experiments"][exp]["status"])
    ENV.current_meta["investigations"][investigation_name]["status"] = str(
        Status.from_clearml_tasks_status(statuses, ENV.current_meta["investigations"][investigation_name]["status"])
    )
    ENV.meta.flush()  # TODO url
    if ENV.current_meta["investigations"][investigation_name]["status"] == str(Status.Completed):
        _generate_results(investigation_name)
    return True


def _generate_results(investigation_name: str):
    spreadsheet = ENV.gc.open_by_key(ENV.current_meta["investigations"][investigation_name]["sheet_id"])
    setup_sheet: Worksheet = list(filter(lambda s: s.title == "ExperimentsSetup", spreadsheet.worksheets()))[0]
    setup_df = pd.DataFrame(setup_sheet.get_all_records())
    results: dict[str, pd.DataFrame] = {}
    for _, row in setup_df.iterrows():
        for filename in row["results-csvs"].split(";"):
            s3_filepath: s3path.S3Path = (
                ENV.EXPERIMENTS_S3_FOLDER
                / ENV.current_meta["investigations"][investigation_name]["id"]
                / row["name"]
                / filename
            )
            if filename not in results:
                results[filename] = pd.DataFrame()
            with s3_filepath.open() as f:
                results[filename] = pd.concat(
                    [results[filename], pd.read_csv(StringIO(f.read()))], join="outer", ignore_index=True
                )

    for filename, df in results.items():
        s = spreadsheet.add_worksheet(filename, rows=0, cols=0)
        gd.set_with_dataframe(s, df)
        ranking_df = _rank_fields(df)
        for row_index, row in ranking_df.iterrows():
            col_index = 0
            for col in df.columns:
                ref = s.cell(
                    row_index + 2, col_index + 1  # type: ignore
                ).address  # +2 = 1 + 1 - 1 for zero- vs. one-indexed and 1 to skip column names
                col: str
                r, g, b = _color_func(row[col] / (len(ranking_df.index)) - 1)
                s.format(f"{ref}", {"backgroundColor": {"red": r, "green": g, "blue": b}})

                col_index += 1


def _rank_fields(df: pd.DataFrame):
    series = []
    df = df.select_dtypes(include="number")
    for col in df.columns:
        srtd = df.sort_values(by=col)
        s = srtd.index
        series.append(s)
    ret = pd.DataFrame(series).transpose()
    ret.columns = df.columns
    return ret


def _color_func(x: float) -> tuple:
    if x > 0.5:
        return (209 - (209 - 27) * (x - 0.5) / 0.5, 209, 27)
    return (209, 27 + (209 - 27) * x / 0.5, 27)


def create(investigation_name: str) -> bool:
    """Create an empty investigation with name `investigation_path`"""
    # ... Generate folder etc.
    investigation_name = _clean_name(investigation_name)
    if investigation_name in ENV.current_meta["investigations"]:
        raise Exception(
            f"There is already an investigation with name {investigation_name} in this context"
        )  # TODO custom exception
    folder_id = ENV.create_gdrive_folder(investigation_name, ENV.root)

    remote_meta_content: dict = {"name": investigation_name, "id": folder_id, "clearml_id": "TBD"}  # TODO static names!
    clowder_meta_yml_id = ENV.write_gdrive_file_in_folder(
        folder_id, "clowder.meta.yml", yaml.dump(remote_meta_content), "application/x-yaml"
    )
    clowder_log_id = ENV.write_gdrive_file_in_folder(folder_id, "clowder.log", "")
    clowder_config_yml_id = ENV.write_gdrive_file_in_folder(folder_id, "config.yml", "", "application/x-yaml")
    sheet = ENV.gc.create("investigation", folder_id)
    sheet.sheet1.update_title("ExperimentsSetup")
    sheet.add_worksheet("ResultsSetup", rows=0, cols=0)  # TODO Insert format
    sheet_id = sheet.id
    experiments_folder_id = ENV.create_gdrive_folder("experiments", folder_id)

    investigation: dict = {
        "id": folder_id,
        "status": str(Status.Created),
        "experiments_folder_id": experiments_folder_id,
        "clowder_meta_yml_id": clowder_meta_yml_id,
        "clowder_log_id": clowder_log_id,
        "clowder_config_yml_id": clowder_config_yml_id,
        "sheet_id": sheet_id,
    }  # TODO
    ENV.current_meta["investigations"][investigation_name] = investigation
    ENV.meta.flush()
    return True


def use_context(root_folder_id: str) -> bool:
    """Change context to folder with id `root_folder_id` reflected in `root` field"""
    ENV.root = root_folder_id
    if root_folder_id not in ENV.meta.data:
        ENV.meta.data[root_folder_id] = {"investigations": {}}
    ENV.meta.flush()
    return True


def list_inv() -> str:  # TODO
    """Lists all investigations in the current context"""
    return "\n".join([inv for inv in ENV.current_meta["investigations"]])


def current_context() -> str:
    return ENV.root


def _clean_name(name: str):
    return name  # TODO
