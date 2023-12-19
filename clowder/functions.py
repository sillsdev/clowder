from typing import Optional
from clowder.environment import ENV
from clowder.status import Status
import yaml


# TODO remote logging
class DuplicateInvestigationException(Exception):
    """There is already an investigation in the current context with that name"""


def untrack(investigation_name: Optional[str], inactive_only: bool = False) -> bool:
    # search for investigation(s) # TODO
    # for name, data in results...
    if investigation_name is not None:
        ENV.get_investigation(investigation_name).delete()  # type: ignore
    return True


def track(investigation_name: Optional[str], active_only: bool = False) -> bool:
    # search for investigation(s) # TODO
    # for name, data in results...
    if investigation_name is not None:
        if ENV.investigation_exists(investigation_name):
            raise DuplicateInvestigationException(
                f"There is already an investigation with name {investigation_name} in this context"
            )
    # TODO
    return True


def duplicate(from_investigation_name: str, new_investigation_name: str) -> bool:
    create(new_investigation_name)
    # ... Copy contents in gdrive
    try:
        # TODO
        pass
    except:
        delete(new_investigation_name)
    return True


def delete(investigation_name: str, delete_from_clearml: bool = True, delete_from_google_drive: bool = True) -> bool:
    ENV.get_investigation(investigation_name).delete(delete_from_clearml, delete_from_google_drive)
    return True


def idfor(investigation_name: str) -> str:
    """Returns GDrive ID for investigation with name `investigation_name` in current context"""  # TODO throws...

    return ENV.get_investigation(investigation_name).id


def urlfor(investigation_name: str) -> str:
    """Returns url for investigation with name `investigation_name` in current context"""  # TODO throws...
    return f"https://drive.google.com/drive/u/0/folders/{idfor(investigation_name)}"


def cancel(investigation_name: Optional[str], only_if_pending: bool = False) -> bool:
    return True


def run(investigation_name: str, force_rerun: bool = False) -> bool:
    sync(investigation_name)

    investigation = ENV.get_investigation(investigation_name)
    if investigation.status == Status.Running:
        return False
    investigation.setup()
    investigation.start_investigation()
    investigation.status = Status.Running
    sync(investigation_name)
    return True


def status(investigation_name: Optional[str], _sync: bool = True) -> "dict[str, Status]":
    """Returns status of investigation with name `investigation_name` in the current context"""  # TODO throws...
    if _sync:
        sync(investigation_name)
    if investigation_name is not None:
        if ENV.investigation_exists(investigation_name):
            return {investigation_name: ENV.get_investigation(investigation_name).status}
    return {inv.name: inv.status for inv in ENV.investigations}


def sync(investigation_name: Optional[str]) -> bool:  # TODO , polling_interval_minutes: int = 0
    if investigation_name is not None:
        return ENV.get_investigation(investigation_name).sync()
    val = True
    for investigation in ENV.investigations:
        val = val and investigation.sync()
    return val


def create(investigation_name: str) -> bool:
    """Create an empty investigation with name `investigation_path`"""
    # ... Generate folder etc.

    if ENV.investigation_exists(investigation_name):
        raise DuplicateInvestigationException(
            f"There is already an investigation with name {investigation_name} in this context"
        )
    folder_id = ENV.create_gdrive_folder(investigation_name, ENV.root)
    clowder_log_id = ENV.write_gdrive_file_in_folder(folder_id, "clowder.log", "")
    clowder_config_yml_id = ENV.write_gdrive_file_in_folder(folder_id, "config.yml", "", "application/x-yaml")
    sheet = ENV.gc.create("investigation", folder_id)
    sheet.sheet1.update_title("ExperimentsSetup")
    sheet_id = sheet.id
    experiments_folder_id = ENV.create_gdrive_folder("experiments", folder_id)
    remote_meta_content: dict = {
        "name": investigation_name,
        "id": folder_id,
        "status": Status.Created.value,
        "experiments_folder_id": experiments_folder_id,
        "clowder_log_id": clowder_log_id,
        "clowder_config_yml_id": clowder_config_yml_id,
        "sheet_id": sheet_id,
    }
    clowder_meta_yml_id = ENV.write_gdrive_file_in_folder(
        folder_id, "clowder.meta.yml", yaml.safe_dump(remote_meta_content), "application/x-yaml"
    )

    investigation_data: dict = {
        "id": folder_id,
        "status": Status.Created.value,
        "experiments_folder_id": experiments_folder_id,
        "clowder_meta_yml_id": clowder_meta_yml_id,
        "clowder_log_id": clowder_log_id,
        "clowder_config_yml_id": clowder_config_yml_id,
        "sheet_id": sheet_id,
    }  # TODO
    ENV.add_investigation(investigation_name, investigation_data)
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
    return "\n".join([inv.name for inv in ENV.investigations])


def current_context() -> str:
    return ENV.root
