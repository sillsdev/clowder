from typing import Optional
from clowder.environment import ENV, Investigation
from clowder.status import Status
import yaml


# TODO remote logging (ignore for mvp)


def untrack(investigation_name: str):
    ENV.get_investigation(investigation_name).delete(delete_from_clearml=False, delete_from_gdrive=False)  # type: ignore


def track(investigation_name: Optional[str]):
    if investigation_name is not None:
        ENV.track_investigation_by_name(investigation_name)
    else:
        ENV.track_all_investigations()


def duplicate(from_investigation_name: str, new_investigation_name: str):
    create(new_investigation_name)
    # ... Copy contents in gdrive
    try:
        # TODO duplicate
        pass
    except:
        delete(new_investigation_name)


def delete(investigation_name: str, delete_from_clearml: bool = True, delete_from_google_drive: bool = True):
    ENV.get_investigation(investigation_name).delete(delete_from_clearml, delete_from_google_drive)


def idfor(investigation_name: str) -> str:
    """Returns GDrive ID for investigation with name `investigation_name` in current context"""
    return ENV.get_investigation(investigation_name).id


def urlfor(investigation_name: str) -> str:
    """Returns url for investigation with name `investigation_name` in current context"""
    return f"https://drive.google.com/drive/u/0/folders/{idfor(investigation_name)}"


def cancel(investigation_name: str):
    ENV.get_investigation(investigation_name).cancel()


def run(investigation_name: str, force_rerun: bool = False) -> bool:
    sync(investigation_name)

    investigation = ENV.get_investigation(investigation_name)
    if investigation.status == Status.Running:
        return False
    elif not force_rerun and investigation.status.value == Status.Completed.value:
        return False
    investigation.setup()
    now_running = investigation.start_investigation(force_rerun)
    if now_running:
        investigation.status = Status.Running
    sync(investigation_name)
    return True


def status(investigation_name: Optional[str], _sync: bool = True) -> "dict[str, Status]":
    """Returns status of investigation with name `investigation_name` in the current context"""
    if _sync:
        sync(investigation_name)
    if investigation_name is not None:
        if ENV.investigation_exists(investigation_name):
            return {investigation_name: ENV.get_investigation(investigation_name).status}
    return {inv.name: inv.status for inv in ENV.investigations}


def sync(investigation_name: Optional[str]):
    if investigation_name is not None:
        ENV.get_investigation(investigation_name).sync()
    else:
        for investigation in ENV.investigations:
            investigation.sync()


def create(investigation_name: str):
    """Create an empty investigation with name `investigation_name`"""
    ENV.create_investigation(investigation_name)


def use_context(root_folder_id: str):
    """Change context to folder with id `root_folder_id` reflected in `root` field"""
    ENV.root = root_folder_id
    if root_folder_id not in ENV.meta.data:
        ENV.meta.data[root_folder_id] = {"investigations": {}}
    ENV.meta.flush()


def list_inv() -> "list[Investigation]":
    """Lists all investigations in the current context"""
    return ENV.investigations


def current_context() -> str:
    return ENV.root
