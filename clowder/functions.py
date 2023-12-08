from typing import Optional
from clowder.environment import ENV
from clowder.investigation import Investigation
import yaml

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
    investigation: dict = {"dirty_name": investigation_name, "id": "TBD", "status": "Created"}  # TODO
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
        url = urlfor(investigation_name)
        ENV.delete_gdrive_folder(url)
    untrack(investigation_name)
    return True


def urlfor(investigation_name: str) -> str:  # TODO idfor AND urlfor
    """Returns GDrive ID for investigation with name `investigation_name` in current context"""  # TODO throws...
    if investigation_name not in ENV.current_meta["investigations"]:
        raise Exception(f"Investigation {investigation_name} does not exist in the current context")
    return ENV.current_meta["investigations"][investigation_name]["id"]


def cancel(investigation_name: Optional[str], only_if_pending: bool = False) -> bool:
    return True


def run(investigation_name: Optional[str], force_rerun: bool = False) -> bool:
    if investigation_name is not None:
        investigation_name = _clean_name(investigation_name)
        url = urlfor(investigation_name)
        investigation = Investigation(url)
        investigation.setup_investigation()
        # run #TODO
        ENV.read_gdrive_file_as_string("")  # TODO keep all fileids
        ENV.current_meta["investigations"][investigation_name]["status"] = "Completed"
        ENV.meta.flush()
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


def sync(investigation_name: Optional[str], polling_interval_minutes: int = 0) -> bool:
    # Fetch infor from clearml
    # Update gdrive, fetch
    # Update locally
    if investigation_name is not None:
        investigation_name = _clean_name(investigation_name)
    return True


def create(investigation_name: str) -> bool:
    """Create an empty investigation with name `investigation_path`"""
    # ... Generate folder etc.
    investigation_name = _clean_name(investigation_name)
    if investigation_name in ENV.current_meta["investigations"]:
        raise Exception(
            f"There is already an investigation with name {investigation_name} in this context"
        )  # TODO custom exception
    folder_id = ENV.create_gdrive_folder(investigation_name, ENV.root)

    remote_meta_content: dict = {"name": investigation_name, "id": folder_id, "clearml_id": "TBD"}
    ENV.write_gdrive_file_in_folder(folder_id, "clowder.meta.yml", yaml.dump(remote_meta_content), "application/x-yaml")
    ENV.write_gdrive_file_in_folder(folder_id, "clowder.log", "")
    ENV.write_gdrive_file_in_folder(folder_id, "config.yml", "", "application/x-yaml")
    ENV.gc.create("investigation-results", folder_id)
    ENV.gc.create("investigation-parameters", folder_id)

    experiments_folder_id = ENV.create_gdrive_folder("experiments", folder_id)

    investigation: dict = {"id": folder_id, "status": "Created", "experiments_folder_id": experiments_folder_id}  # TODO
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


def list() -> str:  # TODO
    """Lists all investigations in the current context"""
    return "\n".join([inv for inv in ENV.current_meta["investigations"]])


def current_context() -> str:
    return ENV.root


def _clean_name(name: str):
    return name  # TODO
