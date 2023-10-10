from clowder.environment import ENV
from clowder.experiments_for_investigation import ExperimentsForInvestigation


class Investigation:
    def __init__(self, folder_id: str):
        self.folder_id = folder_id
        self._checK_investigation()
        self.experiments = ExperimentsForInvestigation(self.folder_id)

    def _checK_investigation(self):
        pass

    def _setup_investigation(self, experiment_id: str):
        pass
