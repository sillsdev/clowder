import s3path
from clowder.environment import ENV


class ExperimentsForInvestigation:
    def __init__(self, investigation_id: str):
        self.investigation_id = investigation_id
        self.clowder_path = s3path.S3Path(ENV.EXPERIMENTS_S3_FOLDER)
        self.investigation_path = self.clowder_path / investigation_id
        self._checK_investigation()

    def _checK_investigation(self):
        if self.investigation_path.exists():
            print("investigation already exists: " + self.investigation_id)
        else:
            print("investigation does not exist: " + self.investigation_id)

    def _setup_investigation(self, experiment_id: str):
        pass
