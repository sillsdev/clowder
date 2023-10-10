# clowder
A herd of Computer Aided Translation (CAT) experiments

## How to install
* Install Poetry
* Establish local user google credentials:
  * Install and initialize the gcloud cli: https://cloud.google.com/sdk/docs/install
  * create your credential file: `gcloud auth application-default login`
  * https://cloud.google.com/docs/authentication/provide-credentials-adc#local-dev
  * Request that you are added to the service accout clowder@clowder-400318.iam.gserviceaccount.com
  * Make the default application access the service account: `gcloud auth application-default login --impersonate-service-account clowder@clowder-400318.iam.gserviceaccount.com --scopes="openid,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/sqlservice.login,https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/drive.resource,https://www.googleapis.com/auth/drive.file"`