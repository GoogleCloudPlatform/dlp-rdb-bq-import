#! /bin/bash
set -x

echo "please to use glocud make sure you completed authentication"
echo "gcloud config set project templates-user"
echo "gcloud auth application-default login"

PROJECT_ID=[PROJECT_ID]
GCS_STAGING_LOCATION=gs://[BUCKET_NAME]/log
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT_ID}/templates:launch"
JOB_NAME="dlp-reid-pipeline-`date +%Y%m%d-%H%M%S-%N`"
PARAMETERS_CONFIG="@reid_config.json"
echo JOB_NAME=$JOB_NAME

time curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 "${TEMPLATES_LAUNCH_API}"`
 `"?validateOnly=false"`
 `"&dynamicTemplate.gcsPath=gs://[BUCKET-SPEC]/dynamic_template_dlp_reid.json"`
 `"&dynamicTemplate.stagingLocation=${GCS_STAGING_LOCATION}" \
 -d "${PARAMETERS_CONFIG}"
 