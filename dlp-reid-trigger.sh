#!/usr/bin/env bash
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.#!/usr/bin/env bash
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
 
set -x 

PROJECT_ID=$1
GCS_TEMP_LOCATION=$2
INSPECT_TEMPLATE_NAME=$3
DEID_TEMPLATE_NAME=$4
QUERY=$5
TOPIC=$6
API_KEY=$7

# publicly hosted image
DYNAMIC_TEMPLATE_BUCKET_SPEC=gs://dynamic-template/dynamic_template_dlp_reid.json
COLUMN_MAP='"{\"card_number\": \"Card Number\", \"card_holders_name\": \"Card Holder'\''s Name\"}"'
JOB_NAME="dlp-df-reid-pipeline-`date +%Y%m%d-%H%M%S-%N`"
echo $JOB_NAME
GCS_STAGING_LOCATION="gs://$GCS_TEMP_LOCATION/temp"
TEMP_LOCATION='$GCS_STAGING_LOCATION'
PARAMETERS_CONFIG='{  
   "jobName":"'$JOB_NAME'",
   "parameters":{  
      "workerMachineType": "n1-standard-8",
      "numWorkers":"1",
      "maxNumWorkers":"9",
	  "inspectTemplateName":"'$INSPECT_TEMPLATE_NAME'",
	  "deidentifyTemplateName":"'$DEID_TEMPLATE_NAME'",
	  "query":"'$QUERY'",
	  "topic":"projects/'${PROJECT_ID}'/topics/'$TOPIC'",
	  "columnMap":'$COLUMN_MAP' 
	  	  
	}
}'
DF_API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${DF_API_ROOT_URL}/v1b3/projects/${PROJECT_ID}/templates:launch"
curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer ${API_KEY}" \
 "${TEMPLATES_LAUNCH_API}"`
 `"?validateOnly=false"`
 `"&dynamicTemplate.gcsPath=${DYNAMIC_TEMPLATE_BUCKET_SPEC}"` \
 `"&dynamicTemplate.stagingLocation=${GCS_STAGING_LOCATION}" \
 -d "${PARAMETERS_CONFIG}"
