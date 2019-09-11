# Relational Database Import to Big Query with Dataflow and DLP API

This is a PoC to use dataflow and DLP api to securely tokenize and import data from a relational database to big Query. The instructions below describe how to use this pipeline with a sample SQL Server database created in GKE and use of DLP template to tokenize PII data before it's persisted in Big Query.
### Before Start 

Assumes you have a GCP project ready to use. 

#### Create a SQL Server DB in GKE

Create a GKE Cluster

```
gcloud beta container --project "" clusters create "standard-cluster-1" --zone "us-central1-a" --username "admin" --cluster-version "1.10.9-gke.5" --machine-type "n1-standard-1" --image-type "COS" --disk-type "pd-standard" --disk-size "100" --scopes "https://www.googleapis.com/auth/cloud-platform" --num-nodes "3" --enable-cloud-logging --enable-cloud-monitoring --no-enable-ip-alias --network "projects/sqlserver-gke-dataflow/global/networks/default" --subnetwork "projects/sqlserver-gke-dataflow/regions/us-central1/subnetworks/default" --addons HorizontalPodAutoscaling,HttpLoadBalancing --enable-autoupgrade --enable-autorepair
	
```

Create persistent disks and reserve a public IP address

```
gcloud compute disks create --size xxxGB mssql-data
gcloud compute disks create --size xxxGB mssql-bkp

gcloud compute addresses create mssql-address --region xxxx
gcloud compute addresses list
NAME          REGION    ADDRESS        STATUS
mssql-address  xxx  XX.YYY.ZZ.XY       RESERVED

```
Create a SA Password

```
kubectl create secret generic mssql --from-literal=password=YOUR_PASSWORD

```
Create Deployment and Service  Yaml files can be found in /sqlserver folder in the repo

```
	kubectl create -f sqlserver.yaml
	
	kubectl create -f sqlserverservice.yaml (Please update the LB IP with the reserved IP created earlier)
	
```
Connect to the SQL Server instance and execute the schema file located in the sqlserver folder in the repo

```
Create a database 
Execute sampledb.sql to create schema and sample data
There are three sample tables created with 1k rows each. (Patient, Medication, Patinetleave)

```
#### Create DLP config 

DLP config is a json file that needs to stored in GCS.  This step is optional. Please only use this if you have sensitive data that needs tokenization before storing in Big Query.
Config file has a list of table name, batch zize and DLP template names.  Below example shows patient table only uses deidentify template but patientleave table uses both deidentify and inspect template. As you can see, DLP templates need to be created beforehand if you don't have them already. Please read DLP documentation if you are not familiar with DLP concepts.

```
[ 
{  
   "tableName": "patient",
   "batchSize":"500",
   "deidTemplate":"projects/{project_id_}/deidentifyTemplates/1919023077517469302"
},
{
   "tableName": "patientleave",
   "batchSize":"500",
   "deidTemplate":"projects/{project_id_}/deidentifyTemplates/5908005131307652110",
   "inspTemplate": "projects/{project_id_}/inspectTemplates/3913777919247397283"
}
]  

```
For this exaxmple, patient table  has  columns: name and age which are deidentified in big query.
patientleave table has sign_by and reason columns which are inspected for phone number and deidentified.
Please see the screen shot below at the end of read me.

### Local Build & Run

Clone the project.

Import as a gradle project in your IDE and execute gradle build or run. You can also use DirectRunner for small files.

There are number of arguments you will need before executung gradle run. 

- JDBCSpec: This is the JDBC string to connect to the database. For our example, SQL Server DB, it may look like this:   

```
JDBCSpec='jdbc:sqlserver://XX.YYY.ZZ.XY:1433;DatabaseName=XXX;user=sa;password=XXX;encrypt=true;trustServerCertificate=true'

```

- dataSet=Big Query Dataset name. Pipline will create the dataset. There is no need to create them before hand in GCP project.

```

 dataSet=sql_servers_db_migration

```

- offsetCount= Number of rows to fetch at a time from select statement. Pipline uses Split DoFn to execute queris in parallel. From our example 500 would mean that there will be two select statements (500*2=1000) fetching data form our sample tables in parallel. If the total number of rows is 1001, there will be three splits.    

```
offsetCount=500 

```

- DLPConfigBucket= GCS buckt name where DLP config file is stored. This is an optional argument. 

```
DLPConfigBucket=dlp_config

```

- DLPConfigObject= Path to JSON file with DLP configs. This is only required if data tokenization is needed.

```
DLPConfigObject=db1/dlpconfigs.json

```

- excludedTables = If you would like to exclude tables to import, please use this argument. It's optional. Format is <table_name>-<table_name> . Table names seperated by '-'

```
- excludedTables = patient-medication

```

To build locally using gradle without integration test

```
 gradle build -x test
 
```
To run integration test. It uses a bucket name (test_db_import) to GCP project. 

```
gradle test 

or

gradle build

```

Run Using Direct Runner. Please replace arguments as required.

```
gradle run -Pargs=" --project=sqlserver-gke-dataflow --runner=DirectRunner --dataSet=sql_servers_db_migration_test --JDBCSpec=jdbc:sqlserver://130.211.216.221:1433;DatabaseName=customer_db;user=sa;password=XXXX;encrypt=true;trustServerCertificate=true --tempLocation=gs://df_db_migration/temp --offsetCount=500 --DLPConfigBucket=dlp_config --DLPConfigObject=db1/dlpconfigs.json"
```

### Run Pipeline in DataFlow 

#### Using gradle run

```
gradle run -Pargs=" --project=sqlserver-gke-dataflow --runner=DataflowRunner --dataSet=sql_servers_db_migration --JDBCSpec=jdbc:sqlserver://130.211.216.221:1433;DatabaseName=customer_db;user=sa;password=XXXX;encrypt=true;trustServerCertificate=true --tempLocation=gs://df_db_migration/temp --offsetCount=500 --DLPConfigBucket=dlp_config --DLPConfigObject=db1/dlpconfigs.json"

```

#### Using dataflow template:

```
Create the template: 

gradle run -Pargs=" --project=sqlserver-gke-dataflow --runner=DataflowRunner --tempLocation=gs://df_db_migration/temp --templateLocation=gs://template-dbimport/sqlserverdb"

Execute the template:

gcloud dataflow jobs run test-run --gcs-location gs://template-dbimport/sqlserverdb --max-workers 5 --parameters JDBCSpec='jdbc:sqlserver://130.211.216.221:1433;DatabaseName=customer_db;user=sa;password=XXXX;encrypt=true;trustServerCertificate=true',offsetCount=500,DLPConfigBucket=dlp_config,DLPConfigObject=db1/dlpconfigs.json,numWorkers=2,workerMachineType=n1-highmem-4,dataSet=db_import

```

### How the Dataflow pipeline works?

This pipeline executes in following steps:

1. Create a inital PCollection using the JDBC Spec provided.   
2. Query the schema table to create list of tables and list og columns for each table
3. For each table split the select query based on offset count argument provided.  This uses split DoFn feature in dataflow.  
4. Tokenize data if DLP config exist for the table. This is based on the template supplied part of DLP config Json file.  
5. Use Dynamic destination feature in Big Query IO to create dataset and table schema as required. 
6. Pipeline uses Bigquery data load jobs (not streaming inserts) to load data.  
 

### Connecting to SQL Server with Active Directory Authentication 
In order to connect to SQL Servers with Active Directory authentication enabled, we must utilise the opensource JTDS driver. The vendor supported JDBC driver does not support AD authentication within DataFlow.

In order to authenticate using AD change your JDBC string as per the following example:
```
jdbc:jtds:sqlserver://1.2.3.4:1433;instance=test;user=svc.user;password='P@55word';useNTLMv2=true;domain=workgroup
```


### DLP Template used in this example

For Patient table - Deid template

```
{
 "name": "projects/<id>/deidentifyTemplates/1919023077517469302",
 "createTime": "2018-12-24T18:58:40.830831Z",
 "updateTime": "2018-12-24T18:58:40.830831Z",
 "deidentifyConfig": {
  "recordTransformations": {
   "fieldTransformations": [
    {
     "fields": [
      {
       "name": "name"
      }
     ],
     "primitiveTransformation": {
      "cryptoReplaceFfxFpeConfig": {
       "cryptoKey": {
        "kmsWrapped": {
         "wrappedKey": "CiQAnI+lCfD6PDVVrk9GPSD0DiePaIQRYm23azYK3JRVd1Ze+akSQQAnZK4QuSJk3Ay/2+OYgGyO7ONTmQQAghCXeKhaCQENNdqDTN4mlAjwcfXfftOf2QSdaHw0twTDoRyyOKjrCs/8",
         "cryptoKeyName": "projects/<id>/locations/global/keyRings/dbimport-kr/cryptoKeys/customerdb"
        }
       },
       "commonAlphabet": "ALPHA_NUMERIC"
      }
     }
    },
    {
     "fields": [
      {
       "name": "age"
      }
     ],
     "primitiveTransformation": {
      "cryptoReplaceFfxFpeConfig": {
       "cryptoKey": {
        "kmsWrapped": {
         "wrappedKey": "CiQAnI+lCfD6PDVVrk9GPSD0DiePaIQRYm23azYK3JRVd1Ze+akSQQAnZK4QuSJk3Ay/2+OYgGyO7ONTmQQAghCXeKhaCQENNdqDTN4mlAjwcfXfftOf2QSdaHw0twTDoRyyOKjrCs/8",
         "cryptoKeyName": "projects/<id>/locations/global/keyRings/dbimport-kr/cryptoKeys/customerdb"
        }
       },
       "customAlphabet": "123456789"
      }
     }
    }
   ]
  }
 }
}

```

For Patientleave table - Deid template


```

- Show headers -
  
{
 "name": "projects/<id>/deidentifyTemplates/5908005131307652110",
 "createTime": "2018-12-24T18:42:38.722733Z",
 "updateTime": "2018-12-24T18:42:38.722733Z",
 "deidentifyConfig": {
  "recordTransformations": {
   "fieldTransformations": [
    {
     "fields": [
      {
       "name": "sign_by"
      }
     ],
     "primitiveTransformation": {
      "cryptoReplaceFfxFpeConfig": {
       "cryptoKey": {
        "kmsWrapped": {
         "wrappedKey": "CiQAnI+lCfD6PDVVrk9GPSD0DiePaIQRYm23azYK3JRVd1Ze+akSQQAnZK4QuSJk3Ay/2+OYgGyO7ONTmQQAghCXeKhaCQENNdqDTN4mlAjwcfXfftOf2QSdaHw0twTDoRyyOKjrCs/8",
         "cryptoKeyName": "projects/<id>>/locations/global/keyRings/dbimport-kr/cryptoKeys/customerdb"
        }
       },
       "commonAlphabet": "ALPHA_NUMERIC"
      }
     }
    },
    {
     "fields": [
      {
       "name": "reason"
      }
     ],
     "infoTypeTransformations": {
      "transformations": [
       {
        "infoTypes": [
         {
          "name": "PHONE_NUMBER"
         }
        ],
        "primitiveTransformation": {
         "cryptoReplaceFfxFpeConfig": {
          "cryptoKey": {
           "kmsWrapped": {
            "wrappedKey": "CiQAnI+lCfD6PDVVrk9GPSD0DiePaIQRYm23azYK3JRVd1Ze+akSQQAnZK4QuSJk3Ay/2+OYgGyO7ONTmQQAghCXeKhaCQENNdqDTN4mlAjwcfXfftOf2QSdaHw0twTDoRyyOKjrCs/8",
            "cryptoKeyName": "projects/<id>/locations/global/keyRings/dbimport-kr/cryptoKeys/customerdb"
           }
          },
          "commonAlphabet": "NUMERIC",
          "surrogateInfoType": {
           "name": "[PHONE]"
          }
         }
        }
       }
      ]
     }
    }
   ]
  }
 }
}


```

Inspect template 


```
{
 "name": "projects/<id>/inspectTemplates/3913777919247397283",
 "createTime": "2018-12-24T17:56:43.435119Z",
 "updateTime": "2018-12-24T17:56:43.435119Z",
 "inspectConfig": {
  "infoTypes": [
   {
    "name": "PHONE_NUMBER"
   }
  ],
  "minLikelihood": "POSSIBLE",
  "limits": {
  }
 }
}

```

Some Screen Shots for Successful Run:


<img width="685" alt="mekehqxhzfq" src="https://user-images.githubusercontent.com/27572451/50616638-4b801500-0eb7-11e9-88b8-f799a71b8720.png">

<img width="1074" alt="t5qnmdqaaey" src="https://user-images.githubusercontent.com/27572451/50616658-66528980-0eb7-11e9-9ca0-e832e35cb862.png">



In BQ:



<img width="264" alt="drtwqwanhw3" src="https://user-images.githubusercontent.com/27572451/50616684-8d10c000-0eb7-11e9-8cea-4be834a04b9e.png">

<img width="746" alt="gv5skqteuxx" src="https://user-images.githubusercontent.com/27572451/50616702-a3b71700-0eb7-11e9-9f90-8c92f93b3e83.png">



### Re-Identification Pipeline from BQ to PubSub

#### To Build and Create Docker Image

```
gradle build -DmainClass=com.google.swarm.sqlserver.migration.BQReidentificationPipeline --x test

gradle jib --image=gcr.io/[PROJECT_ID]/dlp-reid-pipeline:v1 -DmainClass=com.google.swarm.sqlserver.migration.BQReidentificationPipeline

```

#### To Run

###### Create a JSON config file by using necessary parameters. 

```
{  
   "jobName":"dlp-reid-pipeline",
   "parameters":{  
   	"deidentifyTemplateName":"projects/[PROJECT_ID]/deidentifyTemplates/[ID]",
    	"inspectTemplateName":"projects/[PROJECT_ID]/inspectTemplates/[ID]",
      	"topic":"projects/[PROJECT_ID]/topics/[TOPIC_ID]",
      	"query":"[query]",
      	"columnMap":”[OPTIONAL_JSON_CLOUMN_MAP]"
   }
}

```

###### Update dynamic_template_dlp_reid.json

```
{
  "docker_template_spec": {
    "docker_image": "gcr.io/[PROJECT_ID]/dlp-reid-pipeline:v1"
  }
}

```
##### Execute runPipeline.sh

```
set -x

echo "please to use glocud make sure you completed authentication"
echo "gcloud config set project templates-user"
echo "gcloud auth application-default login"

PROJECT_ID=[PROJECT_ID]
GCS_STAGING_LOCATION=gs://[BUCKET_NAME]/log
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT_ID}/templates:launch"
JOB_NAME="dlp-reid-pipeline-`date +%Y%m%d-%H%M%S-%N`"
PARAMETERS_CONFIG="@[CONFIG_JSON]"
echo JOB_NAME=$JOB_NAME

time curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 "${TEMPLATES_LAUNCH_API}"`
 `"?validateOnly=false"`
 `"&dynamicTemplate.gcsPath=gs://[BUCKET_SPEC]/dynamic_template_dlp_reid.json"`
 `"&dynamicTemplate.stagingLocation=${GCS_STAGING_LOCATION}" \
 -d "${PARAMETERS_CONFIG}"
 
```



