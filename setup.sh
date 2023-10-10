#!/bin/bash

#Exporting Project ID
export PROJECT_ID=[] # Add your Project ID

export SUFFIX=fgfgr # Create  

export JOB_NAME=trips-stream-job-${SUFFIX}

export JOB_BUCKET=spark-jobs-${SUFFIX}


#Create a Job Bucket and copy source code
gcloud storage buckets create gs://${JOB_BUCKET} --project=${PROJECT_ID}
gsutil cp main.py gs://${JOB_BUCKET}/pubsub-lite-to-iceberg-stream-${SUFFIX}/

#Coping required jar dependancies to Job bucket.
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.1.0/iceberg-spark-runtime-3.3_2.12-1.1.0.jar
gsutil cp postgresql-42.5.1.jar gs://${JOB_BUCKET}/dependencies/
gsutil cp iceberg-spark-runtime-3.3_2.12-1.1.0.jar gs://${JOB_BUCKET}/dependencies/
rm iceberg-spark-runtime-3.3_2.12-1.1.0.jar
rm postgresql-42.5.1.jar


export CHECKPOINT_BUCKET=checkpoint-location-${SUFFIX}

#Creating Chechpoint Bucket
gcloud storage buckets create gs://${CHECKPOINT_BUCKET} --project=${PROJECT_ID}

export LAKE_BUCKET=iceberg-lake-${SUFFIX}

#Creatring a lake bucket.
gcloud storage buckets create gs://${LAKE_BUCKET} --project=${PROJECT_ID}

export TOPIC_NAME=trips-${SUFFIX}

#Creating Pubsub Lite reservation
gcloud pubsub lite-reservations create ${TOPIC_NAME}-reservation --project=${PROJECT_ID} --location=us-central1 --throughput-capacity=2

gcloud pubsub lite-topics create ${TOPIC_NAME} --project=${PROJECT_ID} --location=us-central1 --partitions=2 --per-partition-bytes=30GiB --throughput-reservation=${TOPIC_NAME}-reservation

#Create Pubsub Lite subscription.
gcloud pubsub lite-subscriptions create datastream-${SUFFIX}-subscription --location=us-central1 --topic=${TOPIC_NAME} --project=${PROJECT_ID}

export METASTORE_NAME=iceberg-metastore

gcloud metastore services create ${METASTORE_NAME} --location=us-central1 --tier=DEVELOPER --network=default --hive-metastore-version=3.1.2 --project=${PROJECT_ID}

export METASTORE_ENDPOINT=$(gcloud metastore services describe ${METASTORE_NAME} --project ${PROJECT_ID} --location=us-central1 --format="value(endpointUri)")

export METASTORE_WAREHOUSE=$(gcloud metastore services describe ${METASTORE_NAME} --project ${PROJECT_ID} --location=us-central1 --format="value(hiveMetastoreConfig.configOverrides[hive.metastore.warehouse.dir])")

export DATAPROC_SA_NAME=dataproc-worker-sa

gcloud iam service-accounts create ${DATAPROC_SA_NAME} --description="Service account for a dataproc worker" --display-name=${DATAPROC_SA_NAME} --project ${PROJECT_ID}

gcloud projects add-iam-policy-binding ${PROJECT_ID} --member=serviceAccount:${DATAPROC_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com --role=roles/dataproc.worker

gcloud projects add-iam-policy-binding ${PROJECT_ID} --member=serviceAccount:${DATAPROC_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com --role=roles/pubsublite.subscriber

gcloud projects add-iam-policy-binding ${PROJECT_ID} --member=serviceAccount:${DATAPROC_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com --role=roles/pubsublite.viewer

# Create Dataproc Cluster
export DATAPROC_CLUSTER_NAME=streaming-cluster

gcloud dataproc clusters create $DATAPROC_CLUSTER_NAME --region=us-central1 --project=${PROJECT_ID} \
--service-account=${DATAPROC_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
--network=default \
--bucket=${CHECKPOINT_BUCKET} \
--num-masters=1 \
--master-machine-type=n1-standard-2 \
--master-boot-disk-size=30 \
--master-boot-disk-type=pd-ssd \
--num-workers=2 \
--worker-machine-type=n1-standard-2 \
--worker-boot-disk-size=30 \
--worker-boot-disk-type=pd-ssd \
--num-worker-local-ssds=1 \
--num-secondary-workers=0 \
--image-version=2.1.8-ubuntu20 \
--dataproc-metastore=projects/${PROJECT_ID}/locations/us-central1/services/${METASTORE_NAME}

export CLOUDSDK_PYTHON_SITEPACKAGES=1
gcloud pubsub lite-topics publish ${TOPIC_NAME} --location=us-central1 --message="{'vendor_id':25,'trip_id': 1000474,'trip_distance': 2.3999996185302734,'fare_amount': 42.13,'store_and_fwd_flag': 'Y'}" --project=${PROJECT_ID}
gcloud pubsub lite-topics publish ${TOPIC_NAME} --location=us-central1 --message="{'vendor_id':26,'trip_id': 1000474,'trip_distance': 2.3999996185302734,'fare_amount': 42.13,'store_and_fwd_flag': 'Y'}" --project=${PROJECT_ID}


gcloud dataproc jobs submit pyspark gs://${JOB_BUCKET}/pubsub-lite-to-iceberg-stream-${SUFFIX}/main.py \
--cluster=${DATAPROC_CLUSTER_NAME} \
--region=us-central1 \
--project=${PROJECT_ID} \
--jars=gs://${JOB_BUCKET}/dependencies/postgresql-42.5.1.jar,gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-1.0.0-with-dependencies.jar,gs://${JOB_BUCKET}/dependencies/iceberg-spark-runtime-3.3_2.12-1.1.0.jar \
--properties spark.app.name=batch_iceberg,spark.hive.metastore.uris=${METASTORE_ENDPOINT},spark.hive.metastore.warehouse.dir=${METASTORE_WAREHOUSE} \
-- --lake_bucket=gs://${LAKE_BUCKET}/datalake/ \
--psub_subscript_id=projects/${PROJECT_ID}/locations/us-central1/subscriptions/datastream-${SUFFIX}-subscription \
--checkpoint_location=gs://${CHECKPOINT_BUCKET}/checkpoint/