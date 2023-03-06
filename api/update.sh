#!/bin/bash
# script to update VpcEndpoint for s3
BUCKET_NAME="aws-glue-scripts-492436075634-us-west-2"
#aws cloudformation --profile='grove-prod' deploy \
aws cloudformation deploy \
  --no-fail-on-empty-changeset \
  --template-file VpcEndpointUpdater.yaml \
  --stack-name ${BUCKET_NAME}-VPCE-Updater \
  --parameter-overrides \
      BucketName=$BUCKET_NAME