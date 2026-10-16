#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

BUILD_DIR="infra/build"
mkdir -p "$BUILD_DIR"

package_lambda() {
  local name="$1" source_dir="$2" handler="$3" zip_path="$BUILD_DIR/$1.zip"
  (cd "$source_dir" && zip -q -r "../../$zip_path" . -x '__pycache__/*' '*.pyc')
  awslocal lambda create-function \
    --function-name "$name" \
    --runtime python3.11 \
    --handler "$handler" \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 30 \
    --zip-file "fileb://$zip_path"
}

package_lambda activation-ingest ingest/activation_ingest lambda_function.lambda_handler
package_lambda purchase-ingest ingest/purchase_ingest lambda_function.lambda_handler

awslocal lambda create-event-source-mapping \
  --function-name activation-ingest \
  --event-source-arn "$(awslocal kinesis describe-stream --stream-name sim-activation-stream --query 'StreamDescription.StreamARN' --output text)" \
  --batch-size 100 --starting-position LATEST --enabled

awslocal lambda create-event-source-mapping \
  --function-name purchase-ingest \
  --event-source-arn "$(awslocal kinesis describe-stream --stream-name bundle-purchases-stream --query 'StreamDescription.StreamARN' --output text)" \
  --batch-size 100 --starting-position LATEST --enabled

awslocal lambda list-event-source-mappings
