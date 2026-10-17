#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
elif [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if ! command -v awslocal >/dev/null 2>&1; then
  echo "ERROR: awslocal not found; activate .venv or install awscli-local." >&2
  exit 1
fi

ACCOUNT_ID="$(awslocal sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.us-east-1.localhost.localstack.cloud:4566"
IMAGE_URI="${REGISTRY}/fraud-scoring:latest"

if awslocal ecr describe-repositories --repository-names fraud-scoring >/dev/null 2>&1; then
  echo "ECR repository fraud-scoring already exists."
else
  awslocal ecr create-repository --repository-name fraud-scoring >/dev/null
  echo "Created ECR repository fraud-scoring."
fi

docker build -t fraud-scoring:latest -f scoring/Dockerfile .
docker tag fraud-scoring:latest "$IMAGE_URI"
awslocal ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin "$REGISTRY"
docker push "$IMAGE_URI"

if awslocal lambda get-function --function-name fraud-scoring >/dev/null 2>&1; then
  echo "Updating existing Lambda fraud-scoring."
  awslocal lambda update-function-code \
    --function-name fraud-scoring \
    --image-uri "$IMAGE_URI" >/dev/null
else
  echo "Creating Lambda fraud-scoring."
  awslocal lambda create-function \
    --function-name fraud-scoring \
    --package-type Image \
    --code "ImageUri=${IMAGE_URI}" \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 60 \
    --memory-size 1024 \
    --environment 'Variables={AWS_ENDPOINT_URL=http://localhost:4566,S3_BUCKET=telco-fraud-bronze}' >/dev/null
fi

for attempt in $(seq 1 60); do
  STATE="$(awslocal lambda get-function --function-name fraud-scoring \
    --query 'Configuration.State' --output text)"
  UPDATE_STATUS="$(awslocal lambda get-function --function-name fraud-scoring \
    --query 'Configuration.LastUpdateStatus' --output text 2>/dev/null || true)"
  if [[ "$STATE" == "Active" && ( "$UPDATE_STATUS" == "Successful" || "$UPDATE_STATUS" == "None" || -z "$UPDATE_STATUS" ) ]]; then
    break
  fi
  if [[ "$STATE" == "Failed" || "$UPDATE_STATUS" == "Failed" ]]; then
    echo "ERROR: Lambda deployment failed (state=$STATE, update=$UPDATE_STATUS)." >&2
    exit 1
  fi
  sleep 1
done

awslocal lambda get-function --function-name fraud-scoring \
  --query 'Configuration.{State:State,Size:CodeSize}'
FUNCTION_ARN="$(awslocal lambda get-function --function-name fraud-scoring \
  --query 'Configuration.FunctionArn' --output text)"
echo "Function ARN: ${FUNCTION_ARN}"
echo "OK: fraud-scoring is deployed."

# TODO(second-iteration): add S3 PutObject trigger on
# gold/model_features/ via `awslocal lambda put-function-event-invoke-config`
# or an EventBridge rule. Kept manual (`make score-day`) for now so
# operators have a dry-run gate.
