# Container image for scoring Lambda.
# Loads model from s3://telco-fraud-bronze/models/fraud_gbm/v1/ at cold
# start, caches in module-level dict for warm reuse. See
# scripts/measure_cold_start.py for measured cold vs warm latency.
FROM public.ecr.aws/lambda/python:3.13

WORKDIR /var/task

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scoring/ ./scoring/
COPY model/ ./model/

CMD ["scoring.score_day.handler"]
