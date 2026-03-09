# CFSR 3-Hourly Meteorology Asset Ingest

### Deploying the cloud function

```
gcloud functions deploy disalexi-meteorology-hourly --project openet --no-gen2 --runtime python311 --region us-central1 --entry-point update --trigger-http --allow-unauthenticated --memory 512 --timeout 540 --max-instances 1 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.

```
gcloud functions call disalexi-meteorology-hourly --project openet --data '{"start":"2021-10-01","end":"2021-10-05"}'
```

If no arguments are passed to the scheduler it will check the last 60 days for missing assets.

```
gcloud functions call disalexi-meteorology-hourly --project openet
```

### Scheduling the job

Update every day

```
gcloud scheduler jobs udpate http disalexi-meteorology-hourly --schedule "5 14 * * *" --uri "https://us-central1-openet.cloudfunctions.net/disalexi-meteorology-hourly" --description "CFSR/DisALEXI 3-Hourly Meteorology Update" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```
