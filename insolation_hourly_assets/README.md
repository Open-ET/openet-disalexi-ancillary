# CFSR Hourly Insolation Asset Ingest

### Deploying the cloud function

```
gcloud functions deploy disalexi-insolation-hourly --project openet --no-gen2 --runtime python311 --region us-central1 --entry-point update --trigger-http --allow-unauthenticated --memory 1024 --timeout 360 --max-instances 1 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.

```
gcloud functions call disalexi-insolation-hourly --project openet --data '{"start":"2021-10-01","end":"2021-10-05"}'
```

If no arguments are passed to the scheduler it will check the last 3 months for missing assets.

```
gcloud functions call disalexi-insolation-hourly --project openet
```

### Scheduling the job

Update every day
```
gcloud scheduler jobs udpate http disalexi-insolation-hourly --schedule "5 13 * * *" --uri "https://us-central1-openet.cloudfunctions.net/disalexi-insolation-hourly" --description "CFSR/DisALEXI Hourly Insolation Update" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```
