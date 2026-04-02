# ALEXI Daily ET Asset Ingest

### Deploying the cloud function

```
gcloud functions deploy alexi-et-daily --project openet --no-gen2 --runtime python311 --region us-central1 --entry-point update --trigger-http --allow-unauthenticated --memory 512 --timeout 540 --max-instances 1 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.

```
gcloud functions call alexi-et-daily --project openet --data '{"status":"final","start":"2025-12-01","end":"2025-12-31"}'
```

If no arguments are passed to the scheduler it will check the last 3 months for missing assets.

```
gcloud functions call alexi-et-daily --project openet
```

### Scheduling the job

Check for a new "provisional" image every day.

```
gcloud scheduler jobs update http alexi-et-daily-provisional --schedule "5 23 * * *" --uri "https://us-central1-openet.cloudfunctions.net/alexi-et-daily?status=provisional" --description "ALEXI Daily ET Provisional Update" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```

Update all "early" assets every day.

```
gcloud scheduler jobs update http alexi-et-daily-early --schedule "10 23 * * *" --uri "https://us-central1-openet.cloudfunctions.net/alexi-et-daily?status=early&overwrite=true" --description "ALEXI Daily ET Early Update" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```
