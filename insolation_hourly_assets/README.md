### Deploying the cloud function

Before deploying or calling the cloud functions, the "project" can be set once with the following call, or passed to each gcloud call.

```
gcloud config set project openet-dri
```

The following are the parameters that were set when deploying the function for the first time.  Subsequent deployments only need the project if not set above.

```
gcloud functions deploy disalexi-insolation-hourly --project openet-dri --runtime python37 --entry-point cron_scheduler --trigger-http --allow-unauthenticated --memory 512 --timeout 540 --max-instances 1 --service-account="openet-dri@appspot.gserviceaccount.com"
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.

```
gcloud functions call disalexi-insolation-hourly --project openet-dri --data '{"start":"2021-10-01","end":"2021-10-05"}'
```

If no arguments are passed to the scheduler it will check the last 3 months for missing assets.

```
gcloud functions call disalexi-insolation-hourly --project openet-dri
```

### Scheduling the job

Historical Ingest
```
gcloud scheduler jobs update http disalexi-insolation-hourly-historical --schedule "*/20 * * * *" --uri "https://us-central1-openet-dri.cloudfunctions.net/disalexi-insolation-hourly?start=2001-01-01&end=2021-12-31" --description "DisALEXI Hourly Insolation Historical" --http-method POST --time-zone "UTC" --project openet-dri --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```

Daily Update
```
gcloud scheduler jobs update http disalexi-insolation-hourly --schedule "42 7 * * *" --uri "https://us-central1-openet-dri.cloudfunctions.net/disalexi-insolation-hourly" --description "DisALEXI Hourly Insolation Update" --http-method POST --time-zone "UTC" --project openet-dri --location us-central1 --max-retry-attempts 5 --attempt-deadline=540s --min-backoff=30s
```

