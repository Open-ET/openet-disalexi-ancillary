### Cloud project

Before deploying or calling the cloud functions, the "project" can be set once with the following call, or passed to each gcloud call.

```
gcloud config set project openet
```

### Deploying the cloud function

The following are the parameters that were set when deploying the function for the first time.  Subsequent deployments only need the project if not set above.

```
gcloud functions deploy disalexi-insolation-daily --project openet --no-gen2 --runtime python311 --region us-central1 --entry-point cron_scheduler --trigger-http --allow-unauthenticated --memory 512 --timeout 540 --max-instances 1 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.

```
gcloud functions call disalexi-insolation-daily --project openet --data '{"region":"conus","start":"2022-06-01","end":"2022-06-10"}'
```

If no date arguments are passed to the scheduler it will check the last 4 months for missing assets.

```
gcloud functions call disalexi-insolation-daily --project openet --data '{"region":"conus"}'
```

### Scheduling the job

Update every Sunday afternoon
```
gcloud scheduler jobs update http disalexi-insolation-daily --schedule "5 23 * * SUN" --uri "https://us-central1-openet.cloudfunctions.net/disalexi-insolation-daily?region=conus" --description "DisALEXI Daily Insolation CONUS" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```

Update CONUS every 10 days before interpolation
```
gcloud scheduler jobs update http disalexi-insolation-daily --schedule "0 7 5,15,25 * *" --uri "https://us-central1-openet.cloudfunctions.net/disalexi-insolation-daily?region=conus" --description "DisALEXI Daily Insolation CONUS" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```
