### Deploying the cloud function

Before deploying or calling the cloud functions, the "project" can be set once with the following call, or passed to each gcloud call.

```
gcloud config set project openet-dri
```

The following are the parameters that were set when deploying the function for the first time.  Subsequent deployments only need the project if not set above.

```
gcloud functions deploy disalexi-insolation-hourly --project openet-dri --runtime python37 --entry-point cron_scheduler --trigger-http --allow-unauthenticated --memory 256 --timeout 540 --max-instances 1 --service-account="openet-dri@appspot.gserviceaccount.com"
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.

```
gcloud functions call disalexi-insolation-hourly --project openet-dri --data '{"start":"2021-10-01","end":"2021-10-05"}'
```

If no arguments are passed to the scheduler it will check the last 60 days for missing assets.

```
gcloud functions call disalexi-insolation-hourly --project openet-dri
```

### Scheduling the job

```
gcloud scheduler jobs update http disalexi-insolation-hourly --schedule "42 7 * * *" --uri "https://us-central1-openet-dri.cloudfunctions.net/disalexi-insolation-hourly" --description "Update Hourly Insolation Assets" --http-method POST --time-zone "UTC" --project openet-dri --max-retry-attempts 5
```
