### Cloud project

Before deploying or calling the cloud functions, the "project" can be set once with the following call, or passed to each gcloud call.

```
gcloud config set project openet
```

To enable task logging when run locally, the GOOGLE_APPLICATION_CREDENTIALS environment variable will need to be set to a local copy of the project GEE key file.

```
# Mac/Linux
export GOOGLE_APPLICATION_CREDENTIALS="/Users/mortonc/Projects/keys/openet-gee.json"
```

### Deploying the cloud function

The following are the parameters that were set when deploying the function for the first time.  Subsequent deployments only need the project if not set above.

```
gcloud functions deploy disalexi-insolation-hourly --project openet --runtime python37 --entry-point cron_scheduler --trigger-http --allow-unauthenticated --memory 512 --timeout 540 --max-instances 1 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com"
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

Daily update
```
gcloud scheduler jobs update http disalexi-insolation-hourly --schedule "5 20,22 * * SUN" --uri "https://us-central1-openet.cloudfunctions.net/disalexi-insolation-hourly" --description "DisALEXI Hourly Insolation Update" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```

Historical ingest from OpenET bucket archive
```
gcloud scheduler jobs update http disalexi-insolation-hourly-historical --schedule "*/20 * * * *" --uri "https://us-central1-openet.cloudfunctions.net/disalexi-insolation-hourly?start=2016-01-01&end=2021-12-31" --description "DisALEXI Hourly Insolation Historical" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 1 --attempt-deadline=540s --min-backoff=30s
```

### Archiving the geotiffs in the OpenET bucket

```
gsutil -m cp gs://meteo_insol_data/insoldata_tif_perband/insol_series_2022*.tif gs://openet/disalexi/insoldata_tif/
```