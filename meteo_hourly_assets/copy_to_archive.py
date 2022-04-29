import calendar

from google.cloud import storage

# storage_client = storage.Client()
storage_client = storage.Client.from_service_account_json('./keys/openet-dri-gee.json')
src_bucket = storage_client.bucket('meteo_insol_data')
dst_bucket = storage_client.bucket('openet')
dst_folder = 'disalexi'

# for year in range(2001, 2022):
# for year in [2022]:
for year in [2021, 2022]:
    print(f'\nYear: {year}')
    for folder, prefix in [['airpressure_tif', 'psfc_series'],
                           ['temperature_tif', 't2_series'],
                           ['vaporpressure_tif', 'q2_series'],
                           ['windspeed_tif', 'wind_surface']]:
        print(folder)
        dst_files = {
            blob.name
            for blob in dst_bucket.list_blobs(prefix=f'{dst_folder}/{folder}/{prefix}_{year}')
        }
        print(len(dst_files))

        for i, src_blob in enumerate(src_bucket.list_blobs(prefix=f'{folder}/{prefix}_{year}')):
            if i % 400 == 0:
                print(src_blob.name)

            # Check if date string in file name is valid
            src_blob_year = int(src_blob.name.split('_')[-2][:4])
            src_blob_doy = int(src_blob.name.split('_')[-2][4:])
            if src_blob_doy > 365 and not calendar.isleap(src_blob_year):
                print(f'{src_blob.name} - invalid doy')
                continue

            # Rename from 3 hour index to hour
            if src_blob.name.endswith('07.tif'):
                dst_blob_name = src_blob.name.replace('_07.tif', '_21.tif')
            elif src_blob.name.endswith('06.tif'):
                dst_blob_name = src_blob.name.replace('_06.tif', '_18.tif')
            elif src_blob.name.endswith('05.tif'):
                dst_blob_name = src_blob.name.replace('_05.tif', '_15.tif')
            elif src_blob.name.endswith('04.tif'):
                dst_blob_name = src_blob.name.replace('_04.tif', '_12.tif')
            elif src_blob.name.endswith('03.tif'):
                dst_blob_name = src_blob.name.replace('_03.tif', '_09.tif')
            elif src_blob.name.endswith('02.tif'):
                dst_blob_name = src_blob.name.replace('_02.tif', '_06.tif')
            elif src_blob.name.endswith('01.tif'):
                dst_blob_name = src_blob.name.replace('_01.tif', '_03.tif')
            elif src_blob.name.endswith('00.tif'):
                dst_blob_name = src_blob.name.replace('_00.tif', '_00.tif')
            else:
                continue

            if f'{dst_folder}/{dst_blob_name}' in dst_files:
                # print('file already in bucket - skipping')
                continue

            # blob_copy = src_bucket.copy_blob(src_blob, dst_bucket, f'{dst_folder}/{dst_blob_name}')
