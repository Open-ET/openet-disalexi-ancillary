import argparse
import calendar
import logging
import time

from google.cloud import storage

# storage_client = storage.Client()
storage_client = storage.Client.from_service_account_json('../../keys/openet-dri-gee.json')
src_bucket = storage_client.bucket('meteo_insol_data')
dst_bucket = storage_client.bucket('openet')
dst_folder = 'disalexi'


def main(start_year, end_year, overwrite_flag=False):
    """"""
    for year in range(start_year, end_year+1):
        print(f'\nYear: {year}')

        for folder, prefix in [
                ['airpressure_tif', 'psfc_series'],
                ['temperature_tif', 't2_series'],
                ['vaporpressure_tif', 'q2_series'],
                ['windspeed_tif', 'wind_surface']
            ]:
            logging.info(folder)

            dst_files = {
                blob.name
                for blob in dst_bucket.list_blobs(prefix=f'{dst_folder}/{folder}/{prefix}_{year}')
            }
            logging.info(len(dst_files))

            for i, src_blob in enumerate(src_bucket.list_blobs(prefix=f'{folder}/{prefix}_{year}')):
                if i % 400 == 0:
                    logging.info(src_blob.name)

                # Check if date string in file name is valid
                src_blob_year = int(src_blob.name.split('_')[-2][:4])
                src_blob_doy = int(src_blob.name.split('_')[-2][4:])
                if src_blob_doy > 365 and not calendar.isleap(src_blob_year):
                    logging.info(f'{src_blob.name} - invalid doy, skipping')
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

                if f'{dst_folder}/{dst_blob_name}' in dst_files and not overwrite_flag:
                    logging.info(f'{src_blob.name} - file already in bucket, skipping')
                    continue

                logging.info(f'{src_blob.name} - copying')
                for i in range(6):
                    try:
                        blob_copy = src_bucket.copy_blob(
                            src_blob, dst_bucket, f'{dst_folder}/{dst_blob_name}'
                        )
                        break
                    except Exception as e:
                        time.sleep(i ** 3)
                        logging.info(f'{src_blob.name} - exception, retrying')


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Archive DisALEXI ancillary meteorology assets ',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=int, metavar='YYYY', default=2022, help='Start year')
    parser.add_argument(
        '--end', type=int, metavar='YYYY', default=2022, help='End year (inclusive)')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Overwrite existing files')
    # parser.add_argument(
    #     '-v', '--variables', nargs='+', metavar='VAR',
    #     choices=VARIABLES, default=VARIABLES,
    #     help=f'DisALEXI Meteorology Variables ({", ".join(VARIABLES)})')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(
        start_year=args.start,
        end_year=args.end,
        overwrite_flag=args.overwrite,
    )
