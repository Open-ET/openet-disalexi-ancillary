import argparse
from datetime import datetime, timedelta
import json
import logging
import os
import pprint
import re
import shutil
import time

from dateutil.relativedelta import relativedelta
import ee
from flask import abort, Response
from google.cloud import storage
import numpy as np
import rasterio
import requests

import openet.core.utils as utils

SOURCE_URL = 'https://nssrgeo.ndc.nasa.gov/SPoRT/land_surface_products/alexi_et'
ASSET_COLL_ID = 'projects/openet/assets/alexi/conus/daily/v006'
# ASSET_COLL_ID = 'projects/ee-tulipyangyun-2/assets/alexi/ALEXI_V006'
ASSET_DT_FMT = '%Y%m%d'
BUCKET_NAME = 'openet'
BUCKET_FOLDER = 'disalexi/alexi_et_tif'
ALEXI_VERSION = 'V10E'
ISO_DT_FMT = '%Y-%m-%dT%H00'
# Maximum number of new tasks that can be submitted in a function call
NEW_TASKS = 300
# Maximum number of queued tasks (intentionally not setting to 3000)
MAX_TASKS = 1000
# NODATA_VALUE = -9999
# START_DAY_OFFSET = 30
START_DAY_OFFSET = 90
END_DAY_OFFSET = 1
STORAGE_CLIENT = storage.Client()
TIF_PREFIX = f'EDAY_ALEXI_{ALEXI_VERSION}'
# TIF_PREFIX = 'EDAY_ALEXI_V10E_EARLY'
TIF_NAME_FMT = '{prefix}_{status}_{date}.tif'
TIF_DT_FMT = '%Y%m%d'
TIF_DT_RE = '(?P<date>\d{8})'
# TODO: Check the units
UNITS = 'MJ m-2 d-1'
STATUS = ['early', 'provisional', 'final']

if 'FUNCTION_REGION' in os.environ:
    # Logging is not working correctly in cloud functions for Python 3.8+
    # Following workflow suggested in this issue:
    # https://issuetracker.google.com/issues/124403972
    import google.cloud.logging
    log_client = google.cloud.logging.Client(project='openet')
    log_client.setup_logging(log_level=20)
    import logging
    # CGM - Not sure if these lines are needed or not
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
else:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.getLogger('earthengine-api').setLevel(logging.INFO)
    logging.getLogger('googleapiclient').setLevel(logging.ERROR)
    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

if 'FUNCTION_REGION' in os.environ:
    # Assume code is deployed to a cloud function
    logging.debug(f'\nInitializing GEE using application default credentials')
    import google.auth
    credentials, project_id = google.auth.default(
        default_scopes=['https://www.googleapis.com/auth/earthengine']
    )
    ee.Initialize(credentials, project=project_id)


def ingest(tgt_dt, status, variable='et', overwrite_flag=False):
    """

    Parameters
    ----------
    tgt_dt : datetime
    status : {'early', 'provisional', 'final'}
    variables : {'et'}, optional
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """
    logging.info(f'ALEXI Daily {variable.upper()} {status} - {tgt_dt.strftime("%Y-%m-%d")}')
    # response = f'ALEXI Daily {variable.upper()}} - {tgt_dt.strftime("%Y-%m-%d")}'

    tif_name = TIF_NAME_FMT.format(
        prefix=TIF_PREFIX, status=status.upper(), date=tgt_dt.strftime(TIF_DT_FMT)
    )
    local_ws = os.path.join(os.getcwd(), variable, tgt_dt.strftime(f'%Y%m%d'))
    local_path = os.path.join(local_ws, tif_name)
    source_path = f'{SOURCE_URL}/{status.lower()}/{tif_name}'
    bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{tif_name}'
    asset_id = f'{ASSET_COLL_ID}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    export_name = f'alexi_daily_{variable}_{tgt_dt.strftime("%Y%m%d")}'

    logging.debug(f'  {source_path}')
    logging.debug(f'  {local_path}')
    logging.debug(f'  {bucket_path}')
    logging.debug(f'  {asset_id}')
    logging.debug(f'  {export_name}')

    if ee.data.getInfo(asset_id):
        if overwrite_flag:
            try:
                ee.data.deleteAsset(asset_id)
            except Exception as e:
                return f'{export_name} - An error occurred while trying to '\
                       f'delete the existing asset, skipping\n{e}\n'
        else:
            return f'{export_name} - The asset already exists and overwrite '\
                   f'is False, skipping\n'

    # Always overwrite temporary files if the asset doesn't exist
    if os.path.isdir(local_ws):
        shutil.rmtree(local_ws)
    if not os.path.isdir(local_ws):
        os.makedirs(local_ws)

    # Download the image from the server
    if not os.path.isfile(local_path) or overwrite_flag:
        logging.debug('  Downloading source image')
        url_download(source_path, local_path)
    if not os.path.isfile(local_path):
        return f'{export_name} - Image was not downloaded, skipping\n'

    # # Set the nodata parameter and tile the geotiff
    # with rasterio.open(local_path) as src:
    #     data = src.read()
    #     profile = src.profile.copy()
    # profile.update(
    #     # [0.04,0,-125.02,0,-0.04,49.78]
    #     transform=rasterio.transform.from_origin(-125.02, 49.78, 0.04, 0.04),
    #     # tiled=True, blockxsize=256, blockysize=256,
    #     # nodata=-9999
    # )
    # with rasterio.open(local_path, "w", **profile) as dst:
    #     dst.write(data)

    # Copy the file to the bucket for ingest and archiving
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{BUCKET_FOLDER}/{tif_name}')
    if blob and (overwrite_flag or not blob.exists()):
        logging.debug('  Uploading to bucket')
        blob.upload_from_filename(local_path)

    properties = {
        'date': tgt_dt.strftime('%Y-%m-%d'),
        'date_ingested': f'{datetime.today().strftime("%Y-%m-%d")}',
        'doy': int(tgt_dt.strftime('%j')),
        'alexi_version': ALEXI_VERSION,
        'bucket_url': bucket_path,
        'source_url': source_path,
        'status': status,
        'units': UNITS,
    }
    params = {
        'name': asset_id,
        'bands': [{'id': variable}],
        'tilesets': [{'sources': [{'uris': [bucket_path]}]}],
        'properties': properties,
        'startTime': tgt_dt.isoformat() + '.000000000Z',
        # 'missingData': {'values': [NODATA_VALUE]},
        # 'pyramiding_policy': 'MEAN',
    }

    logging.debug('  Starting ingest task')
    task = None
    for i in range(1, 4):
        try:
            task_id = ee.data.newTaskId()[0]
            task = ee.data.startIngestion(task_id, params, allow_overwrite=True)
            break
        except Exception as e:
            logging.info(f'  Exception starting ingest - retry {i}')
            logging.debug(str(e))
            time.sleep(i ** 3)
    if task is None:
        return f'{export_name} - could not start ingest task'
        # abort(500, description=f'{export_name} - could not start ingest task')

    if os.path.isdir(local_ws):
        shutil.rmtree(local_ws)

    logging.info(f'{export_name} - {task["id"]}')
    return f'{export_name} - {task["id"]}\n'


def update(request):
    """Responds to any HTTP request.

    Parameters
    ----------
    request (flask.Request): HTTP request object.

    Returns
    -------
    The response text or any set of values that can be turned into a
    Response object using
    `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.

    """
    logging.info('Ingest ALEXI Daily ET')

    request_json = request.get_json(silent=True)
    request_args = request.args

    variable = 'et'

    # Default start and end date to None if not set
    if request_json and ('status' in request_json):
        status = request_json['status']
    elif request_args and ('status' in request_args):
        status = request_args['status']
    else:
        abort(400, description=f'status parameter was net set or could not be parsed')
        # status = 'early'

    if status not in STATUS:
        abort(400, description=f'status parameter was net set or could not be parsed')

    # Default start and end date to None if not set
    if request_json and ('start' in request_json):
        start_date = request_json['start']
    elif request_args and ('start' in request_args):
        start_date = request_args['start']
    else:
        start_date = None

    if request_json and ('end' in request_json):
        end_date = request_json['end']
    elif request_args and ('end' in request_args):
        end_date = request_args['end']
    else:
        end_date = None

    if not start_date and not end_date:
        today = datetime.today()
        start_dt = (datetime(today.year, today.month, today.day) -
                    relativedelta(days=START_DAY_OFFSET))
        end_dt = (datetime(today.year, today.month, today.day) -
                  relativedelta(days=END_DAY_OFFSET))
    elif start_date and end_date:
        # Only process custom range if start and end are both set
        # Limit the end date to the last full month date
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError as e:
            response = 'Error parsing start and/or end date\n'
            response += str(e)
            abort(404, description=response)

        # Force end date to be last day of previous month
        # end_dt = min(end_dt, datetime.today() - timedelta(days=1))

        # TODO: Force start date to be at least one month before end
        # start_dt = min(start_dt, end_dt - relativedelta(months=1) + relativedelta(days=1))

        if start_dt > end_dt:
            abort(404, description='Start date must be before end date')
        # elif (end_dt - start_dt) > timedelta(days=200):
        #     abort(404, description='No more than 6 months can be processed in a single request')
        # if start_dt < datetime(2001, 1, 1):
        #     logging.debug('Start Date: {} - no images before '
        #                   '2001-01-01'.format(start_dt.strftime('%Y-%m-%d')))
        #     start_dt = datetime(2001, 1, 1)
    else:
        abort(404, description='Both start and end date must be specified')

    if request_json and ('overwrite' in request_json):
        overwrite_flag = request_json['overwrite']
    elif request_args and ('overwrite' in request_args):
        overwrite_flag = request_args['overwrite']
    else:
        overwrite_flag = 'false'

    if overwrite_flag.lower() in ['true', 't']:
        overwrite_flag = True
    elif overwrite_flag.lower() in ['false', 'f']:
        overwrite_flag = False
    else:
        abort(400, description=f'overwrite="{overwrite_flag}" could not be parsed')

    args = {
        'status': stats,
        'start_dt': start_dt,
        'end_dt': end_dt,
        'variable': variable,
        'limit': NEW_TASKS,
        'overwrite_flag': overwrite_flag,
    }

    response = ''
    count = 0
    for tgt_dt in ingest_dates(**args):
        response += ingest(tgt_dt=tgt_dt, variable=variable, overwrite_flag=True)
        count += 1

    # response = f'Ingested {count} new assets\n'
    return Response(response, mimetype='text/plain')


def ingest_dates(status, start_dt, end_dt, variable, limit, overwrite_flag=False):
    """Identify hourly datetimes to ingest

    Parameters
    ----------
    status : str
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date, inclusive.
    variable : str
    limit : int
    overwrite_flag : bool, optional

    Returns
    -------
    list of datetimes

    """
    logging.info(f'Building hourly date list')
    logging.info(f'  Start Date: {start_dt.strftime("%Y-%m-%d")}')
    logging.info(f'  End Date:   {end_dt.strftime("%Y-%m-%d")}')

    task_id_re = re.compile(f'Ingest image: "{ASSET_COLL_ID}/(?P<date>\d{{8}})"')

    # Start with a list of dates to check
    test_dt_list = list(hourly_date_range(start_dt, end_dt, hours=[0]))
    if not test_dt_list:
        logging.info('Empty date range')
        return []
    # logging.info('\nTest dates: {}'.format(
    #     ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))
    # ))
    # logging.info(f'Test dates: {len(test_dt_list)}')

    # Check if any of the needed dates are currently being ingested
    # Check task list before checking asset list in case a task switches
    #   from running to done before the asset list is retrieved.
    task_id_list = [
        desc.replace('\nAsset ingestion: ', '')
        for desc in get_ee_tasks(states=['RUNNING', 'READY']).keys()
    ]
    task_count = len(task_id_list)
    task_dates = {
        datetime.strptime(m.group('date'), '%Y%m%d').strftime(ISO_DT_FMT)
        for task_id in task_id_list
        for m in [task_id_re.search(task_id)] if m
    }
    # logging.debug('Task dates: {", ".join(sorted(task_dates))}')

    # Switch date list to be dates that are missing
    test_dt_list = [
        dt for dt in test_dt_list
        if overwrite_flag or (dt.strftime(ISO_DT_FMT) not in task_dates)
    ]
    if not test_dt_list:
        logging.info('All dates are queued for export')
        return []
    # else:
    #     logging.info('\nMissing asset dates: {}'.format(', '.join(
    #         map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))
    #     ))
    # logging.info(f'Test dates: {len(test_dt_list)}')

    # Check if the assets already exist
    # For now, assume the collection exists
    # Get separate date lists by build status
    logging.debug('\nChecking existing assets (by year)')
    asset_early_dates = set(list(
        ee.ImageCollection(ASSET_COLL_ID)
        .filterMetadata('status', 'equals', 'early')
        .filterDate(start_dt.strftime('%Y-%m-%d'), (end_dt + timedelta(days=1)).strftime('%Y-%m-%d'))
        .aggregate_array('system:index').getInfo()
    ))
    asset_provisional_dates = set(list(
        ee.ImageCollection(ASSET_COLL_ID)
        .filterMetadata('status', 'equals', 'provisional')
        .filterDate(start_dt.strftime('%Y-%m-%d'), (end_dt + timedelta(days=1)).strftime('%Y-%m-%d'))
        .aggregate_array('system:index').getInfo()
    ))

    asset_final_dates = set()
    for year in {test_dt.year for test_dt in test_dt_list}:
        # logging.debug(f'  {year}')
        asset_date_coll = (
            ee.ImageCollection(ASSET_COLL_ID)
            .filterDate(start_dt.strftime('%Y-%m-%d'), (end_dt + timedelta(days=1)).strftime('%Y-%m-%d'))
            .filterDate(f'{year}-01-01', f'{year+1}-01-01')
            .filterMetadata('status', 'equals', 'final')
        )
        asset_date_list = []
        for i in range(1, 4):
            try:
                asset_date_list = asset_date_coll.aggregate_array('system:index').getInfo()
                break
            except Exception as e:
                logging.info(f'  Exception get asset list - retry {i}')
                logging.debug(str(e))
                time.sleep(i ** 3)
        if asset_date_list:
            asset_final_dates.update(asset_date_list)

    if status == 'early':
        asset_dates = asset_final_dates | asset_provisional_dates | asset_early_dates
    if status == 'provisional':
        asset_dates = asset_final_dates | asset_provisional_dates
    elif status == 'final':
        asset_dates = asset_final_dates
    # else:
    #     asset_dates = asset_final_dates | asset_provisional_dates | asset_early_dates

    # Switch date list to be dates that are missing
    test_dt_list = [
        dt for dt in test_dt_list
        if overwrite_flag or (dt.strftime(ASSET_DT_FMT) not in asset_dates)
    ]
    if not test_dt_list:
        logging.info('No dates to process after filtering existing assets')
        return []
    logging.debug('\nDates (after filtering existing assets): {}'.format(
        ', '.join(map(lambda x: x.strftime(ISO_DT_FMT), test_dt_list))
    ))

    # # Check bucket by year and only for missing years
    # # This should be faster later on once more of the assets are ingested
    # #   since it will skip most years
    # logging.debug('\nChecking bucket files (by year)')
    # bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    # bucket_dates = set()
    # for year in {test_dt.year for test_dt in test_dt_list}:
    #     logging.debug(f'  {year}')
    #     bucket_date_list = [
    #         datetime.strptime(m.group('date'), TIF_DT_FMT).strftime(ISO_DT_FMT)
    #         for blob in bucket.list_blobs(prefix=f'{BUCKET_FOLDER}/{TIF_PREFIX}{year}')
    #         for m in [re.search(TIF_DT_RE, blob.name)] if m
    #         # if blob.name.endswith('.tif')
    #     ]
    #     # CGM - Check the bucket_dates against the test_dt_list before updating?
    #     bucket_dates.update(bucket_date_list)
    # logging.info(f'Bucket dates: {len(bucket_dates)}')
    #
    # # Keep dates that have a bucket file
    # test_dt_list = [dt for dt in test_dt_list if dt.strftime(ISO_DT_FMT) in bucket_dates]
    # if not test_dt_list:
    #     logging.info('No dates to process after filtering bucket files')
    #     return []
    # logging.debug('\nDates (after filtering bucket files): {}'.format(
    #     ', '.join(map(lambda x: x.strftime(ISO_DT_FMT), test_dt_list))
    # ))

    # Finally, check the server for images
    # TODO: Add code to check if the images have a newer last modified date
    logging.debug('\nChecking server files')
    logging.debug(f'{SOURCE_URL}')
    server_dates = {
        datetime.strptime(m.group('date'), TIF_DT_FMT).strftime(ISO_DT_FMT)
        for item in get_json_file_listing(SOURCE_URL + f'/{status}')
        for m in [re.search(TIF_DT_RE, item['filename'])] if m
        # if item['filename'].endswith('.tif')
    }
    # final_dates = {
    #     datetime.strptime(m.group('date'), TIF_DT_FMT).strftime(ISO_DT_FMT)
    #     for item in get_json_file_listing(SOURCE_URL + '/final')
    #     for m in [re.search(TIF_DT_RE, item['filename'])] if m
    #     # if item['filename'].endswith('.tif')
    # }
    # provisional_dates = {
    #     datetime.strptime(m.group('date'), TIF_DT_FMT).strftime(ISO_DT_FMT)
    #     for item in get_json_file_listing(SOURCE_URL + '/provisional')
    #     for m in [re.search(TIF_DT_RE, item['filename'])] if m
    #     # if item['filename'].endswith('.tif')
    # }
    # early_dates = {
    #     datetime.strptime(m.group('date'), TIF_DT_FMT).strftime(ISO_DT_FMT)
    #     for item in get_json_file_listing(SOURCE_URL + '/early')
    #     for m in [re.search(TIF_DT_RE, item['filename'])] if m
    #     # if item['filename'].endswith('.tif')
    # }
    # server_dates = final_dates | provisional_dates | early_dates

    # Keep dates that have a server file
    test_dt_list = [dt for dt in test_dt_list if dt.strftime(ISO_DT_FMT) in server_dates]
    if not test_dt_list:
        logging.info('No dates to process after filtering server files')
        return []
    logging.debug('\nDates (after filtering server files): {}'.format(
        ', '.join(map(lambda x: x.strftime(ISO_DT_FMT), test_dt_list))
    ))

    # Limit the number of dates returned to the number of open queue spots
    if limit:
        new_tasks = min(max(MAX_TASKS - len(task_id_list), 0), limit)
        logging.debug(f'Date count:    {len(test_dt_list)}')
        logging.debug(f'Date limit:    {limit}')
        logging.info(f'Queued tasks:  {task_count}')
        logging.info(f'Limited dates: {new_tasks}')
        test_dt_list = test_dt_list[:new_tasks]

    return test_dt_list


def hourly_date_range(start_dt, end_dt, hours=list(range(0, 24)), skip_leap_days=False):
    """Generate hourly dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (inclusive).
    hours : list, optional
    skip_leap_days : bool, optional
        If True, skip leap days while incrementing (the default is True).

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt < (end_dt + timedelta(days=1)):
        if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
            if curr_dt.hour in hours:
                yield curr_dt
        curr_dt += timedelta(hours=1)


def get_ee_tasks(states=['RUNNING', 'READY'], retries=4):
    """Return current active tasks

    Parameters
    ----------
    states : list, optional
        List of task states to check (the default is ['RUNNING', 'READY']).
    retries : int, optional
        The number of times to retry getting the task list if there is an error.

    Returns
    -------
    dict : task descriptions (key) and full task info dictionary (value)

    """
    logging.debug('\nRequesting Task List')
    task_list = None
    for i in range(1, retries):
        try:
            # TODO: getTaskList() is deprecated, switch to listOperations()
            task_list = ee.data.getTaskList()
            # task_list = ee.data.listOperations()
            break
        except Exception as e:
            logging.warning(f'  Error getting task list, retrying ({i}/{retries})\n  {e}')
            time.sleep(i ** 3)
    if task_list is None:
        raise Exception('\nUnable to retrieve task list, exiting')

    task_list = sorted(
        [task for task in task_list if task['state'] in states],
        key=lambda t: (t['state'], t['description'], t['id'])
    )
    # task_list = sorted([
    #     [t['state'], t['description'], t['id']] for t in task_list
    #     if t['state'] in states]
    # )

    # Convert the task list to a dictionary with the task name as the key
    return {task['description']: task for task in task_list}


def get_json_file_listing(url, variable=None):
    response = requests.get(url + '/?format=json')
    response.raise_for_status()

    output = json.loads(response.text)['directory_listing']
    if variable:
        output = [item for item in output if variable in item['filename']]

    return output


def url_download(download_url, output_path, verify=True):
    """Download file from a URL using requests module

    Parameters
    ----------
    download_url : str
    output_path : str
    verify : bool, optional

    Returns
    -------
    None

    """
    for i in range(1, 6):
        try:
            response = requests.get(download_url, stream=True, verify=verify)
        except Exception as e:
            logging.info(f'  Exception: {e}')
            return False

        logging.debug(f'  HTTP Status: {response.status_code}')
        if response.status_code == 200:
            pass
        elif response.status_code == 404:
            logging.debug('  Skipping')
            return False
        else:
            logging.info(f'  HTTPError: {response.status_code}')
            logging.info(f'  Retry attempt: {i}')
            time.sleep(i ** 2)
            continue

        logging.debug('  Beginning download')
        try:
            with (open(output_path, 'wb')) as output_f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:  # filter out keep-alive new chunks
                        output_f.write(chunk)
            logging.debug('  Download complete')
            return True
        except Exception as e:
            logging.info(f'  Exception: {e}')
            return False


def arg_parse():
    """"""
    today = datetime.today()

    parser = argparse.ArgumentParser(
        description='Ingest ALEXI Daily ET Assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--status', required=True, choices=STATUS,
        help=f'Build status: {", ".join(STATUS)}')
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='DATE',
        default=(datetime(today.year, today.month, today.day) -
                 relativedelta(days=START_DAY_OFFSET)).strftime('%Y-%m-%d'),
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='DATE',
        default=(datetime(today.year, today.month, today.day) -
                 relativedelta(days=END_DAY_OFFSET)).strftime('%Y-%m-%d'),
        help='End date (format YYYY-MM-DD)')
    parser.add_argument(
        '--delay', default=0, type=float,
        help='Delay (in seconds) between each export tasks')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='Earth Engine service account JSON key file')
    parser.add_argument(
        '--limit', default=0, type=int,
        help='Maximum number of new tasks to submit')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--project', default=None,
        help='Google cloud project ID to use for GEE authentication')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process dates in reverse order')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    # logging.basicConfig(level=args.loglevel, format='%(message)s')

    # if args.key and 'FUNCTION_REGION' not in os.environ:
    if args.key:
        logging.info(f'\nInitializing GEE using user key file: {args.key}')
        try:
            ee.Initialize(ee.ServiceAccountCredentials('_', key_file=args.key))
        except ee.ee_exception.EEException:
            raise Exception('Unable to initialize GEE using user key file')
    elif args.project:
        logging.info(f'\nInitializing Earth Engine using project credentials'
                     f'\n  Project ID: {args.project}')
        ee.Initialize(project=args.project)
        # ee.Initialize(
        #     project=args.project, opt_url='https://earthengine-highvolume.googleapis.com'
        # )
    else:
        logging.info('\nInitializing Earth Engine using user credentials')
        ee.Initialize()

    # Build the image collection if it doesn't exist
    logging.debug(f'Image Collection: {ASSET_COLL_ID}')
    if not ee.data.getInfo(ASSET_COLL_ID):
        logging.info(f'\nImage collection does not exist and will be built'
                     f'\n  {ASSET_COLL_ID}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)

    ingest_dt_list = ingest_dates(
        status=args.status,
        start_dt=args.start,
        end_dt=args.end,
        variable='et',
        limit=args.limit,
        overwrite_flag=args.overwrite,
    )
    if args.loglevel == logging.DEBUG:
        pprint.pprint(ingest_dt_list)
        input('ENTER')

    for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
        # logging.info(f'Date: {ingest_dt.strftime("%Y-%m-%d")}')
        # Checking if overwrite is needed is happening in the ingest_dates() function
        #   so always overwrite if the date is in the list
        response = ingest(tgt_dt=ingest_dt, status=args.status, overwrite_flag=True)
        logging.info(f'  {response}')
        time.sleep(args.delay)
