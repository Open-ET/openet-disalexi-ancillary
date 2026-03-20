import argparse
from datetime import datetime, timedelta, timezone
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
import requests

import openet.core.utils as utils

SOURCE_URL = 'https://nssrgeo.ndc.nasa.gov/SPoRT/land_surface_products/alexi_et/meteo'
ASSET_ROOT = 'projects/earthengine-legacy/assets/projects/disalexi/meteo_data'
ASSET_FOLDER = {
    'airpressure': 'airpressure',
    'temperature': 'airtemperature',
    'vaporpressure': 'vp',
    'windspeed': 'windspeed',
}
ASSET_COLL_NAME = 'global_v001_3hour'
ASSET_DT_FMT = '%Y%m%d%H'
BAND_NAME = {
    'airpressure': 'airpressure',
    'temperature': 'temperature',
    'vaporpressure': 'vp',
    'windspeed': 'windspeed',
}
BUCKET_NAME = 'openet'
BUCKET_FOLDER = {
    'airpressure': 'disalexi/airpressure_tif',
    'temperature': 'disalexi/temperature_tif',
    'vaporpressure': 'disalexi/vaporpressure_tif',
    'windspeed': 'disalexi/windspeed_tif',
}
DATA_VERSION = 2
HOURS = [0, 3, 6, 9, 12, 15, 18, 21]
ISO_DT_FMT = '%Y-%m-%dT%H00'
# Maximum number of new tasks that can be submitted in a function call
NEW_TASKS = 300
# Maximum number of queued tasks (intentionally not setting to 3000)
MAX_TASKS = 1000
# NODATA_VALUE = -9999
START_DAY_OFFSET = 120
END_DAY_OFFSET = 3
STORAGE_CLIENT = storage.Client()
TIF_PREFIX = {
    'airpressure': 'psfc_series_',
    'temperature': 't2_series_',
    'vaporpressure': 'q2_series_',
    'windspeed': 'wind_surface_',
}
TIF_NAME_FMT = '{prefix}{date}.tif'
TIF_DT_FMT = '%Y%m%d_%H'
TIF_DT_RE = '(?P<date>\d{8}_\d{2})'
TODAY_DT = datetime.now(timezone.utc)
# TODO: Check these units
UNITS = {
    'airpressure': 'kPa',
    'temperature': 'K',
    'vaporpressure': 'kPa',
    'windspeed': 'm s-1',
}
VARIABLES = ['airpressure', 'temperature', 'vaporpressure', 'windspeed']

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
    ee.Initialize(
        credentials, project=project_id, opt_url='https://earthengine-highvolume.googleapis.com'
    )


def ingest(tgt_dt, variable, workspace='/tmp', overwrite_flag=False):
    """

    Parameters
    ----------
    tgt_dt : datetime
    variable : str
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """
    logging.info(f'DisALEXI 3 hour {variable} - {tgt_dt.strftime("%Y-%m-%dT%H00")}')
    # response = f'DisALEXI 3 hour {variable} - {tgt_dt.strftime("%Y-%m-%dT%H00")}'

    tif_name = TIF_NAME_FMT.format(prefix=TIF_PREFIX[variable], date=tgt_dt.strftime(TIF_DT_FMT))
    local_ws = os.path.join(workspace, variable, tgt_dt.strftime(f'%Y%m%d%H'))
    local_path = os.path.join(local_ws, tif_name)
    source_path = f'{SOURCE_URL}/{tif_name}'
    bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER[variable]}/{tif_name}'
    asset_id = f'{ASSET_ROOT}/{ASSET_FOLDER[variable]}/{ASSET_COLL_NAME}/' \
               f'{tgt_dt.strftime(ASSET_DT_FMT)}'
    export_name = f'disalexi_3hour_{variable}_{tgt_dt.strftime("%Y%m%d%H")}'

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
    if not os.path.isfile(local_path):
        logging.debug('  Downloading source image')
        url_download(source_path, local_path)
    if not os.path.isfile(local_path):
        return f'{export_name} - Image was not downloaded, skipping\n'

    # Copy the file to the bucket for ingest and archiving
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{BUCKET_FOLDER[variable]}/{tif_name}')
    if blob and (not blob.exists() or overwrite_flag):
        logging.debug('  Uploading to bucket')
        blob.upload_from_filename(local_path)

    properties = {
        'date': tgt_dt.strftime('%Y-%m-%d'),
        'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
        'doy': int(tgt_dt.strftime('%j')),
        'hour': int(tgt_dt.strftime('%H')),
        'meteo_version': DATA_VERSION,
        'bucket_url': bucket_path,
        'source_url': source_path,
        'units': UNITS[variable],
    }
    params = {
        'name': asset_id,
        'bands': [{'id': BAND_NAME[variable]}],
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
            logging.info(f'  Exception starting ingest - retry {i-2}')
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
    logging.info('Ingest DisALEXI 3 Hour Meteo')

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and ('variables' in request_json):
        variables = request_json['variables'].split(',')
    elif request_args and ('variable' in request_args):
        variables = request_args['variables'].split(',')
    else:
        variables = VARIABLES[:]
        # abort(404, description='variables must be specified')
    logging.info(f'Variables: {", ".join(variables)}')

    # TODO: Add support for hours parameter
    hours = HOURS[:]
    # if request_json and 'hours' in request_json:
    #     hours = request_json['hours']
    # elif request_args and 'hours' in request_args:
    #     hours = request_args['hours']
    # else:
    #     hours = HOURS[:]

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
        start_dt = (datetime(TODAY_DT.year, TODAY_DT.month, TODAY_DT.day) -
                    relativedelta(days=START_DAY_OFFSET))
        end_dt = (datetime(TODAY_DT.year, TODAY_DT.month, TODAY_DT.day) -
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
        # end_dt = min(end_dt, TODAY_DT - timedelta(days=1))

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

    response = ''
    count = 0
    for variable in variables:
        logging.info(f'Variable: {variable}')
        args = {
            'start_dt': start_dt,
            'end_dt': end_dt,
            'variable': variable,
            'hours': hours,
            'limit': NEW_TASKS,
            'overwrite_flag': overwrite_flag,
        }
        for tgt_dt in ingest_dates(**args):
            response += ingest(tgt_dt, variable, overwrite_flag=True)
            count += 1

    # response = f'Ingested {count} new assets\n'
    return Response(response, mimetype='text/plain')


def ingest_dates(start_dt, end_dt, variable, hours, limit=0, overwrite_flag=False):
    """Identify datetimes to ingest

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date, inclusive.
    variable : str
    hours : list
    limit : int
    overwrite_flag : bool, optional

    Returns
    -------
    list of datetimes

    """
    logging.info(f'Building datetime list')
    logging.info(f'  Start Date: {start_dt.strftime("%Y-%m-%d")}')
    logging.info(f'  End Date:   {end_dt.strftime("%Y-%m-%d")}')
    logging.info(f'  Hours:      {", ".join(map(str, hours))}')

    task_id_re = re.compile(
        f'Ingest image: "{ASSET_ROOT}/{ASSET_FOLDER[variable]}/'
        f'{ASSET_COLL_NAME}/(?P<date>\d{{10}})"'
    )

    # Start with a list of dates to check
    test_dt_list = list(hourly_date_range(start_dt, end_dt, hours=hours))
    if not test_dt_list:
        logging.info('Empty date range')
        return []
    # logging.info('\nTest dates: {}'.format(
    #     ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))
    # ))
    # logging.info(f'Test dates: {len(test_dt_list)}')

    # CGM - Checking the task list is timing out for some reason
    # # Check if any of the needed dates are currently being ingested
    # # Check task list before checking asset list in case a task switches
    # #   from running to done before the asset list is retrieved.
    # task_id_list = [
    #     desc.replace('\nAsset ingestion: ', '')
    #     for desc in get_ee_tasks(states=['RUNNING', 'READY']).keys()
    # ]
    # task_count = len(task_id_list)
    # task_dates = {
    #     datetime.strptime(m.group('date'), '%Y%m%d%H').strftime(ISO_DT_FMT)
    #     for task_id in task_id_list
    #     for m in [task_id_re.search(task_id)] if m
    # }
    # # logging.debug(f'Task dates: {", ".join(sorted(task_dates))}')
    #
    # # Switch date list to be dates that are missing
    # test_dt_list = [
    #     dt for dt in test_dt_list
    #     if overwrite_flag or (dt.strftime(ISO_DT_FMT) not in task_dates)
    # ]
    # if not test_dt_list:
    #     logging.info('All dates are queued for export')
    #     return []
    # # else:
    # #     logging.info('\nMissing asset dates: {}'.format(', '.join(
    # #         map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))
    # #     ))

    # Check if the assets already exist
    # For now, assume the collection exists
    logging.debug('\nChecking existing assets (by year)')
    asset_coll_id = f'{ASSET_ROOT}/{ASSET_FOLDER[variable]}/{ASSET_COLL_NAME}'
    asset_dates = set()
    for year in {test_dt.year for test_dt in test_dt_list}:
        logging.debug(f'  {year}')
        asset_date_coll = (
            ee.ImageCollection(asset_coll_id)
            .filterDate(start_dt.strftime('%Y-%m-%d'),
                        (end_dt + timedelta(days=1)).strftime('%Y-%m-%d'))
            .filterDate(f'{year}-01-01', f'{year+1}-01-01')
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
            asset_dates.update(asset_date_list)
    # logging.debug(f'\nAsset dates: {", ".join(sorted(asset_dates))}')
    # logging.info(f'Asset dates: {len(asset_dates)}')

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

    # Finally, check the server for images
    # TODO: Add code to check if the images have a newer last modified date
    logging.debug('\nChecking server files')
    logging.debug(f'{SOURCE_URL}')
    server_dates = {
        datetime.strptime(m.group('date'), TIF_DT_FMT).strftime(ISO_DT_FMT)
        for item in get_json_file_listing(SOURCE_URL, variable=TIF_PREFIX[variable])
        for m in [re.search(TIF_DT_RE, item['filename'])] if m
        # if item['filename'].endswith('.tif')
    }

    # Keep dates that have a server file
    test_dt_list = [dt for dt in test_dt_list if dt.strftime(ISO_DT_FMT) in server_dates]
    if not test_dt_list:
        logging.info('No dates to process after filtering server files')
        return []
    logging.debug('\nDates (after filtering server files): {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%dT%H00'), test_dt_list))
    ))

    # CGM - Checking the task list is timing out for some reason
    # # Limit the number of dates returned to the number of open queue spots
    # if limit:
    #     new_tasks = min(max(MAX_TASKS - len(task_id_list), 0), limit)
    #     logging.debug(f'Date count:    {len(test_dt_list)}')
    #     logging.debug(f'Date limit:    {limit}')
    #     logging.info(f'Queued tasks:  {task_count}')
    #     logging.info(f'Limited dates: {new_tasks}')
    #     test_dt_list = test_dt_list[:new_tasks]

    return test_dt_list


def hourly_date_range(start_dt, end_dt, hours=HOURS, skip_leap_days=False):
    """Generate hourly dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date.
    hours : list
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
    # https://nssrgeo.ndc.nasa.gov/SPoRT/land_surface_products/alexi_et/meteo/?format=json

    response = requests.get(url + '/?format=json', timeout=5)
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
    for i in range(1, 4):
        try:
            response = requests.get(download_url, stream=True, verify=verify, timeout=5)
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
    parser = argparse.ArgumentParser(
        description='Ingest DisALEXI 3 hour meteorology assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    parser.add_argument(
        '-v', '--variables', nargs='+', metavar='VAR',
        choices=VARIABLES, default=VARIABLES,
        help=f'DisALEXI Meteorology Variables ({", ".join(VARIABLES)})')
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='DATE',
        default=(datetime(TODAY_DT.year, TODAY_DT.month, TODAY_DT.day) -
                 relativedelta(days=START_DAY_OFFSET)).strftime('%Y-%m-%d'),
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='DATE',
        default=(datetime(TODAY_DT.year, TODAY_DT.month, TODAY_DT.day) -
                 relativedelta(days=END_DAY_OFFSET)).strftime('%Y-%m-%d'),
        help='End date (format YYYY-MM-DD)')
    parser.add_argument(
        '--hours', default=",".join(map(str, HOURS)),
        help=f'Hour timesteps')
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
    else:
        logging.info('\nInitializing Earth Engine using user credentials')
        ee.Initialize()

    # for variable in args.variables:
    #     # Build the image collection if it doesn't exist
    #     asset_coll_id = f'{ASSET_ROOT}/{ASSET_FOLDER[variable]}/{ASSET_COLL_NAME}'
    #     logging.debug(f'Image Collection: {asset_coll_id}')
    #     if not ee.data.getInfo(asset_coll_id):
    #         logging.info(f'\nImage collection does not exist and will be built'
    #                      f'\n  {asset_coll_id}')
    #         input('Press ENTER to continue')
    #         ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, asset_coll_id)

    print(args.variables)
    for variable in args.variables:
        logging.info(f'\nVariable: {variable}')
        ingest_dt_list = ingest_dates(
            start_dt=args.start,
            end_dt=args.end,
            variable=variable,
            hours=list(map(int, args.hours.split(','))),
            limit=args.limit,
            overwrite_flag=args.overwrite,
        )
        if args.loglevel == logging.DEBUG:
            pprint.pprint(ingest_dt_list)
            input('ENTER')

        for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
            # logging.info(f'Date: {ingest_dt.strftime("%Y-%m-%d")}')
            response = ingest(
                tgt_dt=ingest_dt,
                variable=variable,
                workspace=args.workspace,
                overwrite_flag=args.overwrite,
            )
            logging.info(f'  {response}')
            time.sleep(args.delay)
