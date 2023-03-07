import argparse
import datetime
import logging
import os
import pprint
import re
import time

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import ee
from flask import abort, Response
from google.cloud import storage

import openet.core.utils as utils

if 'FUNCTION_REGION' in os.environ:
    credentials = ee.ServiceAccountCredentials(
        '', key_file='../../keys/steel-melody-gee.json')
    ee.Initialize(credentials)

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

ASSET_ROOT = 'projects/earthengine-legacy/assets/' \
             'projects/disalexi/meteo_data'
ASSET_FOLDER = {
    'airpressure': 'airpressure',
    'temperature': 'airtemperature',
    'vaporpressure': 'vp',
    'windspeed': 'windspeed',
}
ASSET_COLL_NAME = 'global_v001_3hour'
# ASSET_COLL_ID = 'projects/earthengine-legacy/assets/' \
#                 'projects/disalexi/meteo_data/{variable}/global_v001_3hour'
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
DATA_VERSION = 1
ISO_DT_FMT = '%Y-%m-%dT%H00'
# Maximum number of new tasks that can be submitted in a function call
NEW_TASKS = 300
# Maximum number of queued tasks (intentionally not setting to 3000)
MAX_TASKS = 1000
# NODATA_VALUE = -9999
# START_MONTH_OFFSET = 3
# END_MONTH_OFFSET = 0
STORAGE_CLIENT = storage.Client()
TIF_PREFIX = {
    'airpressure': 'psfc_series_',
    'temperature': 't2_series_',
    'vaporpressure': 'q2_series_',
    'windspeed': 'wind_surface_',
}
TIF_NAME_FMT = '{prefix}{date}.tif'
TIF_DT_FMT = '%Y%j_%H'
TIF_DT_RE = '(?P<date>\d{7}_\d{2})'
# TODO: Check these units
UNITS = {
    'airpressure': 'kPa',
    'temperature': 'K',
    'vaporpressure': 'kPa',
    'windspeed': 'm s-1',
}
VARIABLES = ['airpressure', 'temperature', 'vaporpressure', 'windspeed']
HOURS = [0, 3, 6, 9, 12, 15, 18, 21]


def ingest(tgt_dt, variable, overwrite_flag=False):
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
    # tgt_date = tgt_dt.strftime('%Y%m%d%H')

    logging.info(f'DisALEXI 3 hour {variable} - {tgt_dt.strftime("%Y-%m-%dT%H00")}')
    # response = f'DisALEXI 3 hour {variable} - {tgt_dt.strftime("%Y-%m-%dT%H00")}'

    # CGM - Assuming the "hours" in the file name as been set correctly
    #   when the file was moved to the archive bucket
    tif_dt = (datetime.datetime(tgt_dt.year, tgt_dt.month, tgt_dt.day) +
              datetime.timedelta(hours=int(tgt_dt.hour)))
    # tif_dt = (datetime.datetime(tgt_dt.year, tgt_dt.month, tgt_dt.day) +
    #           datetime.timedelta(hours=int(tgt_dt.hour) / 3))
    tif_name = TIF_NAME_FMT.format(prefix=TIF_PREFIX[variable],
                                   date=tif_dt.strftime(TIF_DT_FMT))
    bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER[variable]}/{tif_name}'

    asset_id = f'{ASSET_ROOT}/{ASSET_FOLDER[variable]}/{ASSET_COLL_NAME}/' \
               f'{tgt_dt.strftime(ASSET_DT_FMT)}'
    export_name = f'disalexi_3hour_{variable}_{tgt_dt.strftime("%Y%m%d%H")}'

    logging.debug(f'  {bucket_path}')
    logging.debug(f'  {asset_id}')
    logging.debug(f'  {export_name}')

    if ee.data.getInfo(asset_id):
        if overwrite_flag:
            try:
                ee.data.deleteAsset(asset_id)
            except Exception as e:
                return f'{export_name} - An error occured while trying to '\
                       f'delete the existing asset, skipping\n{e}\n'
        else:
            return f'{export_name} - The asset already exists and overwrite '\
                   f'is False, skipping\n'

    properties = {
        'date': tgt_dt.strftime('%Y-%m-%d'),
        'date_ingested': f'{datetime.datetime.today().strftime("%Y-%m-%d")}',
        'doy': int(tgt_dt.strftime('%j')),
        'hour': int(tgt_dt.strftime('%H')),
        'meteo_version': DATA_VERSION,
        'source': bucket_path,
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

    logging.debug('  Starting ingesting task')
    task = None
    for i in range(3, 10):
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

    return f'{export_name} - {task["id"]}\n'


def cron_scheduler(request):
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

    if request_json and 'variable' in request_json:
        variable = request_json['variable']
    elif request_args and 'variable' in request_args:
        variable = request_args['variable']
    else:
        abort(404, description='variable must be specified')
    logging.info(f'Variable: {variable}')

    # Default start and end date to None if not set
    if request_json and 'start' in request_json:
        start_date = request_json['start']
    elif request_args and 'start' in request_args:
        start_date = request_args['start']
    else:
        start_date = None

    if request_json and 'end' in request_json:
        end_date = request_json['end']
    elif request_args and 'end' in request_args:
        end_date = request_args['end']
    else:
        end_date = None

    if start_date and end_date:
        # Only process custom range if start and end are both set
        # Limit the end date to the last full month date
        try:
            start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError as e:
            response = 'Error parsing start and/or end date\n'
            response += str(e)
            abort(404, description=response)

        # Force end date to be last day of previous month
        # end_dt = min(end_dt,
        #              datetime.datetime.today() - datetime.timedelta(days=1))

        # TODO: Force start date to be at least one month before end
        # start_dt = min(
        #     start_dt,
        #     end_dt - relativedelta(months=1) + relativedelta(days=1))

        if start_dt > end_dt:
            abort(404, description='Start date must be before end date')
        # elif (end_dt - start_dt) > datetime.timedelta(days=200):
        #     abort(404, description='No more than 6 months can be processed in a single request')
        # if start_dt < datetime.datetime(2001, 1, 1):
        #     logging.debug('Start Date: {} - no images before '
        #                   '2001-01-01'.format(start_dt.strftime('%Y-%m-%d')))
        #     start_dt = datetime.datetime(2001, 1, 1)
    # elif not start_date and not end_date:
    #     today = datetime.datetime.today()
    #     start_dt = (datetime.datetime(today.year, today.month, today.day) -
    #                 relativedelta(months=START_MONTH_OFFSET))
    #     end_dt = (datetime.datetime(today.year, today.month, today.day) -
    #               relativedelta(months=END_MONTH_OFFSET))
    #     # start_dt = (datetime.datetime(today.year, today.month, today.day) -
    #     #             relativedelta(days=START_DAY_OFFSET))
    #     # end_dt = (datetime.datetime(today.year, today.month, today.day) -
    #     #           relativedelta(days=END_DAY_OFFSET))
    else:
        abort(404, description='Both start and end date must be specified')

    args = {
        'start_dt': start_dt, 'end_dt': end_dt,
        'variable': variable,
        'hours': HOURS,
        'limit': NEW_TASKS,
    }

    count = 0
    for tgt_dt in ingest_dates(**args):
        ingest(tgt_dt, variable, overwrite_flag=True)
        count += 1

    return Response(f'Ingested {count} new assets', mimetype='text/plain')


def ingest_dates(start_dt, end_dt, variable, hours, limit, overwrite_flag=False):
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

    task_id_re = re.compile(f'Ingest image: "{ASSET_ROOT}/{ASSET_FOLDER[variable]}/'
                            f'{ASSET_COLL_NAME}/(?P<date>\d{{10}})"')

    # Start with a list of dates to check
    test_dt_list = list(hourly_date_range(start_dt, end_dt, hours=hours))
    if not test_dt_list:
        logging.info('Empty date range')
        return []
    # logging.info('\nTest dates: {}'.format(
    #     ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))
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
        datetime.datetime.strptime(m.group('date'), '%Y%m%d%H').strftime(ISO_DT_FMT)
        for task_id in task_id_list for m in [task_id_re.search(task_id)] if m
    }
    # logging.debug('Task dates: {", ".join(sorted(task_dates))}')

    # Switch date list to be dates that are missing
    test_dt_list = [
        dt for dt in test_dt_list
        if overwrite_flag or (dt.strftime(ISO_DT_FMT) not in task_dates)]
    if not test_dt_list:
        logging.info('All dates are queued for export')
        return []
    # else:
    #     logging.info('\nMissing asset dates: {}'.format(', '.join(
    #         map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))

    # Check if the assets already exist
    # For now, assume the collection exists
    logging.debug('\nChecking existing assets (by year)')
    asset_coll_id = f'{ASSET_ROOT}/{ASSET_FOLDER[variable]}/{ASSET_COLL_NAME}'
    asset_dates = set()
    for year in {test_dt.year for test_dt in test_dt_list}:
        # logging.debug(f'  {year}')
        asset_date_coll = ee.ImageCollection(asset_coll_id) \
            .filterDate(start_dt.strftime('%Y-%m-%d'),
                        end_dt + datetime.timedelta(days=1))\
            .filterDate(f'{year}-01-01', f'{year+1}-01-01')
        asset_date_list = []
        for i in range(1, 6):
            try:
                asset_date_list = asset_date_coll.aggregate_array('system:index')\
                    .getInfo()
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
        if overwrite_flag or dt.strftime(ASSET_DT_FMT) not in asset_dates
    ]
    if not test_dt_list:
        logging.info('No dates to process after filtering existing assets')
        return []
    # logging.debug('\nDates (after filtering existing assets): {}'.format(
    #     ', '.join(map(lambda x: x.strftime(ISO_DT_FMT), test_dt_list))))

    # Check bucket file list for available dates
    logging.debug('\nChecking bucket files')
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    bucket_file_list = []
    for year in {test_dt.year for test_dt in test_dt_list}:
        # logging.debug(f'  {year}')
        prefix = f'{BUCKET_FOLDER[variable]}/{TIF_PREFIX[variable]}{year}'
        bucket_files = [
            blob.name for blob in bucket.list_blobs(prefix=prefix)
            if blob.name.endswith('.tif')]
        bucket_file_list.extend(bucket_files)

    # CGM - Assuming the "hours" in the file name as been set correctly
    #   when the file was moved to the archive bucket
    bucket_date_list = [
        m.group('date').split('_') for f_name in bucket_file_list
        for m in [re.search(TIF_DT_RE, f_name)]
    ]
    bucket_dates = {
        (datetime.datetime.strptime(date_str, '%Y%j') +
         datetime.timedelta(hours=int(hour))).strftime(ISO_DT_FMT)
        for date_str, hour in bucket_date_list
    }
    # bucket_dates = {
    #     datetime.datetime.strptime(m.group('date'), TIF_DT_FMT).strftime(ISO_DT_FMT)
    #     for f_name in bucket_file_list
    #     for m in [re.search(TIF_DT_RE, f_name)]}

    # Keep dates that have a source file in the bucket
    test_dt_list = [
        dt for dt in test_dt_list
        if overwrite_flag or dt.strftime(ISO_DT_FMT) in bucket_dates
    ]
    if not test_dt_list:
        logging.info('No dates to process after filtering bucket files')
        return []
    logging.debug('\nDates (after filtering bucket files): {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%dT%H00'), test_dt_list))
    ))

    # Limit the number of dates returned to the number of open queue spots
    if limit:
        new_tasks = min(MAX_TASKS - len(task_id_list), limit)
        logging.info(f'Date count:    {len(test_dt_list)}')
        logging.info(f'Date limit:    {limit}')
        logging.info(f'Queued tasks:  {task_count}')
        logging.info(f'Limited dates: {new_tasks}')
        test_dt_list = test_dt_list[:new_tasks]

    return test_dt_list


def hourly_date_range(start_dt, end_dt, hours, skip_leap_days=False):
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
    while curr_dt < (end_dt + datetime.timedelta(days=1)):
        if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
            if curr_dt.hour in hours:
                yield curr_dt
        curr_dt += datetime.timedelta(hours=1)


def get_ee_tasks(states=['RUNNING', 'READY'], retries=6):
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
    for i in range(retries):
        try:
            # TODO: getTaskList() is deprecated, switch to listOperations()
            task_list = ee.data.getTaskList()
            # task_list = ee.data.listOperations()
            break
        except Exception as e:
            logging.warning(
                f'  Error getting task list, retrying ({i}/{retries})\n  {e}')
            time.sleep((i+1) ** 2)
    if task_list is None:
        raise Exception('\nUnable to retrieve task list, exiting')

    task_list = sorted(
        [task for task in task_list if task['state'] in states],
        key=lambda t: (t['state'], t['description'], t['id']))
    # task_list = sorted([
    #     [t['state'], t['description'], t['id']] for t in task_list
    #     if t['state'] in states])

    # Convert the task list to a dictionary with the task name as the key
    return {task['description']: task for task in task_list}


def arg_valid_date(date_str):
    DATE_FORMATS = ['%Y%j', '%Y-%m-%d']
    for dt_format in DATE_FORMATS:
        try:
            d = datetime.datetime.strptime(date_str, dt_format)
            return d
        except ValueError:
            pass
    raise ValueError(f'date "{date_str}" could not be parsed')


def arg_parse():
    """"""
    # today = datetime.date.today()

    parser = argparse.ArgumentParser(
        description='Generate DisALEXI 3 hour meteo assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-v', '--variables', nargs='+', metavar='VAR',
        choices=VARIABLES, default=VARIABLES,
        help=f'DisALEXI Meteorology Variables ({", ".join(VARIABLES)})')
    parser.add_argument(
        '--start', metavar='DATE', type=arg_valid_date,
        # default=(datetime.datetime(today.year, today.month, today.day) -
        #          relativedelta(months=START_MONTH_OFFSET)).strftime('%Y-%m-%d'),
        help='Start date')
    parser.add_argument(
        '--end', metavar='DATE', type=arg_valid_date,
        # default=(datetime.datetime(today.year, today.month, today.day) -
        #          relativedelta(months=END_MONTH_OFFSET)).strftime('%Y-%m-%d'),
        help='End date (inclusive)')
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
        '--reverse', default=False, action='store_true',
        help='Process dates in reverse order')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    # if args.key and 'FUNCTION_REGION' not in os.environ:
    if args.key:
        logging.info(f'\nInitializing GEE using user key file: {args.key}')
        try:
            ee.Initialize(ee.ServiceAccountCredentials('_', key_file=args.key))
        except ee.ee_exception.EEException:
            raise Exception('Unable to initialize GEE using user key file')
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
            start_dt=args.start, end_dt=args.end,
            variable=variable, hours=list(map(int, args.hours.split(','))),
            limit=args.limit, overwrite_flag=args.overwrite,
        )

        for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
            # logging.info(f'Date: {ingest_dt.strftime("%Y-%m-%d")}')
            response = ingest(ingest_dt, variable, overwrite_flag=args.overwrite)
            logging.info(f'  {response}')
            time.sleep(args.delay)
