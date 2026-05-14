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

# SOURCE_URL = 'https://nssrgeo.ndc.nasa.gov/SPoRT/land_surface_products/alexi_et/meteo'
# DEADBEEF
ASSET_COLL_ID = 'projects/earthengine-legacy/assets/projects/openet/insol_data/global_v001_hourly'
# ASSET_COLL_ID = 'projects/earthengine-legacy/assets/projects/disalexi/insol_data/global_v001_hourly'
ASSET_DT_FMT = '%Y%m%d%H'
BUCKET_NAME = 'openet'
# DEADBEEF
BUCKET_FOLDER = 'disalexi-insolation'
# BUCKET_FOLDER = 'disalexi/insoldata_tif'
DATA_VERSION = 3
HOURS = list(range(0, 24))
ISO_DT_FMT = '%Y-%m-%dT%H00'
# Maximum number of new tasks that can be submitted in a function call
NEW_TASKS = 300
# Maximum number of queued tasks (intentionally not setting to 3000)
MAX_TASKS = 1000
# NODATA_VALUE = -9999
START_DAY_OFFSET = 120
END_DAY_OFFSET = 3
STORAGE_CLIENT = storage.Client()
TIF_PREFIX = 'insol_series_'
TIF_NAME_FMT = '{prefix}{date}.tif'
TIF_DT_FMT = '%Y%m%d_%H'
TIF_DT_RE = '(?P<date>\d{8}_\d{2})'
TODAY_DT = datetime.now(timezone.utc)
# TODO: Check these units
UNITS = 'W m-2'
VARIABLE = 'insolation'

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)


def ingest(tgt_dt, workspace, overwrite_flag=False):
    """

    Parameters
    ----------
    tgt_dt : datetime
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """
    logging.info(f'DisALEXI hourly {VARIABLE} - {tgt_dt.strftime("%Y-%m-%dT%H00")}')
    # response = f'DisALEXI hourly {VARIABLE} - {tgt_dt.strftime("%Y-%m")}'

    tif_name = TIF_NAME_FMT.format(prefix=TIF_PREFIX, date=tgt_dt.strftime(TIF_DT_FMT))

    local_ws = os.path.join(workspace, VARIABLE, tgt_dt.strftime(f'%Y'))
    # local_ws = os.path.join(workspace, VARIABLE, tgt_dt.strftime(f'%Y%m%d%H'))
    local_path = os.path.join(local_ws, tif_name)
    bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{tif_name}'
    asset_id = f'{ASSET_COLL_ID}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    export_name = f'disalexi_hourly_{VARIABLE}_{tgt_dt.strftime("%Y%m%d%H")}'

    # logging.debug(f'  {source_path}')
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

    if not os.path.isfile(local_path):
        return f'{export_name} - Image is not available locally, skipping\n'

    # Copy the file to the bucket for ingest and archiving
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{BUCKET_FOLDER}/{tif_name}')
    if blob and (not blob.exists() or overwrite_flag):
        logging.debug('  Uploading to bucket')
        blob.upload_from_filename(local_path)

    properties = {
        'date': tgt_dt.strftime('%Y-%m-%d'),
        'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
        'doy': int(tgt_dt.strftime('%j')),
        'hour': int(tgt_dt.strftime('%H')),
        'insolation_version': DATA_VERSION,
        'bucket_url': bucket_path,
        'units': UNITS,
    }
    params = {
        'name': asset_id,
        'bands': [{'id': VARIABLE}],
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

    logging.info(f'{export_name} - {task["id"]}')
    return f'{export_name} - {task["id"]}\n'


def ingest_dates(start_dt, end_dt, hours, workspace, overwrite_flag=False):
    """Identify hourly datetimes to ingest

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date, inclusive.
    hours : list
    workspace : str
    overwrite_flag : bool, optional

    Returns
    -------
    list of datetimes

    """
    logging.info(f'Building hourly date list')
    logging.info(f'  Start Date: {start_dt.strftime("%Y-%m-%d")}')
    logging.info(f'  End Date:   {end_dt.strftime("%Y-%m-%d")}')
    logging.info(f'  Hours:      {", ".join(map(str, hours))}')

    # Start with a list of dates to check
    test_dt_list = list(hourly_date_range(start_dt, end_dt, hours=hours))
    if not test_dt_list:
        logging.info('Empty date range')
        return []
    # logging.info('\nTest dates: {}'.format(
    #     ', '.join(map(lambda x: x.strftime('%Y-%m-%dT%H00'), test_dt_list))
    # ))
    # logging.info(f'Test dates: {len(test_dt_list)}')

    # Check if the assets already exist
    # For now, assume the collection exists
    logging.debug('\nChecking existing assets (by year)')
    asset_dates = set()
    for year in {test_dt.year for test_dt in test_dt_list}:
        asset_date_coll = (
            ee.ImageCollection(ASSET_COLL_ID)
            .filterDate(start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))
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

    # Finally, check for images
    # TODO: Add code to check if the images have a newer last modified date
    logging.debug('\nChecking local files')
    local_dates = set()
    for year in {test_dt.year for test_dt in test_dt_list}:
        local_date_list = [
            datetime.strptime(m.group('date'), TIF_DT_FMT).strftime(ISO_DT_FMT)
            for item in sorted(os.listdir(os.path.join(workspace, VARIABLE, str(year))))
            for m in [re.search(TIF_DT_RE, item)] if m
        ]
        local_date_list = [
            test_dt for test_dt in local_date_list
            if test_dt >= start_dt.strftime('%Y-%m-%dT%H00') and test_dt < end_dt.strftime('%Y-%m-%dT%H00')
        ]
        if local_date_list:
            local_dates.update(local_date_list)

    # Keep dates that have a server file
    test_dt_list = [dt for dt in test_dt_list if dt.strftime(ISO_DT_FMT) in local_dates]
    if not test_dt_list:
        logging.info('No dates to process after filtering local files')
        return []
    logging.debug('\nDates (after filtering local files): {}'.format(
        ', '.join(map(lambda x: x.strftime(ISO_DT_FMT), test_dt_list))
    ))

    return test_dt_list


def hourly_date_range(start_dt, end_dt, hours=HOURS, skip_leap_days=False):
    """Generate hourly dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (exclusive).
    hours : list, optional
    skip_leap_days : bool, optional
        If True, skip leap days while incrementing (the default is True).

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt < end_dt:
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


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest DisALEXI hourly insolation assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
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

    logging.basicConfig(level=args.loglevel, format='%(message)s')

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

    # Build the image collection if it doesn't exist
    logging.debug(f'Image Collection: {ASSET_COLL_ID}')
    if not ee.data.getInfo(ASSET_COLL_ID):
        logging.info(f'\nImage collection does not exist and will be built'
                     f'\n  {ASSET_COLL_ID}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)

    ingest_dt_list = ingest_dates(
        start_dt=args.start,
        end_dt=args.end,
        hours=list(map(int, args.hours.split(','))),
        workspace=args.workspace,
        overwrite_flag=args.overwrite,
    )
    if args.loglevel == logging.DEBUG:
        pprint.pprint(ingest_dt_list)
        input('ENTER')

    for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
        # logging.info(f'Date: {ingest_dt.strftime("%Y-%m-%d")}')
        response = ingest(
            tgt_dt=ingest_dt,
            workspace=args.workspace,
            overwrite_flag=args.overwrite,
        )
        logging.info(f'  {response}')
        time.sleep(args.delay)
