import argparse
import datetime
import logging
import os
import re
import time

from dateutil.relativedelta import relativedelta
import ee
from flask import abort, Response
# from google.auth.transport.requests import AuthorizedSession

import openet.core.utils as utils

# CGM - Switch over to default credentials after historical images are loaded
# if 'FUNCTION_REGION' in os.environ:
#     # Assume code is deployed to a cloud function
#     logging.debug(f'\nInitializing GEE using application default credentials')
#     import google.auth
#     credentials, project_id = google.auth.default(
#         default_scopes=['https://www.googleapis.com/auth/earthengine'])
#     ee.Initialize(credentials)
if 'FUNCTION_REGION' in os.environ:
    ee.Initialize(ee.ServiceAccountCredentials('', key_file='steel-melody-gee.json'))

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

# ASSET_COLL_ID = 'projects/earthengine-legacy/assets/' \
#                 'projects/disalexi/insol_data/global_v001_daily'
ASSET_COLL_FOLDER = 'projects/earthengine-legacy/assets/projects/disalexi/insol_data'
ASSET_COLL_NAME = {
    'global': 'global_v001_daily',
    'conus': 'global_v001_daily_conus',
}
ASSET_DT_FMT = '%Y%m%d'
SOURCE_COLL_ID = 'projects/earthengine-legacy/assets/' \
                 'projects/disalexi/insol_data/global_v001_hourly'
# Maximum number of new tasks that can be submitted in a function call
NEW_TASKS = 200
# Maximum number of queued tasks (intentionally not setting to 3000)
MAX_TASKS = 1000
# NODATA_VALUE = -9999
START_MONTH_OFFSET = 4
END_MONTH_OFFSET = 0


def ingest(tgt_dt, region, variable='insolation', overwrite_flag=False):
    """

    Parameters
    ----------
    tgt_dt : datetime
    region : {'conus', 'global'}
    variable: {'insolation'}, optional
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """
    # tgt_date = tgt_dt.strftime('%Y%m%d')

    logging.info(f'DisALEXI daily {variable} - {tgt_dt.strftime("%Y-%m-%d")}')
    # response = f'DisALEXI daily {variable} - {tgt_dt.strftime("%Y-%m")}'

    try:
        asset_coll_id = f'{ASSET_COLL_FOLDER}/{ASSET_COLL_NAME[region]}'
    except KeyError:
        raise ValueError(f'Unsupported region parameter: {region}')
    asset_id = f'{asset_coll_id}/{tgt_dt.strftime(ASSET_DT_FMT)}'

    export_name = f'disalexi_daily_{variable}_{region}_{tgt_dt.strftime("%Y%m%d")}'

    logging.debug(f'  {SOURCE_COLL_ID}')
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

    if region.lower() == 'conus':
        utc_offset = 6
    else:
        utc_offset = 0
    logging.debug(f'  UTC offset: {utc_offset}')

    start_dt = tgt_dt + datetime.timedelta(hours=utc_offset)
    end_dt = start_dt + datetime.timedelta(days=1)
    logging.debug(f'  {start_dt.strftime("%Y-%m-%d %H%M")}')
    logging.debug(f'  {end_dt.strftime("%Y-%m-%d %H%M")}')

    source_coll = ee.ImageCollection(SOURCE_COLL_ID)\
        .filterDate(start_dt, end_dt)
    # print(source_coll.aggregate_array('system:index').getInfo())
    # input('ENTER')

    # TODO: Check if there is a different exception for the collection not existing
    #   vs being empty after filtering vs any other EE error
    try:
        source_count = source_coll.size().getInfo()
    except Exception as e:
        logging.info(str(e))
        source_count == -1

    if source_count == -1:
        return f'{export_name} - source image count error\n'
    if source_count == 0:
        return f'{export_name} - source image does not exist\n'
    elif source_count < 24:
        return f'{export_name} - too few source images ({source_count}) for day\n'

    # Use the first image for getting the image properties
    # Assume all properties for the day will be the same
    source_img = source_coll.first()
    properties = {
        'system:time_start': utils.millis(tgt_dt),
        'date_ingested': source_img.get('date_ingested'),
        'doy': tgt_dt.strftime('%j'),
        'insolation_version': source_img.get('insolation_version'),
        'units': source_img.get('units'),
        'utc_offset': utc_offset,
    }

    # Sum the hourly images to daily
    # CGM - Reducing the collection was causing an error,
    #   but going through .toBands() seems to work
    output_img = source_coll.toBands().reduce(ee.Reducer.sum())\
        .rename(['rs']).toInt16()

    if region == 'conus':
        output_img = output_img.resample('bicubic')
        #     .reproject(crs=export_crs, crsTransform=export_transform)
        properties['resample_method'] = 'bicubic'

    if region == 'global':
        asset_transform = [0.25, 0, -180.0, 0, -0.25, 90.0]
        asset_shape = '1440x720'
        asset_crs = 'EPSG:4326'
    elif region == 'conus':
        # CGM - Matching transform of v004/v005 ALEXI
        asset_transform = [0.04, 0.0, -125.02, 0.0, -0.04, 49.78]
        asset_shape = '1456x625'
        asset_crs = 'EPSG:4326'

    # CGM - Could get projection from one of the images
    # asset_info = source_img.select([0]).getInfo()
    # asset_crs = asset_info['bands'][0]['projection']
    # asset_shape = asset_info['bands'][0]['dimensions']
    # asset_shape = '{0}x{1}'.format(*asset_shape)
    # asset_transform = asset_info['bands'][0]['crs_transform']

    task = ee.batch.Export.image.toAsset(
        image=output_img.set(properties),
        description=export_name,
        assetId=asset_id,
        crs=asset_crs,
        crsTransform=asset_transform,
        dimensions=asset_shape,
    )

    # Start the export task
    for i in range(1, 6):
        try:
            task.start()
            break
        except ee.ee_exception.EEException as e:
            logging.warning(f'EE Exception, retry {i}\n{e}')
            time.sleep(i ** 3)
        except Exception as e:
            logging.warning(f'Unhandled Exception: {e}')
            return f'Unhandled Exception: {e}'

    return f'{export_name} - {task.id}\n'


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
    logging.info('Export DisALEXI Daily Insolation (from hourly)')

    request_json = request.get_json(silent=True)
    request_args = request.args

    variable = 'insolation'
    # if request_json and 'variable' in request_json:
    #     variable = request_json['variable']
    # elif request_args and 'variable' in request_args:
    #     variable = request_args['variable']
    # else:
    #     abort(404, description='variable must be specified')

    if request_json and 'region' in request_json:
        region = request_json['region']
    elif request_args and 'region' in request_args:
        region = request_args['region']
    else:
        abort(404, description='Region must be specified')
        # region = 'global'

    # if request_json and 'utc_offset' in request_json:
    #     utc_offset = request_json['utc_offset']
    # elif request_args and 'utc_offset' in request_args:
    #     utc_offset = request_args['utc_offset']
    # else:
    #     utc_offset = 0

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

    if not start_date and not end_date:
        today = datetime.datetime.today()
        start_dt = (datetime.datetime(today.year, today.month, today.day) -
                    relativedelta(months=START_MONTH_OFFSET))
        end_dt = (datetime.datetime(today.year, today.month, today.day) -
                  relativedelta(months=END_MONTH_OFFSET))
        # start_dt = (datetime.datetime(today.year, today.month, today.day) -
        #             relativedelta(days=START_DAY_OFFSET))
        # end_dt = (datetime.datetime(today.year, today.month, today.day) -
        #           relativedelta(days=END_DAY_OFFSET))
    elif start_date and end_date:
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
        # elif (end_dt - start_dt) > datetime.timedelta(days=400):
        #     abort(404, description='No more than 1 year can be processed in a single request')
        # if start_dt < datetime.datetime(1980, 1, 1):
        #     logging.debug('Start Date: {} - no CIMIS images before '
        #                   '1980-01-01'.format(start_dt.strftime('%Y-%m-%d')))
        #     start_dt = datetime.datetime(1980, 1, 1)
    else:
        abort(404, description='Both start and end date must be specified')

    args = {
        'start_dt': start_dt, 'end_dt': end_dt,
        'region': region, 'variable': variable, 'limit': NEW_TASKS,
    }

    count = 0
    for tgt_dt in ingest_dates(**args):
        ingest(tgt_dt, variable, overwrite_flag=True)
        count += 1

    return Response(f'Exported {count} new assets', mimetype='text/plain')


def ingest_dates(start_dt, end_dt, region, variable, limit, overwrite_flag=False):
    """Identify daily datetimes to ingest

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date, inclusive.
    region : {'conus', 'global'}
    variable : str
    limit : int
    overwrite_flag : bool, optional

    Returns
    -------
    list of datetimes

    """
    logging.info(f'Building daily date list')
    logging.info(f'  Start Date: {start_dt.strftime("%Y-%m-%d")}')
    logging.info(f'  End Date:   {end_dt.strftime("%Y-%m-%d")}')

    task_id_re = re.compile('disalexi_daily_{variable}_{region}_(?P<date>\d{8})')
    # asset_id_re = re.compile(
    #     ASSET_COLL_ID.split('projects/')[-1] + '/(?P<date>\d{8})$')

    # Start with a list of dates to check
    test_dt_list = list(date_range(start_dt, end_dt, skip_leap_days=False))
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
        for desc in get_ee_tasks(states=['RUNNING', 'READY']).keys()]
    task_count = len(task_id_list)
    task_dates = {
        datetime.datetime.strptime(m.group('date'), '%Y%m%d').strftime('%Y-%m-%d')
        for task_id in task_id_list for m in [task_id_re.search(task_id)] if m}
    # logging.debug('Task dates: {", ".join(sorted(task_dates))}')

    # Switch date list to be dates that are missing
    test_dt_list = [
        dt for dt in test_dt_list
        if overwrite_flag or dt.strftime('%Y-%m-%d') not in task_dates]
    if not test_dt_list:
        logging.info('All dates are queued for export')
        return []
    # else:
    #     logging.info('\nMissing asset dates: {}'.format(', '.join(
    #         map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))

    # Check if the assets already exist
    # For now, assume the collection exists
    # Bump end date for filterDate() calls
    logging.debug('\nChecking existing assets')
    try:
        asset_coll_id = f'{ASSET_COLL_FOLDER}/{ASSET_COLL_NAME[region]}'
    except KeyError:
        raise ValueError(f'Unsupported region parameter: {region}')
    asset_date_coll = ee.ImageCollection(asset_coll_id)\
        .filterDate(start_dt.strftime('%Y-%m-%d'),
                    (end_dt + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
    asset_dates = set(asset_date_coll.aggregate_array('system:index').getInfo())
    # logging.debug(f'\nAsset dates: {", ".join(sorted(asset_dates))}')

    # Switch date list to be dates that are missing
    test_dt_list = [
        dt for dt in test_dt_list
        if overwrite_flag or dt.strftime(ASSET_DT_FMT) not in asset_dates]
    if not test_dt_list:
        logging.info('No dates to process after filtering existing assets')
        return []
    logging.debug('\nDates (after filtering existing assets): {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))

    # TODO: Should the source collection be checked here to see if there are
    #   enough images?

    # Limit the number of dates returned to the number of open queue spots
    if limit:
        new_tasks = min(max(MAX_TASKS - len(task_id_list), 0), limit)
        logging.debug(f'Date count:    {len(test_dt_list)}')
        logging.debug(f'Date limit:    {limit}')
        logging.info(f'Queued tasks:  {task_count}')
        logging.info(f'Limited dates: {new_tasks}')
        test_dt_list = test_dt_list[:new_tasks]

    return test_dt_list


def date_range(start_dt, end_dt, days=1, skip_leap_days=False):
    """Generate dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date.
    days : int, optional
        Step size (the default is 1).
    skip_leap_days : bool, optional
        If True, skip leap days while incrementing (the default is True).

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt <= end_dt:
        if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
            yield curr_dt
        curr_dt += datetime.timedelta(days=days)


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


def arg_parse():
    """"""
    today = datetime.date.today()

    parser = argparse.ArgumentParser(
        description='Generate DisALEXI daily insolation assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='DATE',
        default=(datetime.datetime(today.year, today.month, today.day) -
                 relativedelta(months=START_MONTH_OFFSET)).strftime('%Y-%m-%d'),
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='DATE',
        default=(datetime.datetime(today.year, today.month, today.day) -
                 relativedelta(months=END_MONTH_OFFSET)).strftime('%Y-%m-%d'),
        help='End date (format YYYY-MM-DD)')
    parser.add_argument(
        '--region', required=True, choices=['conus', 'global'], help='Region')
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

    # Build the image collection if it doesn't exist
    try:
        asset_coll_id = f'{ASSET_COLL_FOLDER}/{ASSET_COLL_NAME[args.region]}'
    except KeyError:
        raise ValueError(f'Unsupported region parameter: {args.region}')
    logging.debug(f'Image Collection: {asset_coll_id}')
    if not ee.data.getInfo(asset_coll_id):
        logging.info(f'\nImage collection does not exist and will be built'
                     f'\n  {asset_coll_id}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, asset_coll_id)

    ingest_dt_list = ingest_dates(
        args.start, args.end, region=args.region, variable='insolation',
        limit=args.limit, overwrite_flag=args.overwrite)

    for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
        # logging.info(f'Date: {ingest_dt.strftime("%Y-%m-%d")}')
        response = ingest(ingest_dt, region=args.region, variable='insolation',
                          overwrite_flag=args.overwrite)
        logging.info(f'  {response}')
        time.sleep(args.delay)
