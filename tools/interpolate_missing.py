import argparse
from datetime import datetime, timedelta
import logging
import os
import pprint
import re
import sys
import time

import ee

import openet.core.utils as utils

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

ASSET_DT_FMT = '%Y%m%d%H'


def main(tgt_dt, gee_key_file, project_id, overwrite_flag=False):
    """Interpolate missing DisALEXI ancillary assets

    Parameters
    ----------
    tgt_dt : datetime
        Target date to fill
    gee_key_file : str, optional
        Earth Engine service account JSON key file (the default is None).
    project_id : str, optional
        File path to an Earth Engine json key file (the default is None).
    overwrite_flag : bool, optional
        If True, overwrite existing assets (the default is False).

    Returns
    -------
    None

    """
    logging.info('\nInterpolate missing DisALEXI ancillary assets')

    if gee_key_file:
        logging.info(f'\nInitializing GEE using user key file: {gee_key_file}')
        try:
            ee.Initialize(ee.ServiceAccountCredentials('_', key_file=gee_key_file))
        except ee.ee_exception.EEException:
            logging.warning('Unable to initialize GEE using user key file')
            return False
    elif project_id is not None:
        logging.info(f'\nInitializing Earth Engine using project credentials'
                     f'\n  Project ID: {project_id}')
        try:
            ee.Initialize(project=project_id)
        except Exception as e:
            logging.warning(f'\nUnable to initialize GEE using project ID\n  {e}')
            return False
    else:
        logging.info('\nInitializing Earth Engine using user credentials')
        ee.Initialize()


    # Meteorology
    for var_name in ['airpressure', 'temperature', 'vaporpressure', 'windspeed']:
        folder = {
            'airpressure': 'airpressure',
            'temperature': 'airtemperature',
            'vaporpressure': 'vp',
            'windspeed': 'windspeed',
        }
        coll_root = 'projects/earthengine-legacy/assets/projects/disalexi/meteo_data'
        coll_id = f'{coll_root}/{folder[var_name]}/global_v001_3hour'

        band_name = {
            'airpressure': 'airpressure',
            'temperature': 'temperature',
            'vaporpressure': 'vp',
            'windspeed': 'windspeed',
        }
        units = {
            'airpressure': 'kPa',
            'temperature': 'K',
            'vaporpressure': 'kPa',
            'windspeed': 'm s-1',
        }

        for hour in [0, 3, 6, 9, 12, 15, 18, 21]:
            hour_dt = tgt_dt + timedelta(hours=hour)
            asset_id = f'{coll_id}/{hour_dt.strftime(ASSET_DT_FMT)}'
            logging.info(f'\n{asset_id}')

            if ee.data.getInfo(asset_id):
                if overwrite_flag:
                    logging.info('  Asset already exists - removing')
                    ee.data.deleteAsset(asset_id)
                else:
                    logging.info('  Asset already exists - skipping')
                    continue

            # Get the image IDs for the bracketing images
            asset_prev_dt = hour_dt - timedelta(days=1)
            asset_next_dt = hour_dt + timedelta(days=1)
            asset_prev_id = f'{coll_id}/{asset_prev_dt.strftime(ASSET_DT_FMT)}'
            asset_next_id = f'{coll_id}/{asset_next_dt.strftime(ASSET_DT_FMT)}'
            logging.debug(f'  {asset_prev_id}')
            logging.debug(f'  {asset_next_id}')
            #asset_prev_info = ee.Image(asset_prev_id).getInfo()
            #asset_next_info = ee.Image(asset_prev_id).getInfo()

            # Compute the missing image as the mean of the bracketing images
            # This approach only works for gaps of 1 day
            output_img = (
                ee.Image(asset_prev_id).select([band_name[var_name]])
                .add(ee.Image(asset_next_id).select([band_name[var_name]]))
                .multiply(0.5)
                .float()
                .rename(band_name[var_name])
                .set({
                    'system:time_start': ee.Date(hour_dt.strftime('%Y-%m-%dT%H:00:00')).millis(),
                    'date': hour_dt.strftime("%Y-%m-%d"),
                    'date_ingested': datetime.today().strftime('%Y-%m-%d'),
                    'doy': int(hour_dt.strftime("%j")),
                    'hour': int(hour_dt.strftime("%H")),
                    'meteo_version': 1,
                    'source': 'interpolate',
                    'units': units[var_name],
                })
            )

            try:
                task = ee.batch.Export.image.toAsset(
                    image=output_img,
                    description=f'disalexi_interpolate_ancillary_{var_name}_{hour_dt.strftime(ASSET_DT_FMT)}',
                    assetId=asset_id,
                    dimensions=[1440, 600],
                    crs='EPSG:4326',
                    crsTransform=[0.25, 0, -180, 0, -0.25, 90],
                )
            except Exception as e:
                logging.info(f'  Export task not built, skipping\n  {e}')
                continue
            ee_task_start(task, n=4)
            logging.info(f'  Starting export task - {task.id}')


    # Insolation
    var_name = 'insolation'
    coll_id = f'projects/earthengine-legacy/assets/projects/disalexi/insol_data/global_v001_hourly'
    for hour in range(24):
        hour_dt = tgt_dt + timedelta(hours=hour)
        asset_id = f'{coll_id}/{hour_dt.strftime(ASSET_DT_FMT)}'
        logging.info(f'\n{asset_id}')

        if ee.data.getInfo(asset_id):
            if overwrite_flag:
                logging.info('  Asset already exists - removing')
                ee.data.deleteAsset(asset_id)
            else:
                logging.info('  Asset already exists - skipping')
                continue

        # Get the image IDs for the bracketing images
        asset_prev_dt = hour_dt - timedelta(days=1)
        asset_next_dt = hour_dt + timedelta(days=1)
        asset_prev_id = f'{coll_id}/{asset_prev_dt.strftime(ASSET_DT_FMT)}'
        asset_next_id = f'{coll_id}/{asset_next_dt.strftime(ASSET_DT_FMT)}'
        logging.debug(f'  {asset_prev_id}')
        logging.debug(f'  {asset_next_id}')
        asset_prev_info = ee.Image(asset_prev_id).getInfo()
        # asset_next_info = ee.Image(asset_prev_id).getInfo()

        # Compute the missing image as the mean of the bracketing images
        # This approach only works for gaps of 1 day
        output_img = (
            ee.Image(asset_prev_id).select([var_name])
            .add(ee.Image(asset_next_id).select([var_name]))
            .multiply(0.5)
            .float()
            .rename(var_name)
            .set({
                'system:time_start': ee.Date(hour_dt.strftime('%Y-%m-%dT%H:00:00')).millis(),
                'date': hour_dt.strftime("%Y-%m-%d"),
                'date_ingested': datetime.today().strftime('%Y-%m-%d'),
                'doy': int(hour_dt.strftime("%j")),
                'hour': int(hour_dt.strftime("%H")),
                'meteo_version': 1,
                'source': 'interpolate',
                'units': 'W m-2',
            })
        )

        try:
            task = ee.batch.Export.image.toAsset(
                image=output_img,
                description=f'disalexi_interpolate_ancillary_{var_name}_{hour_dt.strftime(ASSET_DT_FMT)}',
                assetId=asset_id,
                dimensions=[1440, 600],
                crs='EPSG:4326',
                crsTransform=[0.25, 0, -180, 0, -0.25, 90],
            )
        except Exception as e:
            logging.info(f'  Export task not built, skipping\n  {e}')
            continue
        ee_task_start(task, n=4)
        logging.info(f'  Starting export task - {task.id}')


def ee_task_start(task, n=4):
    """Make an exponential backoff Earth Engine request"""
    output = None
    for i in range(1, n):
        try:
            task.start()
            break
        except Exception as e:
            logging.info(f'    Resending query ({i}/{n-1})')
            logging.debug(f'    {e}')
            time.sleep(i ** 3)

    return task


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Interpolate/fill missing DisALEXI ancillary assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--date', required=True, type=utils.arg_valid_date, help='Date to interpolate')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='Earth Engine service account JSON key file')
    parser.add_argument(
        '--project', default=None,
        help='Google cloud project ID to use for GEE authentication')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(
        tgt_dt=args.date,
        gee_key_file=args.key,
        project_id=args.project,
        overwrite_flag=args.overwrite,
    )
