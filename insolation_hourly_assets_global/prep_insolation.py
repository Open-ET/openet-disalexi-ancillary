import argparse
from datetime import datetime, timedelta, timezone
import logging
import os

import rasterio

import openet.core.utils as utils


def main(start_dt, end_dt, hours=list(range(0, 24)), overwrite_flag=False):

    band_names = ['insolation']

    for year in {tgt_dt.year for tgt_dt in hourly_date_range(start_dt, end_dt, hours=[0])}:
        logging.debug(f'\nYear: {year}')

        src_ws = os.path.join('source', f'insol_series_{year}')
        dst_ws = os.path.join('insolation', f'{year}')
        if not os.path.isdir(dst_ws):
            os.makedirs(dst_ws)

        for item in sorted(os.listdir(src_ws)):
            item_dt = datetime.strptime(item.split('_')[2], '%Y%m%d')
            if item_dt < start_dt:
                continue
            elif item_dt >= end_dt:
                continue
            elif item_dt.hour not in hours:
                continue
            else:
                logging.info(f'{item}')
                logging.debug(f'  {item_dt}')

            src_path = os.path.join(src_ws, item)
            dst_path = os.path.join(dst_ws, item)
            logging.debug(f'  {src_path}')
            logging.debug(f'  {dst_path}')

            if not os.path.isfile(src_path):
                continue
            if not overwrite_flag and os.path.isfile(dst_path):
                continue

            with rasterio.open(src_path, 'r') as src_ds:
                src_array = src_ds.read()[0]

            # Latitude -60 to 75 (Argentina to Alaska)
            dst_height = 540
            dst_geo = (0.25, 0.0, -180.0, 0.0, -0.25, 75.0)

            # Latitude -60 to 85 (Argentina to Greenland)
            # dst_height = 660
            # dst_geo = (0.25, 0.0, -180.0, 0.0, -0.25, 75.0)

            # # Latitude -90 to 90
            # dst_height = 720
            # dst_geo = (0.25, 0.0, -180.0, 0.0, -0.25, 90.0)

            if dst_height < 720:
                src_i_min = round((90 - dst_geo[5]) / abs(dst_geo[4]))
                src_i_max = src_i_min + dst_height
                # print(src_i_min)
                # print(src_i_max)
                dst_array = src_array[src_i_min: src_i_max, :]
            else:
                dst_array = src_array[:]
            # print(src_array)
            # print(src_array.shape)
            # print(dst_array)
            # print(dst_array.shape)

            with rasterio.open(
                    dst_path, 'w',
                    driver='GTiff',
                    tiled=True,
                    blockxsize=256,
                    blockysize=256,
                    # compress='lzw',
                    compress='deflate',
                    count=1,
                    dtype='float32',
                    nodata=-9999,
                    height=dst_height,
                    width=1440,
                    crs='EPSG:4326',
                    transform=dst_geo,
            ) as output_ds:
                for i, band_name in enumerate(band_names):
                    output_ds.set_band_description(i + 1, band_name)
                    output_ds.write(dst_array, i + 1)


def hourly_date_range(start_dt, end_dt, hours=list(range(0, 24)), skip_leap_days=False):
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
    # while curr_dt < (end_dt + timedelta(days=1)):
    while curr_dt < end_dt:
        if not skip_leap_days or (curr_dt.month != 2) or (curr_dt.day != 29):
            if curr_dt.hour in hours:
                yield curr_dt
        curr_dt += timedelta(hours=1)


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Prep DisALEXI hourly insolation assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='DATE',
        default=datetime(2020, 1, 1),
        # default=(datetime(TODAY_DT.year, TODAY_DT.month, TODAY_DT.day) -
        #          timedelta(days=START_DAY_OFFSET)).strftime('%Y-%m-%d'),
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='DATE',
        default=datetime(2021, 1, 1),
        # default=(datetime(TODAY_DT.year, TODAY_DT.month, TODAY_DT.day) -
        #          timedelta(days=END_DAY_OFFSET)).strftime('%Y-%m-%d'),
        help='End date (format YYYY-MM-DD)')
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
        start_dt=args.start,
        end_dt=args.end,
        overwrite_flag=args.overwrite,
    )
