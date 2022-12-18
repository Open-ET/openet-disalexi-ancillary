import argparse
from collections import defaultdict
from datetime import datetime, timedelta
import logging
import pprint
import time

import ee

import openet.core.utils as utils

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)


def main(start_dt, end_dt, insol_hourly_flag=False, insol_daily_flag=False,
         meteo_flag=False, conus_flag=False):
    """"""
    insol_hourly_coll_id = 'projects/disalexi/insol_data/global_v001_hourly'
    insol_hourly_band_name = 'insolation'

    insol_daily_coll_id = 'projects/disalexi/insol_data/global_v001_daily_conus'
    insol_daily_band_name = 'rs'

    meteo_coll_id_fmt = 'projects/disalexi/meteo_data/{meteo_var}/global_v001_3hour'
    # These will be zipped and need to be in the same order
    meteo_coll_names = ['airtemperature', 'airpressure', 'windspeed', 'vp']
    meteo_band_names = ['temperature', 'airpressure', 'windspeed', 'vp']

    years = list(range(int(start_dt.year), int(end_dt.year)+1))
    # logging.debug(years)

    if not insol_hourly_flag and not insol_daily_flag and not meteo_flag:
        logging.info('\nNo processing flags were set, exiting')
        return False

    ee.Initialize()


    # Hourly Insolation
    if insol_hourly_flag:
        logging.info('\nHourly Insolation')
        for year in years:
            logging.debug(f'  {year}')

            target_dates = {
                d.strftime('%Y%m%d%H')
                for d in dt_range(
                    datetime(year, 1, 1), datetime(year+1, 1, 1),
                    hours=list(range(0, 24))
                )
                if d < datetime(2022, 11, 1)}

            hourly_coll = ee.ImageCollection(insol_hourly_coll_id)\
                .filterDate(f'{year}-01-01', f'{year+1}-01-01')\
                .select([insol_hourly_band_name], ['b0'])
            hourly_dates = get_dates(hourly_coll)

            missing_dates = target_dates - set(hourly_dates)
            # if missing_dates:
            for missing_date in sorted(missing_dates):
                missing_dt = datetime.strptime(missing_date, "%Y%m%d%H")
                logging.info(f'{missing_dt.strftime("%Y-%m-%d  %H")}')


    # Daily Insolation
    if insol_daily_flag:
        logging.info('\nDaily Insolation')
        for year in years:
            logging.debug(f'{year}')
            target_dates = {
                d.strftime('%Y%m%d%H')
                for d in dt_range(
                    datetime(year, 1, 1), datetime(year+1, 1, 1), hours=[0]
                )
                if d < datetime(2022, 11, 1)
            }
            insol_coll = ee.ImageCollection(insol_daily_coll_id)\
                .filterDate(f'{year}-01-01', f'{year+1}-01-01')\
                .select([insol_daily_band_name], ['b0'])
            daily_dates = get_dates(insol_coll)
            missing_dates = target_dates - set(daily_dates)
            for missing_date in sorted(missing_dates):
                missing_dt = datetime.strptime(missing_date, "%Y%m%d%H")
                logging.info(f'{missing_dt.strftime("%Y-%m-%d")}')


    # Meteo 3-hour variables
    if meteo_flag:
        logging.info(f'\nMeteo Variables')
        for coll_name, band_name in zip(meteo_coll_names, meteo_band_names):
            logging.info(f'\n{coll_name}')
            meteo_coll_id = meteo_coll_id_fmt.format(meteo_var=coll_name)
            for year in years:
                logging.debug(f'  {year}')

                target_dates = {
                    d.strftime('%Y%m%d%H')
                    for d in dt_range(
                        datetime(year, 1, 1), datetime(year+1, 1, 1),
                        hours=list(range(0, 24, 3))
                    )
                    if d < datetime(2022, 11, 1)
                }

                meteo_coll = ee.ImageCollection(meteo_coll_id)\
                    .filterDate(f'{year}-01-01', f'{year+1}-01-01')\
                    .select([band_name], ['b0'])
                meteo_dates = get_dates(meteo_coll)

                missing_dates = target_dates - set(meteo_dates)
                # if missing_dates:
                for missing_date in sorted(missing_dates):
                    missing_dt = datetime.strptime(missing_date, "%Y%m%d%H")
                    logging.info(f'{missing_dt.strftime("%Y-%m-%d  %H")}')


def get_dates(coll):
    for i in range(6):
        date_list = []
        try:
            date_list = coll.aggregate_array('system:index').getInfo()
            if date_list:
                break
        except Exception as e:
            logging.info(f' {e}\n  Retrying getInfo request')
            time.sleep(i ** 3)

    return date_list


def dt_range(start_dt, end_dt, hours=list(range(0, 24)), skip_leap_days=False):
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


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Check DisALEXI ancillary assets for missing images',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        default='2001-01-01', help='Start date')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        default='2022-10-31', help='End date (inclusive)')
    parser.add_argument(
        '--daily', default=False, action='store_true',
        help='Check daily insolation assets')
    parser.add_argument(
        '--hourly', default=False, action='store_true',
        help='Check hourly insolation assets')
    parser.add_argument(
        '--meteo', default=False, action='store_true',
        help='Check 3-hour meteorology assets')
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
        start_dt=args.start,
        end_dt=args.end,
        insol_hourly_flag=args.hourly,
        insol_daily_flag=args.daily,
        meteo_flag=args.meteo,
    )
