import argparse
from collections import defaultdict
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
         meteo_flag=False, conus_flag=False, ignore_dec31_flag=True):
    """"""
    start_date = start_dt.strftime('%Y-%m-%d')
    end_date = end_dt.strftime('%Y-%m-%d')

    insol_hourly_coll_id = 'projects/disalexi/insol_data/global_v001_hourly'
    insol_hourly_band_name = 'insolation'

    insol_daily_coll_id = 'projects/disalexi/insol_data/global_v001_daily_conus'
    insol_daily_band_name = 'rs'

    meteo_coll_id_fmt = 'projects/disalexi/meteo_data/{meteo_var}/global_v001_3hour'
    # These will be zipped and need to be in the same order
    meteo_coll_names = ['airtemperature', 'airpressure', 'windspeed', 'vp']
    meteo_band_names = ['temperature', 'airpressure', 'windspeed', 'vp']
    # meteo_coll_names = ['vp']
    # meteo_band_names = ['vp']

    years = list(range(int(start_dt.year), int(end_dt.year)+1))
    hours = list(range(0, 24))
    hr3 = list(range(0, 24, 3))
    # hours = [0] + list(range(13, 24))
    # hours = [21]
    # hr3 = [21]

    if not insol_hourly_flag and not insol_daily_flag and not meteo_flag:
        logging.info('\nNo processing flags were set, exiting')
        return False

    ee.Initialize()


    # Hourly Insolation
    if insol_hourly_flag:
        logging.info('\nHourly Insolation')
        for year in years:
            logging.debug(f'  {year}')

            hourly_dates = defaultdict(list)
            for hour in hours:
                logging.debug(f'Hour: {hour:>2d}')

                insol_coll = ee.ImageCollection(insol_hourly_coll_id)\
                    .filterDate(f'{year}-01-01', f'{year+1}-01-01')\
                    .filter(ee.Filter.calendarRange(hour, hour, 'hour'))\
                    .select([insol_hourly_band_name], ['b0'])
                output = duplicate_dates(insol_coll, conus_flag, ignore_dec31_flag)

                for item in output:
                    logging.debug(f'  {item}')
                    hourly_dates[item.split('_')[0]].append(item.split('_')[1])

            for date, hours in sorted(hourly_dates.items()):
                hours_str = ", ".join([str(int(h)) for h in sorted(hours)])
                logging.info(f'{date}  {hours_str}')


    # Daily Insolation
    if insol_daily_flag:
        logging.info('\nDaily Insolation')
        insol_coll = ee.ImageCollection(insol_daily_coll_id)\
            .filterDate(start_date, end_date)\
            .select([insol_daily_band_name], ['b0'])
        output = duplicate_dates(insol_coll, conus_flag, ignore_dec31_flag)
        if output:
            logging.info(output)


    # Meteo 3 hour variables
    if meteo_flag:
        logging.info(f'\nMeteo Variables')
        for coll_name, band_name in zip(meteo_coll_names, meteo_band_names):
            logging.info(f'\n{coll_name}')
            meteo_coll_id = meteo_coll_id_fmt.format(meteo_var=coll_name)
            for year in years:
                logging.debug(f'  {year}')

                for hour in hr3:
                    logging.debug(f'3-Hour: {hour:>2d}')
                    meteo_coll = ee.ImageCollection(meteo_coll_id)\
                        .filterDate(f'{year}-01-01', f'{year+1}-01-01')\
                        .filter(ee.Filter.calendarRange(hour, hour, 'hour'))\
                        .select([band_name], ['b0'])
                    output = duplicate_dates(meteo_coll, conus_flag, ignore_dec31_flag)

                    meteo_var_dates = defaultdict(list)
                    for item in output:
                        meteo_var_dates[item.split('_')[0]].append(item.split('_')[1])
                    for date, hours in sorted(meteo_var_dates.items()):
                        hours_str = ", ".join([str(int(h)) for h in sorted(hours)])
                        logging.info(f'{date}  {hours_str}')


def duplicate_dates(coll, conus_flag=True, ignore_dec31_flag=True):
    def set_join_id(img):
        date = ee.Date(img.get('system:time_start'))
        return img.set('join_id', date.format('yyyyMMdd'))

    def set_offset_id(img):
        date = ee.Date(img.get('system:time_start'))
        return img.set('join_id', date.advance(1, 'day').format('yyyyMMdd'))

    primary = coll.map(set_join_id)
    secondary = coll.map(set_offset_id)
    filter = ee.Filter.equals(leftField='join_id', rightField='join_id')
    join_coll = ee.Join.inner().apply(primary, secondary, filter)

    if conus_flag:
        geom = ee.Geometry.BBox(-125, 25, -65, 50)
    else:
        geom = ee.Geometry.BBox(-125, 25, 145, 50)

    def compute_diff(ftr):
        primary_img = ee.Image(ftr.get('primary'))
        secondary_img = ee.Image(ftr.get('secondary'))
        ftr_date = ee.Date(primary_img.get('system:time_start'))
        diff_img = primary_img.subtract(secondary_img).abs()
        sum = diff_img.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            crs='EPSG:4326',
            crsTransform=[0.25, 0, -180,0, -0.25, 90],
            bestEffort=False,
        )
        return ee.Feature(
            None, {'sum': sum.get('b0'), 'date': ftr_date.format('yyyy-MM-dd_HH')}
        )

    for i in range(6):
        date_list = []
        try:
            date_list = ee.FeatureCollection(join_coll.map(compute_diff))\
                .filterMetadata('sum', 'less_than', 0.001)\
                .aggregate_array('date')\
                .getInfo()
            if date_list:
                break
        except Exception as e:
            logging.info(f' {e}\n  Retrying getInfo request')
            time.sleep(i ** 3)

    # Remove the Dec 31st dates from the list
    if ignore_dec31_flag:
        date_list = [d for d in date_list if '-12-31_' not in d]

    return date_list


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Check DisALEXI ancillary assets for nodata images',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        default='2001-01-01', help='Start date')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        default='2022-11-01', help='End date (exclusive)')
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
