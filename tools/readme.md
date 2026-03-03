# Tools for identifying missing, nodata, and duplicate insolation and meteo assets

The tools should generally be run in the order listed below (missing, nodata, duplicate).  

The `--meteo`, `--hourly`, and `--daily` flags are used to indicate which datasets to check.

Currently, there is a small issue where the duplicate image check tool sometimes register the images after a nodata image as a duplicate.  There may also be an issue with identifying duplicates on the first or last day of each year.

```commandline
python missing_image_check.py --start 2016-01-01 --end 2022-10-31 --meteo --hourly
```

```commandline
python nodata_image_check.py --start 2016-01-01 --end 2022-10-31 --meteo --hourly
```

```commandline
python duplicate_image_check.py --start 2016-01-01 --end 2022-10-31 --meteo --hourly
```
