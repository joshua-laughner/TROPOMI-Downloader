# TROPOMI-Downloader
Python utility to download TROPOMI data. 

## Quickstart
This program requires the package `requests` be available. 
If you are using Anaconda python, this is probably available in your base environment.
It can be run with python3 as `python3 get_tropomi.py`, or if made executable (`chmod u+x`) can be run directly with `./get_tropomi.py`.
Anywhere you see `./get_tropomi.py` in this readme you can substitute `python3 get_tropomi.py`.

There are a number of subcommands for different actions. 
Use the `./get_tropomi.py --help` to see the list of subcommands and `./get_tropomi <subcommand> --help` to see the details for that subcommand.

## Configuration file

This program requires a INI-style configuration file as one of the command line arguments for many of the subcommands.
To create a template config file, use the `make-cfg` subcommand.
Here is an example configuration file:

```
[DEFAULT]
hub = https://s5phub.copernicus.eu/dhus/
num_tries = 5
on_bad_checksum = record
record_file = failed_downloads.txt
username = ****
password = ****
log_block_size = 50M

[NO2]
product = L2__NO2___
output_dir = data

[HCHO]
product = L2__HCHO__
output_dir = data-hcho
record_file = failed_hcho_downloads.txt
```

The `DEFAULT` section contains options common to all products to be downloaded. Other sections define options specific to individual products; options in the specific sections override those in `DEFAULT`.
The options are:

* `hub` - the root URL for the Copernicus hub.
* `num_tries` - how many times to retry a download if you get an HTTP or checksum error before moving on to the next file.
* `on_bad_checksum` - what to do if the checksum of a downloaded file doesn't match to expected hash. The options are:
  - "record": writes the offending file to the record file and moves on to the next file
  - "retry:" tries downloading again; this counts against the `num_tries` option (i.e. if the checksum is wrong too many times will also just move on to the next file)
* `record_file` - where to write reports of failed downloads. If this is a relative path, it will be relative to where you run `get_tropomi.py`
* `username`, `password` - the credentials you use to access the Copernicus Hub.
* `log_block_size` - controls how frequently `get_tropomi.py` will report progress in downloading files. This is a number of bytes, if suffixed by "K", "M", or "G", it is interpreter as kilo-, mega-, or giga- bytes, respectively.
* `product` - which TROPOMI product to download.
* `output_dir` - where to download the TROPOMI data to.

Some options not included in the example:

* `platform` - which satellite to download data for, default is "Sentinel-5"
* `mode` - which processing mode to download data for, default is "Offline", but note that this may not be the best option for all products. Check the TROPOMI product information to verify this.
* `block_size` - size (in bytes, kilobytes, megabytes, or gigabytes) to stream from the hub at once. Default is "1M".

To see the most up to date list of config options, run `./get_tropomi.py make-cfg help`.

## Subcommands

Different subcommands are available. The most commonly useful are:

* `dlbatch` - download TROPOMI files for one product in a specific date range.
* `dlfailed` - retry downloading TROPOMI files that failed to download and were recorded in a record file.
* `check-by-date` or `cbd` - verify the checksums for TROPOMI files in a given date range.

See the subcommand help for the required command line arguments.

## Examples

The following examples assume that you have the configuration file `tropomi.cfg` in the current directory.

*Download files using the "NO2" section in `tropomi.cfg` for summer 2020:*

```
./get_tropomi.py dlbatch tropomi.cfg NO2 20200601 20200831
```

*Download failed files recorded in "failed_downloads.txt":*

```
./get_tropomi.py dlfailed tropomi.cfg NO2 failed_downloads.txt
```
