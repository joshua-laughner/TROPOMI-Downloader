#!/usr/bin/env python3
from argparse import ArgumentParser
import certifi
import configparser
import datetime as dt
import hashlib
import logging
import os
import re
import requests
import shutil
import time

# Either need to use their API: https://sentinelsat.readthedocs.io/en/stable/install.html
# or try requests - urllib3 doesn't like spaces in the URL

logger = logging.getLogger('gggplotter')
sample_cfg = """[DEFAULT]
hub = https://s5phub.copernicus.eu/dhus/
num_tries = 5
on_bad_checksum = record
record_file = failed_downloads.txt
username = 
password = 

[NO2]
product = L2__NO2___
"""

class ConfigError(Exception):
    pass


class HTTPError(Exception):
    pass


def build_query(date, hub, product, platform, mode):
    urlfmt = '{hub}/search?q=beginPosition:[{start} TO {stop}] AND platformname:{platform} AND producttype:{product} AND processingmode:{mode}&rows=50&start=0'
    start = date.strftime('%Y-%m-%dT00:00:00.000Z')
    stop = date.strftime('%Y-%m-%dT23:59:59.000Z')
    return urlfmt.format(hub=hub, start=start, stop=stop, platform=platform, product=product, mode=mode)


def find_tags(xml, tag, start_at=0):
    tagged_lines = []
    i = 0
    for line in xml.splitlines():
        if re.match(tag, line):
            if i >= start_at:
                tagged_lines.append(line)
            i += 1
    return tagged_lines


def get_url(url, username, password, tries=5, **kws):
    logger.info('Requesting %s', url)
    r = requests.get(url, auth=(username, password), **kws)
    n = 1
    while r.status_code != 200 and n < tries:
        logger.warning('Request failed with status %d, waiting 30 sec before trying again', r.status_code)
        time.sleep(30)
        r = requests.get(url, auth=(username, password), **kws)
        n += 1
        
    if r.status_code != 200:
        raise HTTPError('Failed to return query after {} attempts'.format(n))
        
    return r


def build_product_list_for_date(date, query_args, username, password, tries=5):
    query = build_query(date, **query_args)
    r = get_url(query, username, password)
    xml = r.text
    return extract_products(xml)


def extract_products(xml):
    ids = find_tags(xml, '<id>', 1)
    ids = [re.search('(?<=\<id\>).+(?=\</id\>)', x).group() for x in ids]

    links = find_tags(xml, '<link rel="alternative" href=')
    links = [re.search('(?<=href\=").+(?=/"/\>)', x).group() for x in links]
    
    filenames = find_tags(xml, '<title>', 1)
    filenames = [re.search('(?<=\<title\>).+(?=\</title\>)', x).group() for x in filenames]
    
    return sorted([{'id': i, 'link': l, 'file': f} for i, l, f in zip(ids, links, filenames)], key=lambda x: x['file'])


def build_failed_list(failed_list_file, cfg):
    hub = cfg['hub']
    failed_list = []
    with open(failed_list_file) as f:
        for line in f:
            filename, product_id, md5 = [x.strip() for x in line.split()]
            link, _ = build_product_url(hub, product_id)
            failed_list.append({'id': product_id, 'link': link, 'file': filename})

    return failed_list
            


def build_product_url(hub, product_id):
    root = "{hub}/odata/v1/Products('{prod_id}')".format(hub=hub, prod_id=product_id)
    data_url = "{}/$value".format(root)
    md5_url = '{}/Checksum/Value/$value'.format(root)
    return data_url, md5_url


def _convert_block_size(block):
    if isinstance(block, int):
        return block
    elif isinstance(block, str):
        m = re.match(r'(\d+)([KMG]?)$', block)
        val = int(m.group(1))
        if m.group(2) == 'K':
            return val*1024
        elif m.group(2) == 'M':
            return val*1024**2
        elif m.group(3) == 'G':
            return val*1024**3
        else:
            return val
        
        
def _pretty_bytes(b):
    if b > 1024**3:
        return '{:.2f} GB'.format(b / 1024**3)
    elif b > 1024**2:
        return '{:.2f} MB'.format(b / 1024**2)
    elif b > 1024:
        return '{:.2f} KB'.format(b / 1024)
    else:
        return '{:.0f} B'.format(b)
    
    
def _compute_hash(filename):
    chksum = hashlib.md5()
    with open(filename, 'rb') as robj:
        chunk = robj.read(1000000)
        while chunk:
            chksum.update(chunk)
            chunk = robj.read()
            
    return chksum.hexdigest()


def download_product_file(product_id, output_name, cfg):
    hub = cfg['hub']
    username = cfg['username']
    password = cfg['password']
    block_size = cfg.get('block_size', '1M')
    checksum_action = cfg.get('on_bad_checksum', 'record')
    tries = cfg.get('num_tries', 5)
    log_block_size = cfg.get('log_block_size', '10M')
    
    data_url, md5_url = build_product_url(hub, product_id)
        
    r_md5 = get_url(md5_url, username, password, tries=tries)
    true_md5 = r_md5.text.lower()
    block_size = _convert_block_size(block_size)
    log_block_size = _convert_block_size(log_block_size)
    
    n = 0
    last_log = 0
    while n < tries:
        r_data = get_url(data_url, username, password, tries=tries, stream=True)
        wobj = open(output_name, 'wb')

        try:
            bytes_downloaded = 0
            for chunk in r_data.iter_content(chunk_size=block_size):
                wobj.write(chunk)
                
                bytes_downloaded += len(chunk)
                if bytes_downloaded - last_log > log_block_size:
                    logger.info('%s downloaded for %s', _pretty_bytes(bytes_downloaded), output_name)
                    last_log = bytes_downloaded
        except:
            logger.info('FAILURE:  %s downloaded for %s',  _pretty_bytes(bytes_downloaded), output_name)
        else:
            logger.info('COMPLETE: %s downloaded for %s', _pretty_bytes(bytes_downloaded), output_name)
        finally:
            wobj.close()
            n += 1
            
            
        curr_md5 = _compute_hash(output_name)
        if curr_md5 == true_md5:
            logger.info('%s MD5 is correct', output_name)
            return True, true_md5
        elif checksum_action == 'record':
            logger.warning('%s MD5 is incorrect, recording', output_name)
            return False, true_md5
        else:
            logger.warning('%s MD5 is incorrect, will retry', output_name)
            continue
            
    # If get here, exited loop due to number of tries so the result is a failed download
    return False, true_md5


def _record_failed_download(h, output_name, product_id, true_md5):
    h.write('{}  {}  {}\n'.format(os.path.basename(output_name), product_id, true_md5))


def single_download_driver(product_id, output_name, cfg):
    result, true_md5 = download_product_file(product_id=product_id, output_name=output_name, cfg=cfg)
    if not result:
        with open(cfg['record_file'], 'w') as wobj:
            _record_failed_download(wobj, output_name, product_id, true_md5)


def multi_download_driver(start_date, end_date, cfg):
    query_args = {k: cfg[k] for k in ('hub', 'product', 'platform', 'mode')}
    curr_date = start_date

    with open(cfg['record_file'], 'w') as recobj:
        while curr_date <= end_date:
            products_list = build_product_list_for_date(curr_date, query_args=query_args, 
                                                        username=cfg['username'], password=cfg['password'],
                                                        tries=cfg['num_tries'])
            for file_info in products_list:
                outname = os.path.join(cfg['output_dir'], file_info['file'])
                result, true_md5 = download_product_file(product_id=file_info['id'], output_name=outname, cfg=cfg)
                if not result:
                    _record_failed_download(recobj, outname, file_info['id'], true_md5)
                
            curr_date += dt.timedelta(days=1)


def check_md5_dates_driver(start_date, end_date, cfg):
    query_args = {k: cfg[k] for k in ('hub', 'product', 'platform', 'mode')}
    hub = query_args['hub']
    curr_date = start_date

    with open(cfg['record_file'], 'w') as recobj:
        while curr_date <= end_date:
            products_list = build_product_list_for_date(curr_date, query_args=query_args,
                                                        username=cfg['username'], password=cfg['password'],
                                                        tries=cfg['num_tries'])

            for file_info in products_list:
                outname = os.path.join(cfg['output_dir'], file_info['file'])
                    
                data_url, md5_url = build_product_url(hub, file_info['id'])
                r_md5 = get_url(md5_url, cfg['username'], cfg['password'], tries=cfg['num_tries'])
                true_md5 = r_md5.text.lower()
                if not os.path.exists(outname) or true_md5 != _compute_hash(outname):
                    _record_failed_download(recobj, outname, file_info['id'], true_md5)

            curr_date += dt.timedelta(days=1)


def failed_redownload_driver(failed_list_file, cfg):
    failed_list = build_failed_list(failed_list_file, cfg)
    if failed_list_file == cfg['record_file']:
        logger.warning('Backing up list of failed files (%s) as it will be overwritten with new downloads', failed_list_file)
        new_name = '{}.bak.{}'.format(failed_list_file, dt.datetime.now().strftime('%Y%m%dT%H%M%S'))
        shutil.copy2(failed_list_file, new_name)
    
    with open(cfg['record_file'], 'w') as recobj:
        for file_info in failed_list:
            outname = os.path.join(cfg['output_dir'], file_info['file'])
            result, true_md5 = download_product_file(product_id=file_info['id'], output_name=outname, cfg=cfg)
            if not result:
                _record_failed_download(recobj, outname, file_info['id'], true_md5)



def create_demo_config(config_file):
    if config_file == 'help':
        print_config_help()
        return
    else:
        with open(config_file, 'w') as wobj:
            wobj.write(sample_cfg)


def print_config_help():
    print('A config file looks like this:\n')
    for line in sample_cfg.splitlines():
        print('  {}'.format(line))
    print("""
The lines in brackets, e.g. "[NO2]" denote sections and the other lines
are key-value pairs. All sections, except default, allow you to specify
settings for downloading a particular data set. When you use the batch 
downloader, you will pass the section name as one of the arguments, and
the configuration values from that section will be used. The DEFAULT 
section contains default values that will be used if that key is not present
in the main section chosen. In the example, only "product" is defined
in the NO2 section, so all the other options will be taken from DEFAULT.

Here are the possible keys for each section, along with the fallback
value that will be used if they are not specified anywhere in the config:
""")
    for key, (default, _, helptext) in cfg_settings.items():
        if default is None:
            defstr = 'required'
        elif isinstance(default, str) and len(default) == 0:
            defstr = 'required for batch'
        else:
            defstr = 'default: {}'.format(default)
        print('  {} ({}) - {}'.format(key, defstr, helptext))


def _datetype(v):
    if len(v) == 8:
        return dt.datetime.strptime(v, '%Y%m%d')
    elif len(v) == 10:
        return dt.datetime.strptime(v, '%Y-%m-%d')
    else:
        raise ValueError('Bad format for date string')


def parse_demo_config_args(p):
    p.description = 'Create a demo config file'
    p.add_argument('config_file', help='Path to create the new config file at')
    p.set_defaults(driver=create_demo_config, read_config=False)


def parse_dlone_args(p):
    p.description = 'Download a single TROPOMI file'
    p.add_argument('config_file', help='Path to an INI-style config file that has the common download options')
    p.add_argument('product_id', help='The hex ID string that points to the file you want to download')
    p.add_argument('output_name', help='The name to give the downloaded file')
    p.set_defaults(driver=single_download_driver, section='DEFAULT')


def parse_dlbatch_args(p):
    p.description = 'Download TROPOMI files for a date range'
    p.add_argument('config_file', help='Path to an INI-style config file that has the common download options')
    p.add_argument('section', help='Section of the config with the settings to use to download')
    p.add_argument('start_date', type=_datetype, help='First date to download for, in YYYYMMDD or YYYY-MM-DD format')
    p.add_argument('end_date', type=_datetype, help='Last date to download for, in YYYYMMDD or YYYY-MM-DD format')
    p.set_defaults(driver=multi_download_driver)


def parse_checkbatch_args(p):
    p.description = 'Verify TROPOMI files MD5 checksums for a date range'
    p.add_argument('config_file', help='Path to an INI-style config file that has the common download options')
    p.add_argument('section', help='Section of the config with the settings to use to download')
    p.add_argument('start_date', type=_datetype, help='First date to download for, in YYYYMMDD or YYYY-MM-DD format')
    p.add_argument('end_date', type=_datetype, help='Last date to download for, in YYYYMMDD or YYYY-MM-DD format')
    p.set_defaults(driver=check_md5_dates_driver)


def parse_dlfailed_args(p):
    p.description = 'Redownload TROPOMI files whose checksums did not match'
    p.add_argument('config_file', help='Path to an INI-style config file that has the common download options')
    p.add_argument('section', help='Section of the config with the settings to use to download')
    p.add_argument('failed_list_file', help='Path to a "failed_downloads" file that has the file name, file ID, and MD5 sum of failed files')
    p.set_defaults(driver=failed_redownload_driver)


def parse_top_args():
    p = ArgumentParser(description='Download TROPOMI files')
    p.add_argument('-v', '--verbose', action='store_const', const=2, default=1,
                   help='Set console logger to maximum')
    p.add_argument('-q', '--quiet', action='store_const', const=0, dest='verbose',
                   help='Set console logger to minimum')
    p.add_argument('--pdb', action='store_true', help='Launch python debugger')
    p.set_defaults(read_config=True)
    subp = p.add_subparsers()
    
    p_dlone = subp.add_parser('dlone')
    parse_dlone_args(p_dlone)

    p_dlbatch = subp.add_parser('dlbatch')
    parse_dlbatch_args(p_dlbatch)

    p_dlfailed = subp.add_parser('dlfailed')
    parse_dlfailed_args(p_dlfailed)

    p_dlcheck = subp.add_parser('check-by-dates', aliases=['cbd'])
    parse_checkbatch_args(p_dlcheck)

    p_demo = subp.add_parser('make-cfg')
    parse_demo_config_args(p_demo)

    return vars(p.parse_args())


cfg_settings = {'hub': (None, str, 'URL to the data hub.'),
                'username': (None, str, 'Username to access the data hub.'),
                'password': (None, str, 'Password to access the data hub.'),
                'product': ('', str, 'Which data product to download. Required if downloading in batch.'),
                'platform': ('Sentinel-5', str, 'Which satellite to download data for.'),
                'mode': ('Offline', str, 'Which processing mode of the satellite data to download.'),
                'block_size': ('1M', str, 'How much data to stream from the data hub at once. A number, optionally followed by K (kilobytes), M (megabytes), or G (gigabytes).'),
                'log_block_size': ('25M', str, 'How frequently to report download progress. A number, optionally followed by K (kilobytes), M (megabytes), or G (gigabytes).'),
                'on_bad_checksum': ('record', str, 'What to do if a data file has a bad checksum: "record" (just write it to a list) or "retry".'),
                'num_tries': (5, int, 'How many times to try retrieving information from the data hub.'),
                'record_file': ('failed_downloaded.txt', str, 'File to write reports of failed downloads to.'),
                'output_dir': ('.', str, 'Directory to save batch downloaded files to')}

def read_config_file(filename, section):
    cfg = configparser.ConfigParser()
    cfg.read(filename)

    # Make sure you have what you need, or set defaults/convert types if possible

    final_cfg = dict()
    csect = cfg[section]
    for key, (default, converter, _) in cfg_settings.items():
        if key in csect:
            final_cfg[key] = converter(csect[key])
        elif default is not None:
            final_cfg[key] = default
        else:
            raise ConfigError('Required key "{}" not present in section "{}" or the DEFAULT section'.format(key, section))

    return final_cfg


def main():
    cl_args = parse_top_args()

    verb = cl_args.pop('verbose')
    logging.basicConfig(format='[%(levelname)s] %(message)s')
    levels = [logging.WARN, logging.INFO, logging.DEBUG]
    logger.setLevel(levels[verb])

    driver_fxn = cl_args.pop('driver')
    if cl_args.pop('pdb'):
        import pdb 
        pdb.set_trace()
    
    if cl_args.pop('read_config'):
        config_file = cl_args.pop('config_file')
        section = cl_args.pop('section')
        cfg = read_config_file(config_file, section)

        logger.debug('Configuration read:')
        for k, v in cfg.items():
            logger.debug('  {} = {}'.format(k, v))

        return driver_fxn(cfg=cfg, **cl_args)
    else:
        return driver_fxn(**cl_args)
    

if __name__ == '__main__':
    main()


