import os
import pandas as pd
import time
import boto3

#from common import log as logger
from datetime import datetime, timedelta

#*******************************************************************************
#* Globals
#*******************************************************************************

### Get ENV variables

## Input file
g_env_input_file = os.environ['INPUT_FILE']

## Debug/Logging
#g_env_disable_logging = os.environ['LOGGING_DISABLE']
#g_env_logging_level   = os.environ['LOGGING_LEVEL']

g_debug_ind = True #if g_env_logging_level.upper() == 'DEBUG' else False

# Configure the logger to write to stdout (config 1)
#g_log = logger.init_config_1(g_env_logging_level)

## Postgres ENV variables
# db_host = os.environ['DB_HOST']
# db_dbname = os.environ['DB_NAME']
# db_port = os.environ['DB_PORT']
# db_user = os.environ['DB_USER']
# db_password = os.environ['DB_PWD']
# db_connect_timeout = os.environ['DB_CONNECT_TIMEOUT']



### Set proxies ENV variables

g_proxy_env = os.environ['PROXY_ENV']
set_proxy = 'prod' if g_proxy_env.upper() == 'PROD' else 'nonprod'

if set_proxy == 'prod' :
   ## PROD (stg, ple and prod)
   os.environ['NO_PROXY']    = '169.254.169.254'
   os.environ['http_proxy']  = 'http://vzproxy.verizon.com:80'
   os.environ['https_proxy'] = 'http://vzproxy.verizon.com:80'
   os.environ['HTTP_PROXY']  = 'http://vzproxy.verizon.com:80'
   os.environ['HTTPS_PROXY'] = 'http://vzproxy.verizon.com:80'
else:
    pass
   ## Nonprod
   #os.environ['https_proxy'] = 'http://proxy.ebiz.verizon.com:80'
   #os.environ['http_proxy']  = 'http://proxy.ebiz.verizon.com:80'
   #os.environ['NO_PROXY']    = '169.254.169.254'


## Pandas column definitions

## List of outputgcanonical for column names for a transformed CDR
g_col_list = ['rcd_type', 'id', 'company_id', 'mdn', 'esn', 'dialed_digits', 'term_min', 'switch_id', 'switch_type', 'cell_site', \
   'home_ind', 'orig_sid', 'orig_cc', 'term_cc', 'call_dirtion', 'carrier_code', 'answer_time', 'release_time', 'call_start_utc', \
   'call_end_utc', 'call_dur_mou', 'call_fwd_ind', 'three_way_call_ind', 'call_wait_ind', 'utc_offset_db', 'imsi', 'imei', \
   'enode_id', 'msc_id', 'billing_system_id' ]


g_col_dict = {
   'rcd_type':str, 'id':str, 'company_id':str, 'mdn':str, 'esn':str, 'dialed_digits':str, 'term_min':str, 'switch_id':str, \
   'switch_type':str, 'cell_site':str, 'home_ind':str, 'orig_sid':str, 'orig_cc':str, 'term_cc':str, 'call_dirtion':str, \
   'carrier_code':str, 'answer_time':str, 'release_time':str, 'call_start_utc':str, 'call_end_utc':str, 'call_dur_mou':str, \
   'call_fwd_ind':str, 'three_way_call_ind':str, 'call_wait_ind':str, 'utc_offset_db':str, 'imsi':str, 'imei':str, \
   'enode_id':str, 'msc_id':str, 'billing_system_id':str \
}

vdu_columns = ['mdn', 'home_ind', 'orig_cc',
                     'term_cc', 'origin_domesitc_indicator', 'term_domesitc_indicator',
                     'call_start_utc', 'counts', 'mou', 'now', 'now', 'billing_system_id']
group_columns = ['mdn', 'home_ind', 'orig_cc', 'term_cc', 'call_start_utc', 'billing_system_id']
## Handler - Called from main

def make_vdu_data_set(data_frame):
    import pdb;pdb.set_trace()
    data_frame = data_frame.groupby(group_columns).size().reset_index(name='counts')
    vdu_d_frames = []
    for index, row in enumerate(data_frame.values):
        mdn, home_ind, orig_cc, term_cc, call_start_utc, billing_system_id, counts = row
        origin_domesitc_indicator = 1
        if orig_cc.lower() == "usa":
            origin_domesitc_indicator = 0

        term_domesitc_indicator = 1
        if term_cc.lower() == "usa":
            term_domesitc_indicator = 0
        now = str(datetime.now())
        data_row = [mdn, home_ind, orig_cc,
                     term_cc, origin_domesitc_indicator, term_domesitc_indicator,
                     call_start_utc, counts, '', now, now, billing_system_id]

        vdu_df = pd.DataFrame([data_row], columns=vdu_columns)
        vdu_d_frames.append(vdu_df)
    vdu_d_frames = pd.concat(vdu_d_frames)
    print vdu_d_frames


def load_retail(event, context):
    #g_log.debug('Starting load_retail handler by a call from main()')


    #g_log.debug('Reading messages from input file: {}'.format(g_env_input_file))

    ## Open the input file of cdrs from an SQS msg
    try:
      fptr=open(g_env_input_file,"r")
    except IOError as e:
      msg='Exiting. I/O error({0}): {1} {2} file not found'.format(e.errno, e.strerror, switch_input)
      log.error(msg)
      print(msg)
      sys.exit()

    ## Read the CDRs from the file
    cdrs = fptr.readlines()

    ## create a list to hold the CDRs
    cdr_list =[]

    ## add each CDR to the list
    for cdr in cdrs:
     
      if len(cdr) != 0:   
         #g_log.debug('cdr:{}'.format(cdr))
         cdr_list.append(cdr)


    #df = pd.DataFrame([cdr_in_list.split("|") for cdr_in_list in cdr_list]])
    df = pd.DataFrame([cdr_in_list.split("|")[:-1] for cdr_in_list in cdr_list], columns=g_col_list)
    make_vdu_data_set(df)


    #g_log.debug('Completed load_retail handler')





def main() :
    load_retail('event', 'context')

if __name__ == '__main__':
    rv = main()
