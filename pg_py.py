#---- DB definitions for RDS Postgre Database connections
import sys
import psycopg2, psycopg2.extras
import os
#from common import log as logger
import logging as g_log

## Debug/Logging
g_env_disable_logging = os.environ['LOGGING_DISABLE']
g_env_logging_level   = os.environ['LOGGING_LEVEL']

# Configure the logger to write to stdout (config 1)
#g_log = logging.init_config(g_env_logging_level)

#---Aws Postgres connection
#Postgres settings, set these up as the environment variables for your lambda function
#
db_connect_timeout =  os.environ['DB_CONNECT_TIMEOUT']

def pg_connect(db_param) :
    if db_param in ('NW', 'RE')  :  #-- use same env variables for network or reseller
        #--db connection for network_own
        db_host = os.environ['DB_HOST']
        db_dbname = os.environ['DB_NAME']
        db_port = os.environ['DB_PORT']
        db_user = os.environ['DB_USER']
        db_password = os.environ['DB_PWD']
        try:
            pg_conn = psycopg2.connect(host=db_host, dbname=db_dbname, port=db_port, \
                       user=db_user, password=db_password, connect_timeout=db_connect_timeout)
            pg_cur = pg_conn.cursor()
            g_log.debug('RDS {0} DB connection successful... '.format(db_dbname))
            #print('DB connection successful')
            return pg_conn, pg_cur
        except psycopg2.Error as e:
            g_log.debug('RDS {0} DB connection Error... '.format(db_dbname))
            g_log.debug('Code:{0} Sev:{1} Description:{2}'.format(e.pgcode, e.diag.severity, e.pgerror))
            #print('DB Connection Error:')
            print('Exiting...')
            sys.exit()
    elif db_param == 'RX' :
        #--db connection for reseller_own
        dbr_host = os.environ['DBR_HOST']
        dbr_dbname = os.environ['DBR_NAME']
        dbr_port = os.environ['DBR_PORT']
        dbr_user = os.environ['DBR_USER']
        dbr_password = os.environ['DBR_PWD']
        try:
            pg_conn = psycopg2.connect(host=dbr_host, dbname=dbr_dbname, port=dbr_port, \
                    user=dbr_user, password=dbr_password, connect_timeout=db_connect_timeout)
            pg_cur = pg_conn.cursor()
            g_log.debug('RDS {0} DB connection successful... '.format(dbr_dbname))
            #print('DB connection successful')
            return pg_conn, pg_cur
        except psycopg2.Error as e:
            g_log.debug('RDS {0} DB connection Error... '.format(db_dbname))
            g_log.debug('Code:{0} Sev:{1} Description:{2}'.format(e.pgcode, e.diag.severity, e.pgerror))
            #print('DB Connection Error:')
            print('Exiting...')
            sys.exit()
    
    return None
    #--end of pg_connect(db_param)

# -- load records to INTERNATIONAL_CDR table
def pg_execute_batchRetIntlNW(myBatch, sql_query):
    # g_ENFINW_conn, g_ENFINW_cur
    g_ENFINW_conn, g_ENFINW_cur = pg_connect('NW')
    g_db_nw_load_status = False
    try:
        psycopg2.extras.execute_batch(g_ENFINW_cur, sql_query, myBatch, page_size=5000)
        g_ENFINW_conn.commit()
        g_log.debug('SQS : Commit to {} successful...'.format(sql_query))
        g_db_nw_load_status = True

    except psycopg2.Error as e:
        g_ENFINW_conn.rollback()
        g_log.debug('NW DB : Load to {} failed ...'.format(sql_query))
        g_log.debug('NW DB : Code:{0} Description:{1}'.format(e.pgcode, e.pgerror))
        if '22P' in e.pgcode:  # -- delete the incorrectly sent messages
            g_db_nw_load_status = True
        else:
            g_db_nw_load_status = False
    return g_db_nw_load_status


def pg_execute_query_str(query_str):
    g_ENFINW_conn, g_ENFINW_cur = pg_connect('NW')
    g_db_nw_load_status = False
    try:
        g_ENFINW_cur.execute(query_str)
        g_ENFINW_conn.commit()
        g_log.debug('NW DB : Commit to INTERNATIONAL_CDR successful...')
        g_db_nw_load_status = True
    except psycopg2.Error as e:
        g_ENFINW_conn.rollback()
        g_log.debug('NW DB : Load to {} FAILED ...'.format(query_str))
        g_log.debug('NW DB : Code:{0} Description:{1}'.format(e.pgcode, e.pgerror))
        if '22P' in e.pgcode:
            g_db_nw_load_status = True  # -- delete the incorrectly sent messages
            # -- Code:23505, ERROR: duplicate key value violates unique constraint...
        elif '235' in e.pgcode:
            g_db_nw_load_status = True  # -- delete the incorrectly sent messages
            # -- Code:42601 Description:ERROR:  syntax error at or near ...
        else:
            g_db_nw_load_status = False  # --Don't delete, warn the Developer
            # g_db_nw_load_status=False  #-- unit test mode, to avoid having to re-populate SQS msg
    return g_db_nw_load_status

def create_international_cdr(data=[]):
    international_cdr_columns = ['international_cdr_id', 'mdn', 'esn_meid', 'dailed_digits',
                                 'dailed_digits_noprefix', 'terminating_msid', 'switch_id',
                                 'switch_type', 'cell_site', 'roaming_indicator', 'origin_sid',
                                 'origin_country_code', 'term_country_code', 'call_direction',
                                 'carrier_code', 'is_digital', 'begin_time', 'end_time',
                                 'begin_time_utc', 'end_time_utc', 'mou', 'call_forwarding',
                                 'three_way_calling', 'call_waiting', 'utc_offset', 'imsi',
                                 'imei', 'enb_id', 'mscid', 'ts_inserted', 'billing_system_id']

    sql_RetailIntl = """INSERT INTO network_own.international_cdr \
          (INTERNATIONAL_CDR_ID, MDN, ESN_MEID, DIALED_DIGITS, DIALED_DIGITS_NOPREFIX,\
           TERMINATING_MSID, SWITCH_ID, SWITCH_TYPE, CELL_SITE, ROAMING_INDICATOR, ORIGIN_SID,\
          ORIGIN_COUNTRY_CODE, TERM_COUNTRY_CODE, CALL_DIRECTION, CARRIER_CODE, IS_DIGITAL\
          BEGIN_TIME, END_TIME, BEGIN_TIME_UTC, END_TIME_UTC, MOU, \
          CALL_FORWARDING, THREE_WAY_CALLING, CALL_WAITING, UTC_OFFSET, IMSI, \
          IMEI, ENB_ID, MSCID, BILLING_SYSTEM_ID, TS_INSERTED) VALUES %s"""

    if not data or (not isinstance(data, list)):
        return "Invalid data"
    query_str = ""
    for row in data:
        sql_id = '{}'.format("nextval('international_cdr_id')")
        query_values = "'{}'".format("', '".join(['null' if i == None else str(i) for i in row]))
        query_str += "(" + sql_id + ", " + query_values + "), "
        print query_str

    print sql_RetailIntl % query_str
    #pg_execute_strListRetIntlNW(sql_RetailIntl % query_str)





data = [('2461719485', 'A012006C2A75C3', '011919105300715', None, 'rlgh2', 'N', '0E9A', '1', '0', 'USA', 'IND', '1', 'vzw', '2018-07-25 13:52:00', '2018-07-25 13:52:29', '2018-07-25 18:52:00', '2018-07-25 18:52:29', '1', 'N', 'N', 'N', -5, None, None, None, None, 'P', '2019-01-16 14:57:40.263631'),
        ('2461719486', 'A012006C2A75C3', '011919105300715', None, 'rlgh2', 'N', '0E9A', '1', '0', 'USA', 'IND', '1', 'vzw', '2018-07-26 13:52:00', '2018-07-26 13:52:29', '2018-07-26 18:52:00', '2018-07-26 18:52:29', '1', 'N', 'N', 'N', -6, None, None, None, None, 'P', '2019-01-17 14:57:40.263631')]
create_international_cdr(data)




