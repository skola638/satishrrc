#---- DB definitions for RDS Postgre Database connections
import sys
import psycopg2, psycopg2.extras
import os
from common import log as logger

## Debug/Logging
g_env_disable_logging = os.environ['LOGGING_DISABLE']
g_env_logging_level   = os.environ['LOGGING_LEVEL']

# Configure the logger to write to stdout (config 1)
g_log = logger.init_config_1(g_env_logging_level)   

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
