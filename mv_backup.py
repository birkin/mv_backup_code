"""
Manages mongodump backups, for possible restore -- and mongoexport exports, for human use.

(useful: <https://janakiev.com/blog/python-shell-commands/>)
"""

import datetime, logging, logging.config, os, pprint, subprocess, sys, time


## setup logging
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': { 'default': { 'format': '[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s', } },
    'handlers': {
        'logfile': {
            'level':'DEBUG',
            'class':'logging.FileHandler',  # note: configure server to use system's log-rotate to avoid permissions issues
            'filename': os.environ['MV_BCKP__LOG_PATH'],
            # 'class': 'logging.StreamHandler',
            'formatter': 'default' }, },
    'loggers': {
        'mv_backup_logger': { 'level': 'DEBUG', 'handlers': ['logfile'], 'propagate': False }  }
    })
log = logging.getLogger( 'mv_backup_logger' )
log.info( 'logging setup complete' )


## load envars
container_name = os.environ['MV_BCKP__MONGO_CONTAINER_NAME']
db_name = os.environ['MV_BCKP__MONGO_DB_NAME']
collection_a_name = os.environ['MV_BCKP__MONGO_COLLECTION_A_NAME']
collection_b_name = os.environ['MV_BCKP__MONGO_COLLECTION_B_NAME']
collection_c_name = os.environ['MV_BCKP__MONGO_COLLECTION_C_NAME']
output_dir = os.environ['MV_BCKP__MONGO_CONTAINER_OUT_DIR']
output_filename_segment = os.environ['MV_BCKP__MONGO_CONTAINER_OUT_FILENAME_SEGMENT']


## get to work!
today_str = str( datetime.date.today() )


def run_exports():
    """ Creates human-readable exports of the data.
        Called by docker-server cron-script """

    collection_a_output_filename = f'{today_str}_{output_filename_segment}_{collection_a_name}.json'
    collection_a_command_str = f'''docker exec -it {container_name} mongoexport --db={db_name} --collection={collection_a_name} --out={output_dir}/{collection_a_output_filename} --jsonArray --pretty --assertExists'''
    log.debug( f'collection_a_command_str, ```{collection_a_command_str}```' )

    collection_b_output_filename = f'{today_str}_{output_filename_segment}_{collection_b_name}.json'
    collection_b_command_str = f'''docker exec -it {container_name} mongoexport --db={db_name} --collection={collection_b_name} --out={output_dir}/{collection_b_output_filename} --jsonArray --pretty --assertExists'''
    log.debug( f'collection_b_command_str, ```{collection_b_command_str}```' )

    collection_c_output_filename = f'{today_str}_{output_filename_segment}_{collection_c_name}.json'
    collection_c_command_str = f'''docker exec -it {container_name} mongoexport --db={db_name} --collection={collection_c_name} --out={output_dir}/{collection_c_output_filename} --jsonArray --pretty --assertExists'''
    log.debug( f'collection_c_command_str, ```{collection_c_command_str}```' )

    output_export_a = os.popen( collection_a_command_str ).read()
    log.debug( 'output_export_a, ```%s```' % pprint.pformat(output_export_a.replace("\t", " -- ").split("\n")) )
    time.sleep( 1 )

    output_export_b = os.popen( collection_b_command_str ).read()
    log.debug( 'output_export_b, ```%s```' % pprint.pformat(output_export_b.replace("\t", " -- ").split("\n")) )
    time.sleep( 1 )

    output_export_c = os.popen( collection_c_command_str ).read()
    log.debug( 'output_export_c, ```%s```' % pprint.pformat(output_export_c.replace("\t", " -- ").split("\n")) )
    time.sleep( 1 )

    return



if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) == 2 else None
    log.debug( f'argument, `{arg}`' )
    if arg == 'run_exports':
        run_exports()
    elif arg == 'run_backups':
        run_backups()
    else:
        raise Exception( 'bad argument' )
