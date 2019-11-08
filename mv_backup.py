"""
Manages mongodump backups, for possible restore -- and mongoexport exports, for human use.

(useful: <https://janakiev.com/blog/python-shell-commands/>)
"""

import datetime, logging, logging.config, os, pprint, subprocess, time


## setup logging
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': { 'default': { 'format': '[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s', } },
    'handlers': {
        'logfile': {
            'level':'DEBUG',
            # 'class':'logging.FileHandler',  # note: configure server to use system's log-rotate to avoid permissions issues
            # 'filename': os.environ['MV__LOGFILE_PATH'],
            'class': 'logging.StreamHandler',
            'formatter': 'default' }, },
    'loggers': {
        'foo_logger': { 'level': 'DEBUG', 'handlers': ['logfile'], 'propagate': False }  }
    })
log = logging.getLogger( 'foo_logger' )
log.info( 'logging setup complete' )


## load envars
container_name = os.environ['MV__MONGO_CONTAINER_NAME']
db_name = os.environ['MV__MONGO_DB_NAME']
collection_a_name = os.environ['MV__MONGO_COLLECTION_A_NAME']
collection_b_name = os.environ['MV__MONGO_COLLECTION_B_NAME']
collection_c_name = os.environ['MV__MONGO_COLLECTION_C_NAME']
output_dir = os.environ['MV__MONGO_CONTAINER_OUT_DIR']
output_filename_segment = os.environ['MV__MONGO_CONTAINER_OUT_FILENAME_SEGMENT']


## get to work!
today_str = str( datetime.date.today() )

collection_a_output_filename = f'{today_str}_{output_filename_segment}_{collection_a_name}.json'
# log.debug( f'collection_a_output_filename, ```{collection_a_output_filename}```' )
collection_a_command_str = f'''docker exec -it {container_name} mongoexport --db={db_name} --collection={collection_a_name} --out={output_dir}/{collection_a_output_filename} --jsonArray --pretty --assertExists'''
log.debug( f'collection_a_command_str, ```{collection_a_command_str}```' )

collection_b_output_filename = f'{today_str}_{output_filename_segment}_{collection_b_name}.json'
# log.debug( f'collection_b_output_filename, ```{collection_b_output_filename}```' )
collection_b_command_str = f'''docker exec -it {container_name} mongoexport --db={db_name} --collection={collection_b_name} --out={output_dir}/{collection_b_output_filename} --jsonArray --pretty --assertExists'''
log.debug( f'collection_b_command_str, ```{collection_b_command_str}```' )

collection_c_output_filename = f'{today_str}_{output_filename_segment}_{collection_c_name}.json'
# log.debug( f'collection_c_output_filename, ```{collection_c_output_filename}```' )
collection_c_command_str = f'''docker exec -it {container_name} mongoexport --db={db_name} --collection={collection_c_name} --out={output_dir}/{collection_c_output_filename} --jsonArray --pretty --assertExists'''
log.debug( f'collection_c_command_str, ```{collection_c_command_str}```' )

output_export_a = os.popen( collection_a_command_str ).read()
time.sleep( 1 )
log.debug( 'output_export_a, ```%s```' % pprint.pformat(output_export_a.replace("\t", " -- ").split("\n")) )


# output = process.stdout
# log.debug( f'output, ```{output}```' )
# log.debug( f'type(output), `{type(output)}`' )

# lines = output.split('\n')
# log.debug( f'lines, ```{pprint.pformat(lines)}```' )
# log.debug( f'type(lines), `{type(lines)}`' )


