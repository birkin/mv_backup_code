# -*- coding: utf-8 -*-

from __future__ import unicode_literals

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
# export_filename_segment = os.environ['MV_BCKP__MONGO_EXPORT_FILENAME_SEGMENT']
# backup_filename_segment = os.environ['MV_BCKP__MONGO_BACKUP_FILENAME_SEGMENT']
container_output_dir = os.environ['MV_BCKP__MONGO_CONTAINER_OUT_DIR']
server_output_dir = os.environ['MV_BCKP__SERVER_CP_DESTINATION_DIR']


## get to work!
today_str = str( datetime.date.today() )


def run_exports():
    """ Creates human-readable exports of the data.
        Each of the three collections is saved to a container-directory, and then copied to a server-directory.
        Called by docker-server cron-script every Friday. """

    ## create export command strings
    ## (note, the `docker exec -it ...` normally used interactively won't work with cron, thus the `t` is removed here.)

    collection_a_output_filename = '%s_mv_mongo_EXPORT_%s.json' % ( today_str, collection_a_name )
    collection_a_export_command_str = '''docker exec -i %s mongoexport --db=%s --collection=%s --out=%s/%s --jsonArray --pretty --assertExists''' % (
        container_name,  db_name, collection_a_name, container_output_dir, collection_a_output_filename )
    log.debug( 'collection_a_export_command_str, ```%s```' % collection_a_export_command_str )

    collection_b_output_filename = '%s_mv_mongo_EXPORT_%s.json' % ( today_str, collection_b_name )
    collection_b_export_command_str = '''docker exec -i %s mongoexport --db=%s --collection=%s --out=%s/%s --jsonArray --pretty --assertExists''' % (
        container_name,  db_name, collection_b_name, container_output_dir, collection_b_output_filename )
    log.debug( 'collection_b_export_command_str, ```%s```' % collection_b_export_command_str )

    collection_c_output_filename = '%s_mv_mongo_EXPORT_%s.json' % ( today_str, collection_c_name )
    collection_c_export_command_str = '''docker exec -i %s mongoexport --db=%s --collection=%s --out=%s/%s --jsonArray --pretty --assertExists''' % (
        container_name,  db_name, collection_c_name, container_output_dir, collection_c_output_filename )
    log.debug( 'collection_c_export_command_str, ```%s```' % collection_c_export_command_str )

    ## export to container output directory

    output_export_a = os.popen( collection_a_export_command_str ).read()
    log.debug( 'output_export_a, ```%s```' % pprint.pformat(output_export_a.replace("\t", " -- ").split("\n")) )
    time.sleep( .5 )

    output_export_b = os.popen( collection_b_export_command_str ).read()
    log.debug( 'output_export_b, ```%s```' % pprint.pformat(output_export_b.replace("\t", " -- ").split("\n")) )
    time.sleep( .5 )

    output_export_c = os.popen( collection_c_export_command_str ).read()
    log.debug( 'output_export_c, ```%s```' % pprint.pformat(output_export_c.replace("\t", " -- ").split("\n")) )
    time.sleep( .5 )

    ## create copy command strings

    source_a_filepath = '%s/%s' % (container_output_dir, collection_a_output_filename)
    destination_a_filepath = '%s/%s' % ( server_output_dir, collection_a_output_filename )
    collection_a_copy_command_str = '''docker cp %s:%s %s''' % (container_name, source_a_filepath, destination_a_filepath )
    log.debug( 'collection_a_copy_command_str, ```%s```' % collection_a_copy_command_str )

    source_b_filepath = '%s/%s' % (container_output_dir, collection_b_output_filename)
    destination_b_filepath = '%s/%s' % ( server_output_dir, collection_b_output_filename )
    collection_b_copy_command_str = '''docker cp %s:%s %s''' % (container_name, source_b_filepath, destination_b_filepath )
    log.debug( 'collection_b_copy_command_str, ```%s```' % collection_b_copy_command_str )

    source_c_filepath = '%s/%s' % (container_output_dir, collection_c_output_filename)
    destination_c_filepath = '%s/%s' % ( server_output_dir, collection_c_output_filename )
    collection_c_copy_command_str = '''docker cp %s:%s %s''' % (container_name, source_c_filepath, destination_c_filepath )
    log.debug( 'collection_c_copy_command_str, ```%s```' % collection_c_copy_command_str )

    ## copy to server output directory

    output_copy_a = os.popen( collection_a_copy_command_str ).read()
    log.debug( 'output_copy_a, ```%s```' % pprint.pformat(output_copy_a.replace("\t", " -- ").split("\n")) )
    time.sleep( .5 )

    output_copy_b = os.popen( collection_b_copy_command_str ).read()
    log.debug( 'output_copy_b, ```%s```' % pprint.pformat(output_copy_b.replace("\t", " -- ").split("\n")) )
    time.sleep( .5 )

    output_copy_c = os.popen( collection_c_copy_command_str ).read()
    log.debug( 'output_copy_c, ```%s```' % pprint.pformat(output_copy_c.replace("\t", " -- ").split("\n")) )
    time.sleep( .5 )

    return

    ## end of run_exports()


def run_backups():
    """ Creates bson backup of the data.
        Output is saved to a container-directory, and then copied to a server-directory.
        Called by docker-server cron-script every Friday. """

    ## create backup command
    output_filename = '%s_mv_mongo_backup' % today_str
    container_output_filepath = '%s/%s' % ( container_output_dir, output_filename )
    dump_command_str = 'docker exec -i %s mongodump --out %s' % (
        container_name, container_output_filepath )  # note, the `docker exec -it ...` normally used interactively won't work with cron, thus the `t` is removed here.
    log.debug( 'dump_command_str, ```%s```' % dump_command_str )

    ## run backup command
    output_dump = os.popen( dump_command_str ).read()
    log.debug( 'output_dump, ```%s```' % pprint.pformat(output_dump.replace('\t', ' -- ').split('\n')) )
    time.sleep( .5 )

    ## create copy command
    source_filepath = container_output_filepath
    destination_filepath = '%s/%s' % ( server_output_dir, output_filename )
    copy_command_str = '''docker cp %s:%s %s''' % (container_name, source_filepath, destination_filepath )
    log.debug( 'copy_command_str, ```%s```' % copy_command_str )

    ## run copy command
    output_copy = os.popen( copy_command_str ).read()
    log.debug( 'output_copy, ```%s```' % pprint.pformat(output_copy.replace('\t', ' -- ').split('\n')) )
    time.sleep( .5 )

    return


if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) == 2 else None
    log.debug( 'argument, `%s`' % arg )
    if arg == 'run_export':
        run_exports()
    elif arg == 'run_backup':
        run_backups()
    else:
        raise Exception( 'bad argument' )
