#!/usr/bin/python
import sys
import logging

def setup_logging(filename):
    save_dir = r'/dcfs-share/task-logs/'
    logging.basicConfig(level=logging.DEBUG,
                    # format='%(asctime)s - %(levelname)s - %(message)s',
                    format='%(asctime)s - [%(levelname)s]\n%(message)s\n[%(pathname)s %(funcName)s %(lineno)d]\n',
                    filename=save_dir+filename,
                    filemode='w')
    # Until here logs only to file: 'logs_file'

    # define a new Handler to log to console as well
    console = logging.StreamHandler()
    # optional, set the logging level
    console.setLevel(logging.DEBUG)
    # set a format which is the same for console use
    # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s]\n%(message)s\n[%(pathname)s %(funcName)s %(lineno)d]\n')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger().addHandler(console)

    # Set all other logger levels to WARNING
    for logger_name in logging.root.manager.loggerDict:
        if logger_name != __name__:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)

    # for logger in loggers:

setup_logging('test' + '.log')

# sys.argv[0]?/dcfs-share/dcfs-run/logTest.py
# sys.argv[1]?/tmp/hadoop-root/nm-local-dir/usercache/root/appcache/application_1665929621102_0006/container_1665929621102_0006_01_000002/dcfs_temp/52502_test.txt
for i in range(len(sys.argv)):
    logging.info(f"sys.argv[{i}]?" + sys.argv[i])