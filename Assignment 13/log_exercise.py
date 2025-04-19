import logging

logging.basicConfig(level=logging.ERROR,
                    filename='app log',
                    filemode='w',
                    format='%(name)s - '
                            '%(levelname)s - '
                            '%(messages)s')

logging.debug('This is a debug statement')
logging.info('This will get logged to a file')
logging.warning('This is a Warning')
logging.error('This is an error!')