import database
import transfer_data
import phone_buitifier

# Set up logging
import logging.config

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    # database.create_tables()
    transfer_data.transfer_data()
    phone_buitifier.phone_beautify()