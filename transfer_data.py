from datetime import datetime

import database

# Set up logging
import logging.config

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def transfer_data():
    customers_info_dict = database.get_customers_info()
    customers_ids_set = set(customers_info_dict.keys())
    logger.debug(f'Items_ids set received: {len(customers_ids_set)}')
    customers_telegram_ids = database.get_telegram_customers_ids()
    logger.debug(
        f'customers_telegram_ids received: {len(customers_telegram_ids)}')
    new_customers_ids = customers_ids_set.difference(customers_telegram_ids)

    # Transfer data from Main DB to Telegram DB
    customers = []
    for customer_id in new_customers_ids:
        customer = customers_info_dict[customer_id]
        customer_dict = {'customer_id': customer[0],
                         'firstname': customer[1],
                         'lastname': customer[2],
                         'email': customer[3],
                         'email_check': customer[4],
                         'telephone': customer[5],
                         'telephone_beautified': None,
                         'telephone_num': None,
                         'telegram': None,
                         'telegram_chat_id': None,
                         'telegram_username': None,
                         'telegram_first_name': None,
                         'telegram_last_name': None,
                         'city': customer[6],
                         'zone': customer[7],
                         'date_added': customer[8],
                         'date_modified': datetime.now()
                         }
        logger.debug(f'customer_dict: {customer_dict}')
        customers.append(customer_dict)
    database.write_to_customers_table(customers)


if __name__ == '__main__':
    transfer_data()
