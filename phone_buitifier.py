from datetime import datetime
import time

import database

# Set up logging
import logging.config

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def spent_time():
    global start_time
    sec_all = time.time() - start_time
    if sec_all > 60:
        minutes = sec_all // 60
        sec = sec_all % 60
        time_str = f'| {int(minutes)} min {round(sec, 1)} sec'
    else:
        time_str = f'| {round(sec_all, 1)} sec'
    start_time = time.time()
    return time_str


def phone_to_only_numbers(phone):
    logger.debug(f'phone: {phone}')
    phone_only_num = ''.join(char for char in phone if char.isdecimal())
    phone_only_num_len = len(phone_only_num)
    logger.debug(f'phone length: {phone_only_num_len}')
    if phone_only_num_len != 12:
        logger.debug('*******************************************************')
    phone_out = (phone_only_num)
    logger.debug(f'phone_out: {phone_out}')
    return phone_out


def phone_remover_list(phones):
    phones_out = []
    # print(phones)
    for phone in phones:
        logger.debug(f'phone: {phone}')
        phone_only_num = ''.join(
            char for char in phone if char.isdecimal())
        phone_only_num_len = len(phone_only_num)
        logger.debug(f'phone length: {phone_only_num_len}')
        if phone_only_num_len != 12:
            logger.debug(
                '**************************************************************')
        phone_out = (phone_only_num)
        logger.debug(f'phone_out: {phone_out}')
        phones_out.append(phone_out)
    return phones_out


def phone_beautify():
    time_begin = start_time = time.time()
    ###############################################################################

    customers = database.get_telegram_phones()
    # logger.debug(f'customers: {customers}')

    customers_out = []
    customers_bad_phones = []
    for customer in customers:
        logger.debug('*******************************************************')
        logger.debug(f'customer: {customer}')
        phone_only_num = phone_to_only_numbers(customer[1])
        phone_only_num_len = len(phone_only_num)
        if phone_only_num_len == 12:
            customer_out = (customer[0], customer[1], phone_only_num)
            logger.debug(f'customer_out: {customer_out}')
            customers_out.append(customer_out)
        elif phone_only_num_len < 12:
            customers_bad_phones.append(
                (customer[0], customer[1], phone_only_num, 'BAD. Few numbers'))
        elif phone_only_num_len > 12:
            customers_bad_phones.append((customer[0], customer[1],
                                         phone_only_num,
                                         'BAD. Lots of numbers'))
    logger.debug(
        f'customers: {len(customers)} | customers_out: {len(customers_out)} | customers_bad_phones: {len(customers_bad_phones)} | customers_out: \n{customers_out}')
    logger.debug(
        f'customers_bad_phones: {len(customers_bad_phones)} | {customers_bad_phones}')

    # Write correct only numbers phones to DB
    for customer in customers_out:
        database.put_telephone_num(customer[0], customer[2])

    # Write message for bad phones to DB
    for customer in customers_bad_phones:
        database.put_telephone_num(customer[0], customer[3])

    ###############################################################################
    time_end = time.time()
    elapsed_time = time_end - time_begin
    if elapsed_time > 60:
        elapsed_minutes = elapsed_time // 60
        elapsed_sec = elapsed_time % 60
        elapsed_time_str = f'| {int(elapsed_minutes)} min {round(elapsed_sec, 1)} sec'
    else:
        elapsed_time_str = f'| {round(elapsed_time, 1)} sec'
    message = f'Elapsed run time: {elapsed_time_str} seconds'
    logger.info(message)


if __name__ == '__main__':
    phone_beautify()
