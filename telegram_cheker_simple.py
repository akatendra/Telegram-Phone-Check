from telethon import TelegramClient, errors, sync
from telethon.tl.types import InputPhoneContact
from telethon import functions
from dotenv import load_dotenv
import os
from getpass import getpass
import time

import database

# Set up logging
import logging.config

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
logging.getLogger('telethon').setLevel(logging.WARNING)

load_dotenv()

result = {}

API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE_NUMBER = os.getenv('PHONE_NUMBER')


def get_names(phone_number):
    try:
        contact = InputPhoneContact(client_id=0, phone=phone_number,
                                    first_name="", last_name="")
        contacts = client(functions.contacts.ImportContactsRequest([contact]))
        logger.debug(f'contacts: {contacts}')
        # username = contacts.to_dict()['users'][0]['username'] # not working

        users_dic = contacts.__dict__['users']
        logger.debug(f'users_dic[0]: {users_dic[0]}')
        if not (len(users_dic) > 0):
            logger.debug(
                "*" * 5 + f' Response detected, but no chat_id returned by the API for the number: {phone_number} ' + "*" * 5)
            client(functions.contacts.DeleteContactsRequest(
                id=[users_dic[0]]))  # delete user contact
            return
        else:
            chat_id = users_dic[0].__dict__['id']
            username = users_dic[0].__dict__['username']
            first_name = users_dic[0].__dict__['first_name']
            last_name = users_dic[0].__dict__['last_name']
            client(functions.contacts.DeleteContactsRequest(
                id=[users_dic[0]]))  # delete user contact
            return chat_id, username, first_name, last_name
    except IndexError as e:
        return logger.debug(
            f'ERROR: there was no response for the phone number: {phone_number}')
    except TypeError as e:
        return logger.debug(
            f'TypeError: {e}. --> The error might have occured due to the inability to delete the {phone_number} from the contact list.')
    except errors.rpcerrorlist.FloodWaitError as error:
        sleep_time = error.seconds
        logger.debug(f'FloodWaitError: {error.message} | seconds: {sleep_time}')
        logger.debug(f'Sleeping for {sleep_time} seconds!!!')
        for i in range(1, sleep_time + 1):
            time.sleep(1)
            logger.debug(f'Sleep for {sleep_time} | Spent second {i}')
        return
    except:
        raise


def user_validator():
    '''
    The function uses the get_api_response function to first check if the user exists and if it does, then it returns the first user name and the last user name.
    '''
    input_phones = input("Phone numbers: ")
    phones = input_phones.split()
    try:
        for phone in phones:
            api_res = get_names(phone)
            result[phone] = api_res
    except:
        raise


if __name__ == '__main__':
    client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)
    client.connect()
    if not client.is_user_authorized():
        client.send_code_request(PHONE_NUMBER)
        try:
            client.sign_in(PHONE_NUMBER,
                           input('Enter the code (sent on telegram): '))
        except errors.SessionPasswordNeededError:
            pw = getpass(
                'Two-Step Verification enabled. Please enter your account password: ')
            client.sign_in(password=pw)
    ###########################################################################



    user_validator()
    print(result)
