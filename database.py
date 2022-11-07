import mysql.connector
from mysql.connector import Error

# https://github.com/PyMySQL/PyMySQL
import pymysql

from datetime import datetime
import config_db

from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric, Text, BigInteger, Boolean, not_
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

# Set up logging
import logging.config

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


###############################################################################
################################### MySQL #####################################
###############################################################################

def get_connection():
    try:
        with mysql.connector.connect(**config_db.DB) as connection:
            print(connection)
    except Error as e:
        print(e)
    return connection


def create_db(db_name):
    sql_create_db = f'''
                        CREATE DATABASE {db_name} 
                        DEFAULT CHARACTER SET utf8mb4  
                        DEFAULT COLLATE utf8mb4_general_ci ;
                     '''
    customers = execute_sql_query(sql_create_db, fetch=False)
    logger.debug(f'DB {db_name} created!')
    return True


def execute_sql_query(sql, fetch=True, data=None):
    try:
        # Connection to MySQL DB

        with mysql.connector.connect(**config_db.DB) as connection:
            # Курсор для выполнения операций с базой данных
            with connection.cursor() as cursor:
                if cursor:
                    logger.info('MySQL connected!')
                if data is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, data)
                if fetch is True:
                    return cursor.fetchall()
    except (Exception, Error) as error:
        logger.debug("Error during work with MySQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            logger.info("Connection with MySQL closed!")


def get_customers_ids():
    sql_get_customers_ids = f'''
                        SELECT customer_id
                        FROM   oc_customer;
                        '''
    customers_ids = execute_sql_query(sql_get_customers_ids)
    customers_ids_set = set((customer[0] for customer in customers_ids))
    logger.debug(f'Items_ids set received: {len(customers_ids_set)}')
    return customers_ids_set


def get_customers_phones():
    sql_get_customers = f'''
                            SELECT customer_id, telephone 
                            FROM oc_customer 
                            ORDER BY customer_id;
                         '''
    customers = execute_sql_query(sql_get_customers)
    # customers_tuple = set((item[0] for item in customers))
    logger.debug(f'Customers received: {len(customers)}')
    return customers


def get_customers_info():
    sql_get_customers_info = f'''
                            SELECT oc_customer.customer_id,
                                   oc_customer.firstname,
                                   oc_customer.lastname,
                                   oc_customer.email,
                                   oc_customer.email_check,
                                   oc_customer.telephone,
                                   oc_address.city,
                                   oc_zone.NAME,
                                   oc_customer.date_added
                            FROM   oc_customer
                                   LEFT JOIN oc_address
                                          ON oc_address.customer_id = oc_customer.customer_id
                                   LEFT JOIN oc_zone
                                          ON oc_address.zone_id = oc_zone.zone_id
                            ORDER  BY oc_customer.customer_id;
                         '''
    customers_info = execute_sql_query(sql_get_customers_info)
    customers_info_dict = {customer[0]: customer for customer in
                           customers_info}
    logger.debug(f'Customers info received: {len(customers_info_dict)}')
    return customers_info_dict


def get_orders_phones():
    sql_get_customers = f'''
                            SELECT order_id, customer_id, telephone
                            FROM oc_order
                            ORDER BY order_id;
                         '''
    customers = execute_sql_query(sql_get_customers)
    customers_tuple = set((item[0] for item in customers))
    logger.debug(f'Customers tuple received: {len(customers_tuple)}')
    return customers_tuple


###############################################################################
################################# SQLALCHEMY ##################################
###############################################################################

# Create SQLAlchemy engine for MySQL DB
def make_engine():
    # db_string = 'mysql:///?User=myUser&Password=myPassword&Database=NorthWind&Server=myServer&Port=3306'
    # db_string = 'postgresql+psycopg2://root:pass@localhost/mydb'
    # db_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    # dialect = 'postgresql'
    # driver = 'psycopg2'
    # db_uri = f'{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}'
    db_uri = URL.create(**config_db.DB_TELEGRAM)
    # logger.debug(f'db_uri: {db_uri}')
    # engine = create_engine(URL(**DATABASE), echo=True)
    # logger.debug(f'engine: {engine}')
    engine = create_engine(db_uri)
    return engine


def get_telegram_customers_ids():
    session = Session(bind=engine)
    customers_ids = session.query(Customers.customer_id).all()
    customers_ids_set = set((customer_id[0] for customer_id in customers_ids))
    logger.debug(
        f'customers_ids_set received from DB: {len(customers_ids_set)}')
    session.close()
    return customers_ids_set


def get_telegram_phones():
    session = Session(bind=engine)
    customers = session.query(Customers.customer_id, Customers.telephone).all()
    logger.debug(
        f'customers received from DB: {len(customers)}')
    session.close()
    return customers


def get_customers_to_check():
    session = Session(bind=engine)
    '''customers = session.query(Customers.customer_id,
                              Customers.telephone_num).filter(
        not_(Customers.telephone_num.like('BAD%')),
        Customers.telegram == None).all()'''
    customers = session.query(Customers.customer_id,
                              Customers.telephone_num).filter(
        not_(Customers.telephone_num.like('BAD%')),
        Customers.telegram == None).limit(1000).all()
    logger.debug(
        f'customers received from DB: {len(customers)}')
    session.close()
    return customers


def write_to_customers_table(customers):
    session = Session(bind=engine)
    logger.debug(f'session: {session}')
    # Connect to DB table
    customers_table = Table('Customers', metadata, autoload=True,
                            autoload_with=engine)
    ins = customers_table.insert()
    logger.debug(f'ins: {ins}')
    for customer in customers:
        logger.debug(f'customer in write_to_customers_table: {customer}')
        session.execute(ins, customer)
        logger.debug(f'Customer {customer} saved into DB')
        session.commit()
    session.close()
    logger.info('Data saved into table Customers!')


def telegram_on(customer_id):
    session = Session(bind=engine)
    updated_customer = session.query(Customers).filter_by(
        customer_id=customer_id).first()
    updated_customer.telegram = True
    logger.debug(f'Telegram customer_id: {customer_id} updated to ON!')
    session.commit()
    session.close()


def telegram_off(customer_id):
    session = Session(bind=engine)
    updated_customer = session.query(Customers).filter_by(
        customer_id=customer_id).first()
    updated_customer.telegram = False
    logger.debug(f'Telegram customer_id: {customer_id} updated to OFF!')
    session.commit()
    session.close()


def put_telephone_num(customer_id, telephone):
    session = Session(bind=engine)
    updated_customer = session.query(Customers).filter_by(
        customer_id=customer_id).first()
    updated_customer.telephone_num = telephone
    logger.debug(
        f'Telephone {telephone} of customer_id: {customer_id} updated in DB!')
    session.commit()
    session.close()


def put_beautified_telephone(customer_id, telephone):
    session = Session(bind=engine)
    updated_customer = session.query(Customers).filter_by(
        customer_id=customer_id).first()
    updated_customer.telephone_beautified = telephone
    logger.debug(
        f'Beatified telephone {telephone} of customer_id: {customer_id} updated in DB!')
    session.commit()
    session.close()


def put_telegram_user(customer_id, telephone, chat_id, username, first_name,
                      last_name):
    session = Session(bind=engine)
    updated_customer = session.query(Customers).filter_by(
        customer_id=customer_id).first()
    updated_customer.telegram = True
    updated_customer.telegram_chat_id = chat_id
    updated_customer.telegram_username = username
    updated_customer.telegram_first_name = first_name
    updated_customer.telegram_last_name = last_name
    logger.debug(
        f'Customer customer_id: {customer_id} | telephone: {telephone} | chat_id: {chat_id} | username: {username} | first_name: {first_name} | last_name: {last_name} updated in DB!')
    session.commit()
    session.close()


Base = declarative_base()
# Connect to DB
engine = make_engine()
metadata = MetaData(engine)


def drop_table(table_name):
    '''
    Don't working! Why?
    '''
    logger.debug(f'table_name: {table_name}')
    logger.debug(f'Hi from drop_table!')
    table = metadata.tables.get(table_name)
    logger.debug(f'table: {table}')
    if table is not None:
        logging.info(f'Deleting {table_name} table')
        Base.metadata.drop_all(engine, [table], checkfirst=True)


class Customers(Base):
    __tablename__ = 'customers'
    customer_id = Column(Integer, primary_key=True)
    firstname = Column(String(32), nullable=False)
    lastname = Column(String(32), nullable=False)
    email = Column(String(96), nullable=False)
    email_check = Column(String(32), nullable=True)
    telephone = Column(String(32), nullable=False)
    telephone_beautified = Column(String(32), nullable=True)
    telephone_num = Column(String(32), nullable=True)
    telegram = Column(Boolean, nullable=True)
    telegram_chat_id = Column(BigInteger, nullable=True)
    telegram_username = Column(String(32), nullable=True)
    telegram_first_name = Column(String(32), nullable=True)
    telegram_last_name = Column(String(32), nullable=True)
    city = Column(String(128), nullable=True)
    zone = Column(String(128), nullable=True)
    date_added = Column(DateTime(), nullable=False)
    date_modified = Column(DateTime(), default=datetime.now,
                           onupdate=datetime.now)


class Orders(Base):
    __tablename__ = 'orders'
    order_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, nullable=True)
    firstname = Column(String(32), nullable=False)
    lastname = Column(String(32), nullable=False)
    email = Column(String(96), nullable=False)
    email_check = Column(String(32), nullable=True)
    telephone = Column(String(32), nullable=False)
    telephone_beautified = Column(String(32), nullable=True)
    telephone_num = Column(String(32), nullable=True)
    telegram = Column(Boolean, nullable=True)
    telegram_chat_id = Column(BigInteger, nullable=True)
    telegram_username = Column(String(32), nullable=True)
    telegram_first_name = Column(String(32), nullable=True)
    telegram_last_name = Column(String(32), nullable=True)
    shipping_city = Column(String(128), nullable=True)
    shipping_zone = Column(String(128), nullable=True)
    date_added = Column(DateTime(), nullable=False)
    date_modified = Column(DateTime(), default=datetime.now,
                           onupdate=datetime.now)


def create_tables():
    drop_table('customers')
    drop_table('orders')
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    # get_connection()
    # get_customers_phones()
    # get_orders_phones()
    # create_db('natamoda_telegram')
    Base.metadata.create_all(engine)
    # telegram_on(1)
    # telegram_off(1)
    # get_telegram_phone_nums()
    # drop_table('customers')
    # drop_table('orders')
    # create_tables()
