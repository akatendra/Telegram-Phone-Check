DB = {
    'user': 'user',
    'password': 'password',
    'host': 'host',
    'port': 'port',
    'database': 'database',
    'raise_on_warnings': True
}
DB_TELEGRAM = {
    # 'drivername': 'mysql+mysqldb', # Подключение к серверу MySQL с использованием mysql-python DBAPI.
    'drivername': 'mysql+pymysql', # Подключение к серверу MySQL на localhost с помощью PyMySQL DBAPI.
    'host': 'host',
    'port': 'port',
    'username': 'user',
    'password': 'password',
    'database': 'database'
}