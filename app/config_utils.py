from configparser import SafeConfigParser
import os


def DBConnection():
    config = {}

    parser = SafeConfigParser()
    abs_path = "/home/akshay/Chistats/"
    log_config_path = os.path.join(abs_path, 'config.ini')
    parser.read(log_config_path)
    print(parser.sections)
    config['host'] = parser.get('db_config', 'HOST')
    config['user'] = parser.get('db_config', 'USER')
    config['password'] = parser.get('db_config', 'PASSWORD')
    config['port'] = parser.get('db_config', 'PORT')
    config['AWS_ACCESS_KEY_ID'] = parser.get('aws_config', 'AWS_ACCESS_KEY_ID')
    config['AWS_SECRET_ACCESS_KEY'] = parser.get('aws_config', 'AWS_SECRET_ACCESS_KEY')
    config['psql_host'] = parser.get('db_config_psql', 'PSQL_HOST')
    config['psql_user'] = parser.get('db_config_psql', 'PSQL_USER')
    config['psql_password'] = parser.get('db_config_psql', 'PSQL_PASSWORD')
    config['psql_port'] = parser.get('db_config_psql', 'PSQL_PORT')
    return config
