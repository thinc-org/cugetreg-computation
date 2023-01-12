import configparser
import os

def parse_config():
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.ini'))
    return config

def get_config():
    global config
    try:
        return config
    except NameError:
        config = parse_config()
        return config
