from configparser import ConfigParser
import os


config = ConfigParser()
config.read(os.path.join('conf', 'config.ini'))
