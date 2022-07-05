import os
import sys

from config.main import ConfigClass

sys.path.append(os.getcwd())

CONFIG_PATH = os.getenv("CONFIG_PATH")
if CONFIG_PATH is None:
    CONFIG_PATH = 'config_V15.yaml'

Config = ConfigClass(CONFIG_PATH)

__all__ = ['Config']
