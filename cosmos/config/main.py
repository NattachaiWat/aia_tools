from os import getcwd
from os import getenv
from os.path import join
import json

from yaml import YAMLError, safe_load as load


def _load_config(config_file="config_V15.yaml"):
    path_config_file = join(getcwd(), config_file)
    with open(path_config_file, 'r') as stream:
        try:
            config_parser = load(stream)
            return config_parser
        except YAMLError as error:
            assert f"Can't load config.yaml Error:{error}"


class ConfigClass:

    def get_from_single_key(self, key):
        return self._config_parser[key] if key in self._config_parser else None

    def get_from_two_key(self, key1, key2):
        if key1 in self._config_parser:
            if key2 in self._config_parser[key1]:
                return self._config_parser[key1][key2]
            else:
                return None
        else:
            return None

    def __init__(self, config_file="config.yaml"):
        self._config_parser = _load_config(config_file=config_file)

    @property
    def MONGODB(self):
        output = dict(
            CONNECTION_STRING=getenv('MONGODB_CONNECTION_STRING'),
            IS_LOG=getenv("MONGODB_IS_LOG", 'False').lower() in ['true', '1', 't']
        )
        if all(v is not None for v in output.values()):
            return output
        else:
            output = self.get_from_single_key('MONGODB')
            return output


