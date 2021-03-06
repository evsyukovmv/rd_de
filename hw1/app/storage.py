import json
import os


class Storage:
    def __init__(self, config, key):
        self.__config = config
        self.__key = key

    def save(self, name, data):
        self.__create_directory(name)

        with open(os.path.join(self.__path(name), self.__key + '.json'), 'w') as json_file:
            json.dump(data, json_file)

    def __path(self, name):
        return os.path.join(self.__config['path'], name)

    def __create_directory(self, name):
        os.makedirs(self.__path(name), exist_ok=True)
