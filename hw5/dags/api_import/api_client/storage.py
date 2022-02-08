import json
import os
from hdfs import InsecureClient


class Storage:
    def __init__(self, config, key):
        self.__config = config
        self.__key = key
        self.__hadoop_client = InsecureClient(url=self.__config['hadoop']['url'], user=self.__config['hadoop']['user'])

    def save(self, name, data):
        self.__create_directory(name)

        path = os.path.join(self.__path(name), self.__key + '.json')
        with self.__hadoop_client.write(path, encoding='utf-8') as json_file:
            json.dump(data, json_file)

    def __path(self, name):
        return os.path.join(self.__config['path'], name)

    def __create_directory(self, name):
        os.makedirs(self.__path(name), exist_ok=True)
