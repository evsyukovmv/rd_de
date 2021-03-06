import json
import os
from hdfs import InsecureClient
from airflow.models import Variable


class Storage:
    def __init__(self, config, key):
        self.__config = config
        self.__key = key
        self.__hadoop_client = InsecureClient(**Variable.get('HDFS_CREDENTIALS', deserialize_json=True))

    def save(self, name, data):
        path = os.path.join(self.__path(name), self.__key + '.json')
        with self.__hadoop_client.write(path, encoding='utf-8', overwrite=True) as json_file:
            json.dump(data, json_file)

    def __path(self, name):
        return os.path.join(self.__config['path'], name)
