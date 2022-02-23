import requests
import json
from airflow.models import Variable


class Api:
    def __init__(self, config):
        self.__config = config
        self.__cached_token = None

    def authenticate(self):
        url = self.__url_for('authentication')

        response = requests.post(
            url,
            headers=self.__headers(),
            data=json.dumps(Variable.get('API_CREDENTIALS', deserialize_json=True)),
            timeout=self.__config['timeout']
        )
        self.__validate_response(response)

        return response.json()

    def out_of_stock(self, date):
        url = self.__url_for('out_of_stock')
        data = {"date": date}
        response = requests.get(
            url,
            headers=self.__headers(auth=True),
            data=json.dumps(data),
            timeout=self.__config['timeout']
        )
        self.__validate_response(response)

        return response.json()

    def __url_for(self, endpoint):
        return self.__config['base_url'] + self.__config['endpoints'][endpoint]

    def __token(self):
        if self.__cached_token is None:
            self.__cached_token = self.authenticate()['access_token']
        return self.__cached_token

    def __headers(self, auth=False):
        headers = {"content-type": "application/json"}
        if auth:
            headers['Authorization'] = "JWT " + self.__token()
        return headers

    @staticmethod
    def __validate_response(response):
        if response.status_code != requests.codes.ok:
            raise Exception("Api authentication failed: " + response.text)
