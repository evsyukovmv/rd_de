import os

from hw1.app.config import Config
from hw1.app.api import Api
from hw1.app.storage import Storage


def main():
    config = Config.load(os.path.join('./app', 'config.yaml'), 'robot_dreams_de')
    api = Api(config['api'])
    storage = Storage(config['storage'], 'out_of_stock')

    try:
        for i in range(1, 31):
            day = str(i).rjust(2, '0')
            date = f'2021-11-{day}'

            print(date)

            print('\tRetrieving data')
            data = api.out_of_stock(date)

            print('\tSaving data')
            storage.save(date, data)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
