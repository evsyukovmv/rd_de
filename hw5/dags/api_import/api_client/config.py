import yaml


class Config:
    @staticmethod
    def load(path, root_key):
        with open(path, 'r') as yaml_file:
            config = yaml.load(yaml_file, Loader=yaml.FullLoader)

        return config.get(root_key)
