import os


class ApiConfig:
    def __init__(self, api_token, instance):
        self.api_token = api_token
        self.instance = instance


def create_api_config():
    atlan_api_token = os.getenv('ATLAN_TOKEN')
    atlan_instance = os.environ.get('ATLAN_INSTANCE')
    return ApiConfig(atlan_api_token, atlan_instance)
