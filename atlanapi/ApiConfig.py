import os


class ApiConfig:
    def __init__(self, api_key, instance):
        self.api_key = api_key
        self.instance = instance


def create_api_config():
    atlan_api_key = os.getenv('ATLAN_API_KEY')
    atlan_instance = os.environ.get('ATLAN_INSTANCE')
    return ApiConfig(atlan_api_key, atlan_instance)
