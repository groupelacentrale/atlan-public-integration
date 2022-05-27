class EnvVariableNotFound(Exception):
    def __init__(self, asset, env_variable):
        self.asset = asset
        self.message = "{} not found in environment variables, creation of {} {} is not possible"\
            .format(env_variable, asset.get_atlan_type_name(), asset.get_asset_name())
        super().__init__(self.message)
