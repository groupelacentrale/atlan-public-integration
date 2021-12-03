import utils


class TableDefinition(object):
    def __init__(self, name, integration_type):
        self.name = name
        self.integration_type = integration_type.lower()
        self.path = utils.get_path(self.name, self.integration_type)

    def __hash__(self):
        return hash((self.name, self.integration_type))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.name == other.name and self.integration_type == other.integration_type
