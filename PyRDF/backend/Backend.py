class Backend(object):

    def __init__(self, config={}):
        self.config = config

    def execute(self, generator):
        raise NotImplementedError("Incorrect backend environment !")