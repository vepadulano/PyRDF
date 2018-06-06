from __future__ import print_function
from .Backend import Backend

class Dist(Backend):

    def __init__(self, config):
        self.config = config

    def execute(self, generator):
        """
        Execution of the event-loop
        in distributed environment

        """
        print("Distributed execution "\
              "hasn't been implemented yet !")