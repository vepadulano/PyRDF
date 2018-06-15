import ROOT
from .Backend import Backend

class Local(Backend):

    def execute(self, generator):
        """
        Execution of the event-loop
        in local environment
        """
        mapper = generator.get_callable()

        rdf = ROOT.ROOT.RDataFrame(*generator.root_node.args)

        values, nodes = mapper(rdf)

        values[0].GetValue() # Trigger event-loop

        for i in range(len(values)):
            nodes[i].value = values[i]