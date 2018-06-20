import ROOT
from .Backend import Backend

class Local(Backend):
    """
    Backend that relies on the C++ implementation
    of RDataFrame to locally execute the current graph.

    Attributes
    ----------
    config
        The config object for the Local
        backend.

    """

    def execute(self, generator):
        """
        Executes locally the current RDataFrame graph.

        """

        mapper = generator.get_callable() # Get the callable

        rdf = ROOT.ROOT.RDataFrame(*generator.head_node.args) # Create RDF object

        values, nodes = mapper(rdf) # Execute the mapper function

        values[0].GetValue() # Trigger event-loop

        for i in range(len(values)):
            # Set the values of action nodes to
            # 'TResultPtr's obtained
            nodes[i].value = values[i]