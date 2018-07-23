import ROOT
from .Backend import Backend
from .Utils import Utils

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
    def __init__(self, config={}):
        """
        Creates a new instance of the
        Local implementation of `Backend`.

        Parameters
        ----------
        config (optional)
            The config object for the required
            backend. The default value is an
            empty Python dictionary `{}`.

        """
        super(Local, self).__init__(config)
        operations_not_supported = [
        'Take',
        'Snapshot',
        'Foreach',
        'Reduce',
        'Aggregate'
        ]
        if ROOT.ROOT.IsImplicitMTEnabled():
            # Add `Range` operation only if multi-threading
            # is disabled.
            operations_not_supported.append('Range')

        self.supported_operations = [op for op in self.supported_operations if op not in operations_not_supported]

    def execute(self, generator):
        """
        Executes locally the current RDataFrame graph.

        """
        from .. import includes

        mapper = generator.get_callable() # Get the callable

        Utils.declare_headers(includes) # Declare headers if any

        rdf = ROOT.ROOT.RDataFrame(*generator.head_node.args) # Create RDF object

        values = mapper(rdf) # Execute the mapper function

        # Get the action nodes in the same order as values
        nodes = generator.get_action_nodes()

        values[0].GetValue() # Trigger event-loop

        for i in range(len(values)):
            # Set the values of action nodes to
            # 'TResultPtr's obtained
            nodes[i].value = values[i]