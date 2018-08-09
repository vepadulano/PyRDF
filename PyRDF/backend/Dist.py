from __future__ import print_function
from .Backend import Backend
from .Local import Local
from .Utils import Utils
from abc import abstractmethod

class Dist(Backend):
    """
    Base class for implementing all distributed backends.

    """
    def __init__(self, config = {}):
        """
        Creates an instance of Dist.

        Parameters
        ----------
        config : dict (optional)
            The config options for the current
            distributed backend. Default value is
            an empty python dictionary `{}`.

        """
        super(Dist, self).__init__(config)
        # Operations that aren't supported in distributed backends
        operations_not_supported = [
        'Sum',
        'Mean',
        'Max',
        'Min',
        'Count',
        'Range',
        'Take',
        'Snapshot',
        'Foreach',
        'Reduce',
        'Aggregate'
        ]
        self.supported_operations = [op for op in self.supported_operations if op not in operations_not_supported]

    def BuildRanges(self, nentries, npartitions):
        """
        Builds range pairs from the given values
        of the number of entries in the dataset and
        number of partitions required.

        Parameters
        ----------
        nentries : int
            The number of entries in a dataset.

        npartitions : int
            The number of parallel computations.

        Returns
        -------
        list
            This is a list of range pairs (represented here
            as 2-member tuples).

        """
        if npartitions > nentries:
            # Restrict 'npartitions' if it's greater
            # than 'nentries'
            npartitions = nentries

        partition_size = int(nentries/npartitions)

        i = 0 # Iterator

        ranges = []

        remainder = nentries%npartitions

        while i < nentries:
            # Start value of current range
            start = i
            end = i = start + partition_size

            if remainder:
                # If the modulo value is not
                # exhausted, add '1' to the end
                # of the current range
                end = i = end+1
                remainder -= 1

            ranges.append((start, end))

        return ranges

    def execute(self, generator):
        """
        Executes the current RDataFrame graph
        in the given distributed environment.

        Parameters
        ----------
        generator : PyRDF.CallableGenerator
            An instance of type `CallableGenerator` that is
            responsible for generating the callable function.

        """
        callable_function = generator.get_callable()
        # Arguments needed to create PyROOT RDF object
        rdf_args = generator.head_node.args

        from .. import includes

        def mapper(current_range):
            """
            Triggers the event-loop and executes all
            nodes in the computational graph using the
            callable.

            Parameters
            ----------
            current_range : tuple
                A pair that contains the starting and ending
                values of the current range.

            Returns
            -------
            list
                This respresents the list of values of all
                action nodes in the computational graph.

            """
            import ROOT

            Utils.declare_headers(includes) # Declare headers if any
            rdf = ROOT.ROOT.RDataFrame(*rdf_args) # PyROOT RDF object

            # TODO : If we want to run multi-threaded in a Spark node in
            # the future, use `TEntryList` instead of `Range`
            rdf_range = rdf.Range(current_range[0], current_range[1])

            output = callable_function(rdf_range) # output of the callable

            for i in range(len(output)):
                # FIX ME : RResultPtrs aren't serializable,
                # because of which we have to manually find
                # out the types here and copy construct the
                # values.

                # The type of the value of the action node
                value_type = type(output[i].GetValue())
                # The `value_type` is required here because,
                # after a call to `GetValue`, the values die
                # along with the RResultPtrs
                output[i] = value_type(output[i].GetValue())

            return output

        def reducer(values_list1, values_list2):
            """
            Merges two given lists of values that were
            returned by the mapper function for two different
            ranges.

            Parameters
            ----------
            values_list1
                A list of computed values for a given entry
                range in a dataset.

            values_list2
                A list of computed values for a given entry
                range in a dataset.

            Returns
            -------
            list
                This is a list of values obtained after
                merging two given lists.

            """
            import ROOT
            for i in range(len(values_list1)):
                # A bunch of if-else conditions to merge two values
                if isinstance(values_list1[i], ROOT.TH1) or isinstance(values_list1[i], ROOT.TH2):
                    # Merging two objects of type ROOT.TH1D or ROOT.TH2D
                    values_list1[i].Add(values_list2[i])
                elif isinstance(values_list1[i], ROOT.TGraph):
                    # Prepare a TList
                    tlist = ROOT.TList()
                    tlist.Add(values_list2[i])

                    # Merge the second graph onto the first
                    num_points = values_list1[i].Merge(tlist)

                    # Check if there was an error in merging
                    if num_points == -1:
                        raise Exception("Error reducing two result values of type TGraph !")
                else:
                    raise NotImplementedError("The type \"{}\" isn't supported by the reducer yet !".format(type(values_list1[i])))

            return values_list1

        # Get number of entries in the input dataset using
        # arguments passed to RDataFrame constructor
        self.nentries = generator.head_node.get_num_entries()

        if not self.nentries:
            # Fall back to local execution
            # if 'nentries' is '0'
            return Local.execute(generator)

        # Values produced after Map-Reduce
        values = self.ProcessAndMerge(mapper, reducer)
        # List of action nodes in the same order as values
        nodes = generator.get_action_nodes()

        # Set the value of every action node
        for node, value in zip(nodes, values):
            node.value = value

    @abstractmethod
    def ProcessAndMerge(self, mapper, reducer):
        pass