from __future__ import print_function
from .Backend import Backend
from .Local import Local
from .Utils import Utils
from abc import abstractmethod
from glob import glob
import warnings

class Range(object):
    """
    Base class to represent ranges.

    A range represents a logical partition of the entries of a chain and is
    the basis for parallelization. First entry of the range (start) is inclusive
    while the second one is not (end).
    """
    def __init__(self, start, end, filelist=None):
        self.start = start
        self.end = end
        self.filelist = filelist

    def __repr__(self):
        if self.filelist:
            return "(" + str(self.start) + "," + str(self.end) + "), " + str(self.filelist)
        else:
            return "(" + str(self.start) + "," + str(self.end) + ")"

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

    def GetClusters(self, treename, filelist):
        """
        Extract a list of cluster boundaries for the given tree and files

        Parameters
        ----------
        treename: str
            Name of the TTree split into one or more files

        filelist: list
            List of one or more ROOT files

        Returns
        -------
        list
            List of tuples defining the cluster boundaries

            Each tuple contains four elements: first entry of a cluster, last
            entry of cluster, offset of the cluster and file where the cluster
            belongs to.

        """
        import ROOT

        clusters = []
        offset = 0

        for filename in filelist:
            f = ROOT.TFile.Open(filename)
            t = f.Get(treename)

            entries = t.GetEntriesFast()
            it = t.GetClusterIterator(0)
            start = it()
            end = 0

            while start < entries:
              end = it()
              clusters.append((start + offset, end + offset, offset, filename))
              start = end

            offset += entries

        return clusters

    def _getBalancedRanges(self, nentries, npartitions):
        """
        Builds range pairs from the given values of the number of entries in the
        dataset and number of partitions required. Each range contains the same
        amount of entries, except for those cases where the number of
        entries is not a multiple of the partitions.

        Parameters
        ----------
        nentries : int
            The number of entries in a dataset.

        npartitions : int
            The number of parallel computations.

        Returns
        -------
        list
            List of ranges

        """
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

            ranges.append(Range(start, end))

        return ranges

    def _getClusteredRanges(self, nentries, npartitions, treename, filelist):
        """
        Builds range pairs taking into account the clusters of the dataset.

        Parameters
        ----------
        nentries : int
            The total number of entries in a dataset.

        npartitions : int
            The number of parallel computations.

        treename : str
            Name of the tree

        filelist : list
            List of ROOT files

        Returns
        -------
        list
            List of ranges

        """
        clusters = self.GetClusters(treename, filelist)
        numclusters = len(clusters)

        # Restrict 'npartitions' if it's greater
        # than number of clusters in the filelist
        if npartitions > numclusters:
            msg = "Number of partitions is greater than number of clusters in the filelist"
            msg += "\nUsing {} partition(s)".format(numclusters)
            warnings.warn(msg, UserWarning, stacklevel=2)
            npartitions = numclusters

        partSize = numclusters / npartitions
        remainder = numclusters % npartitions

        i = 0 # Iterator
        ranges = []
        entries_to_process = 0

        while i < numclusters:
            index_start = i
            start = clusters[i][0]
            i = i + partSize
            if remainder > 0:
                i += 1
                remainder -= 1
            index_end = i
            if i == numclusters:
                end = clusters[-1][1]
            else:
                end = clusters[i-1][1]

            range_files = []
            for idx in range(index_start, index_end):
                current_file = clusters[idx][3]
                if range_files and range_files[-1] == current_file:
                    continue
                range_files.append(clusters[idx][3])

            offset_first_cluster = clusters[index_start][2]
            ranges.append(Range(start - offset_first_cluster, end - offset_first_cluster, range_files))
            entries_to_process += (end - start)

        return ranges

    def _getFilelist(self, files):
        """
        Convert single file into list of files and expand globbing

        Parameters
        ----------
        files : str or list
            String containing name of a single file or list with several file
            names, both cases may contain globbing characters.

        Returns
        -------
        list
            List of file names
        """
        if isinstance(files, str):
            files = [files, ]
        expanded_files = map(glob, files)
        flattened_list = [fname for flist in expanded_files for fname in flist ]
        return flattened_list

    def BuildRanges(self, npartitions):
        """
        Define ranges based on the arguments passed the RDataFrame head node
        """
        if npartitions > self.nentries:
            # Restrict 'npartitions' if it's greater
            # than 'nentries'
            npartitions = self.nentries

        if self.treename and self.files:
            filelist = self._getFilelist(self.files)
            return self._getClusteredRanges(self.nentries, npartitions, self.treename, filelist)
        else:
            return self._getBalancedRanges(self.nentries, npartitions)


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
            rdf_range = rdf.Range(current_range.start, current_range.end)

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
        self.treename = generator.head_node.get_treename()
        self.files    = generator.head_node.get_inputfiles()

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
