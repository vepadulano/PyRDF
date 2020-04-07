from __future__ import print_function
import PyRDF
from PyRDF.backend.Backend import Backend
from abc import abstractmethod
import glob
import warnings
import ROOT
import numpy


class Range(object):
    """
    Base class to represent ranges.

    A range represents a logical partition of the entries of a chain and is
    the basis for parallelization. First entry of the range (start) is
    inclusive while the second one is not (end).
    """

    def __init__(self, start, end, filelist=None, friend_info=None):
        """
        Create an instance of a Range

        Args:
            start (int): First entry of the range.

            end (int): Last entry of the range, which is exclusive.

            filelist (list, optional): Files where the range of entries
                belongs to.
        """
        self.start = start
        self.end = end
        self.filelist = filelist
        self.friend_info = friend_info

    def __repr__(self):
        """Return a string representation of the range composition."""
        if self.filelist:
            return ("(" + str(self.start) + "," + str(self.end) + "), " +
                    str(self.filelist))
        else:
            return "(" + str(self.start) + "," + str(self.end) + ")"


class FriendInfo(object):
    """
    A simple class to hold information about friend trees.

    Attributes:
        friend_names (list): A list with the names of the `ROOT.TTree` objects
            which are friends of the main `ROOT.TTree`.

        friend_file_names (list): A list with the paths to the files
            corresponding to the trees in the `friend_names` attribute. Each
            element of `friend_names` can correspond to multiple file names.
    """

    def __init__(self, friend_names=[], friend_file_names=[]):
        """
        Create an instance of FriendInfo

        Args:
            friend_names (list): A list containing the treenames of the friend
                trees.

            friend_file_names (list): A list containing the file names
                corresponding to a given treename in friend_names. Each
                treename can correspond to multiple file names.
        """
        self.friend_names = friend_names
        self.friend_file_names = friend_file_names

    def __bool__(self):
        """
        Define the behaviour of FriendInfo instance when boolean evaluated.
        Both lists have to be non-empty in order to return True.

        Returns:
            bool: True if both lists are non-empty, False otherwise.
        """
        return bool(self.friend_names) and bool(self.friend_file_names)

    def __nonzero__(self):
        """
        Python 2 dunder method for __bool__. Kept for compatibility.
        """
        return self.__bool__()


class Dist(Backend):
    """
    Base class for implementing all distributed backends.

    Attributes:
        npartitions (int): The number of chunks to divide the dataset in, each
            chunk is then processed in parallel.

        supported_operations (list): list of supported RDataFrame operations
            in a distributed environment.

        friend_info (PyRDF.Dist.FriendInfo): A class instance that holds
            information about any friend trees of the main ROOT.TTree
    """

    def __init__(self, config={}):
        """
        Creates an instance of Dist.

        Args:
            config (dict, optional): The config options for the current
                distributed backend. Default value is an empty python
                dictionary: :obj:`{}`.

        """
        super(Dist, self).__init__(config)
        # Operations that aren't supported in distributed backends
        operations_not_supported = [
            'Mean',
            'Max',
            'Min',
            'Range',
            'Take',
            'Foreach',
            'Reduce',
            'Report',
            'Aggregate'
        ]

        # Remove the value of 'npartitions' from config dict
        self.npartitions = config.pop('npartitions', None)

        self.supported_operations = [op for op in self.supported_operations
                                     if op not in operations_not_supported]

        self.friend_info = FriendInfo()

    def get_clusters(self, treename, filelist):
        """
        Extract a list of cluster boundaries for the given tree and files

        Args:
            treename (str): Name of the TTree split into one or more files.

            filelist (list): List of one or more ROOT files.

        Returns:
            list: List of tuples defining the cluster boundaries. Each tuple
            contains four elements: first entry of a cluster, last entry of
            cluster, offset of the cluster and file where the cluster
            belongs to.
        """
        import ROOT

        clusters = []
        offset = 0

        for filename in filelist:
            f = ROOT.TFile.Open(str(filename))
            t = f.Get(treename)

            entries = t.GetEntriesFast()
            it = t.GetClusterIterator(0)
            start = it()
            end = 0

            while start < entries:
                end = it()
                cluster = (start + offset, end + offset, offset, filename)
                clusters.append(cluster)
                start = end

            offset += entries

        return clusters

    def _get_balanced_ranges(self, nentries):
        """
        Builds range pairs from the given values of the number of entries in
        the dataset and number of partitions required. Each range contains the
        same amount of entries, except for those cases where the number of
        entries is not a multiple of the partitions.

        Args:
            nentries (int): The number of entries in a dataset.

        Returns:
            list: List of :obj:`Range`s objects.
        """
        partition_size = int(nentries / self.npartitions)

        i = 0  # Iterator

        ranges = []

        remainder = nentries % self.npartitions

        while i < nentries:
            # Start value of current range
            start = i
            end = i = start + partition_size

            if remainder:
                # If the modulo value is not
                # exhausted, add '1' to the end
                # of the current range
                end = i = end + 1
                remainder -= 1

            ranges.append(Range(start, end))

        return ranges

    def _get_clustered_ranges(self, nentries, treename, filelist,
                              friend_info=FriendInfo()):
        """
        Builds range pairs taking into account the clusters of the dataset.

        Args:
            nentries (int): The number of entries in a dataset.

            treename (str): Name of the tree.

            filelist (list): List of ROOT files.

            friend_info (FriendInfo): Information about friend trees.

        Returns:
            list: List of :obj:`Range`s objects.
        """
        clusters = self.get_clusters(treename, filelist)
        numclusters = len(clusters)

        # Restrict 'npartitions' if it's greater
        # than number of clusters in the filelist
        if self.npartitions > numclusters:
            msg = ("Number of partitions is greater than number of clusters"
                   "in the filelist")
            msg += "\nUsing {} partition(s)".format(numclusters)
            warnings.warn(msg, UserWarning, stacklevel=2)
            self.npartitions = numclusters

        partSize = numclusters // self.npartitions
        remainder = numclusters % self.npartitions

        i = 0  # Iterator
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
                end = clusters[i - 1][1]

            range_files = []
            for idx in range(index_start, index_end):
                current_file = clusters[idx][3]
                if range_files and range_files[-1] == current_file:
                    continue
                range_files.append(clusters[idx][3])

            offset_first_cluster = clusters[index_start][2]
            ranges.append(Range(start - offset_first_cluster,
                                end - offset_first_cluster,
                                range_files,
                                friend_info))
            entries_to_process += (end - start)

        return ranges

    def _get_filelist(self, files):
        """
        Convert single file into list of files and expand globbing

        Args:
            files (str, list): String containing name of a single file or list
                with several file names, both cases may contain globbing
                characters.

        Returns:
            list: list of file names.
        """
        if isinstance(files, str):
            # Expand globbing excluding remote files
            remote_prefixes = ("root:", "http:", "https:")
            if not files.startswith(remote_prefixes):
                files = glob.glob(files)
            else:
                # Convert single file into a filelist
                files = [files, ]

        return files

    def build_ranges(self):
        """
        Define two type of ranges based on the arguments passed to the
        RDataFrame head node.
        """
        if self.npartitions > self.nentries:
            # Restrict 'npartitions' if it's greater
            # than 'nentries'
            self.npartitions = self.nentries

        if self.treename and self.files:
            filelist = self._get_filelist(self.files)
            return self._get_clustered_ranges(self.nentries, self.treename,
                                              filelist, self.friend_info)
        else:
            return self._get_balanced_ranges(self.nentries)

    def _get_friend_info(self, tree):
        """
        Retrieve friend tree names and filenames of a given `ROOT.TTree`
        object.

        Args:
            tree (ROOT.TTree): the ROOT.TTree instance used as an argument to
                PyRDF.RDataFrame(). ROOT.TChain inherits from ROOT.TTree so it
                is a valid argument too.

        Returns:
            (FriendInfo): A FriendInfo instance with two lists as variables.
                The first list holds the names of the friend tree(s), the
                second list holds the file names of each of the trees in the
                first list, each tree name can correspond to multiple file
                names.
        """
        friend_names = []
        friend_file_names = []

        # Get a list of ROOT.TFriendElement objects
        friends = tree.GetListOfFriends()
        if not friends:
            # RDataFrame may have been created with a TTree without
            # friend trees.
            return FriendInfo()

        for friend in friends:
            friend_tree = friend.GetTree()  # ROOT.TTree
            real_name = friend_tree.GetName()  # Treename as string

            # TChain inherits from TTree
            if isinstance(friend_tree, ROOT.TChain):
                cur_friend_files = [
                    # The title of a TFile is the file name
                    chain_file.GetTitle()
                    for chain_file
                    # Get a list of ROOT.TFile objects
                    in friend_tree.GetListOfFiles()
                ]

            else:
                cur_friend_files = [
                    friend_tree.
                    GetCurrentFile().  # ROOT.TFile
                    GetName()  # Filename as string
                ]
            friend_file_names.append(cur_friend_files)
            friend_names.append(real_name)

        return FriendInfo(friend_names, friend_file_names)

    def execute(self, generator):
        """
        Executes the current RDataFrame graph
        in the given distributed environment.

        Args:
            generator (PyRDF.CallableGenerator): An instance of
                :obj:`CallableGenerator` that is responsible for generating
                the callable function.
        """
        callable_function = generator.get_callable()
        # Arguments needed to create PyROOT RDF object
        rdf_args = generator.head_node.args
        treename = generator.head_node.get_treename()
        selected_branches = generator.head_node.get_branches()

        # Avoid having references to the instance inside the mapper
        initialization = Backend.initialization

        def mapper(current_range):
            """
            Triggers the event-loop and executes all
            nodes in the computational graph using the
            callable.

            Args:
                current_range (tuple): A pair that contains the starting and
                    ending values of the current range.

            Returns:
                list: This respresents the list of values of all action nodes
                in the computational graph.
            """
            import ROOT

            # We have to decide whether to do this in Dist or in subclasses
            # Utils.declare_headers(worker_includes)  # Declare headers if any

            # Run initialization method to prepare the worker runtime
            # environment
            initialization()

            # Build rdf
            start = int(current_range.start)
            end = int(current_range.end)

            if treename:
                # Build TChain of files for this range:
                chain = ROOT.TChain(treename)
                for f in current_range.filelist:
                    chain.Add(str(f))

                # We assume 'end' is exclusive
                chain.SetCacheEntryRange(start, end)

                # Gather information about friend trees
                friend_info = current_range.friend_info
                if friend_info:
                    # Zip together the treenames of the friend trees and the
                    # respective file names. Each friend treename can have
                    # multiple corresponding friend file names.
                    tree_files_names = zip(
                        friend_info.friend_names,
                        friend_info.friend_file_names
                    )
                    for friend_treename, friend_filenames in tree_files_names:
                        # Start a TChain with the current friend treename
                        friend_chain = ROOT.TChain(friend_treename)
                        # Add each corresponding file to the TChain
                        for filename in friend_filenames:
                            friend_chain.Add(filename)

                        # Set cache on the same range as the parent TChain
                        friend_chain.SetCacheEntryRange(start, end)
                        # Finally add friend TChain to the parent
                        chain.AddFriend(friend_chain)

                if selected_branches:
                    rdf = ROOT.ROOT.RDataFrame(chain, selected_branches)
                else:
                    rdf = ROOT.ROOT.RDataFrame(chain)
            else:
                rdf = ROOT.ROOT.RDataFrame(*rdf_args)  # PyROOT RDF object

            # # TODO : If we want to run multi-threaded in a Spark node in
            # # the future, use `TEntryList` instead of `Range`
            # rdf_range = rdf.Range(current_range.start, current_range.end)

            # Output of the callable
            output = callable_function(rdf, rdf_range=current_range)

            for i in range(len(output)):
                # `AsNumpy` and `Snapshot` return respectively `dict` and `list`
                # that don't have the `GetValue` method.
                if isinstance(output[i], dict):
                    # Fix class name to 'ndarray' to avoid issues with
                    # Pickle protocol 2
                    for value in output[i].values():
                        value.__class__.__name__ = "ndarray"
                    continue
                if isinstance(output[i], list):
                    continue
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

            Args:
                values_list1 (list): A list of computed values for a given
                    entry range in a dataset.

                values_list2 (list): A list of computed values for a given
                    entry range in a dataset.

            Returns:
                list: This is a list of values obtained after merging two
                given lists.
            """
            import ROOT

            for i in range(len(values_list1)):
                # A bunch of if-else conditions to merge two values

                # Create a global list with all the files of the partial
                # snapshots
                if isinstance(values_list1[i], list):
                    values_list1[i].extend(values_list2[i])

                elif isinstance(values_list1[i], dict):
                    combined = {
                        key: numpy.concatenate([values_list1[i][key],
                                                values_list2[i][key]])
                        for key in values_list1[i]
                    }
                    values_list1[i] = combined
                elif (isinstance(values_list1[i], ROOT.TH1) or
                      isinstance(values_list1[i], ROOT.TH2)):
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
                        msg = "Error reducing two result values of type TGraph!"
                        raise Exception(msg)

                elif isinstance(values_list1[i], float):
                    # Adding values resulting from a Sum() operation
                    # Sum() always returns a float in python
                    values_list1[i] += values_list2[i]

                elif (isinstance(values_list1[i], int) or
                        isinstance(values_list1[i], long)):  # noqa: Python 2
                    # Adding values resulting from a Count() operation
                    values_list1[i] += values_list2[i]

                else:
                    msg = ("Type \"{}\" is not supported by the reducer yet!"
                           .format(type(values_list1[i])))
                    raise NotImplementedError(msg)

            return values_list1

        # Get number of entries in the input dataset using
        # arguments passed to RDataFrame constructor
        self.nentries = generator.head_node.get_num_entries()

        # Retrieve the treename used to initialize the RDataFrame
        self.treename = generator.head_node.get_treename()

        # Retrieve the filenames used to initialize the RDataFrame
        self.files = generator.head_node.get_inputfiles()

        # Retrieve the ROOT.TTree instance used to initialize the RDataFrame
        self.tree = generator.head_node.get_tree()

        # Retrieve info about the friend trees
        if self.tree:
            self.friend_info = self._get_friend_info(self.tree)

        if not self.nentries:
            # Fall back to local execution
            # if 'nentries' is '0'
            msg = ("No entries in the Tree, falling back to local execution!")
            warnings.warn(msg, UserWarning, stacklevel=2)
            PyRDF.use("local")
            from .. import current_backend
            return current_backend.execute(generator)

        # Values produced after Map-Reduce
        values = self.ProcessAndMerge(mapper, reducer)
        # List of action nodes in the same order as values
        nodes = generator.get_action_nodes()

        # Set the value of every action node
        for node, value in zip(nodes, values):
            if node.operation.name == "Snapshot":
                # Retrieve treename from operation args and start TChain
                snapshot_treename = node.operation.args[0]
                snapshot_chain = ROOT.TChain(snapshot_treename)
                # Add partial snapshot files to the chain
                for filename in value:
                    snapshot_chain.Add(filename)
                # Create a new rdf with the chain and return that to user
                snapshot_rdf = PyRDF.RDataFrame(snapshot_chain)
                node.value = snapshot_rdf
            else:
                node.value = value

    @abstractmethod
    def ProcessAndMerge(self, mapper, reducer):
        """
        Subclasses must define how to run map-reduce functions on a given
        backend.
        """
        pass

    @abstractmethod
    def distribute_files(self, includes_list):
        """
        Subclasses must define how to send all files needed for the analysis
        (like headers and libraries) to the workers.
        """
        pass
