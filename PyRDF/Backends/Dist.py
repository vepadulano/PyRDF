from __future__ import print_function

import collections
import glob
import logging
import warnings
from abc import abstractmethod

import numpy
import ROOT
from PyRDF.Backends import Base
from PyRDF.Backends import Utils

logger = logging.getLogger(__name__)

Range = collections.namedtuple("Range",
                               ["start", "end", "filelist", "friend_info"])


def _n_even_chunks(iterable, n_chunks):
    """
    Yield `n_chunks` as even chunks as possible from `iterable`. Though generic,
    this function is used in _get_clustered_ranges to split a list of clusters
    into multiple sublists. Each sublist will hold the clusters that should fit
    in a single partition of the distributed dataset::

        [
            # Partition 1 will process the following clusters
            [
                (start_0_0, end_0_0, offset_0, (filename_0, 0)),
                (start_0_1, end_0_1, offset_0, (filename_0, 0)),
                ...,
                (start_1_0, end_1_0, offset_1, (filename_1, 1)),
                (start_1_1, end_1_1, offset_1, (filename_1, 1)),
                ...,
                (start_n_0, end_n_0, offset_n, (filename_n, n)),
                (start_n_1, end_n_1, offset_n, (filename_n, n)),
                ...
            ],
            # Partition 2 will process these other clusters
            [
                (start_n+1_0, end_n+1_0, offset_n+1, (filename_n+1, n+1)),
                (start_n+1_1, end_n+1_1, offset_n+1, (filename_n+1, n+1)),
                ...,
                (start_m_0, end_m_0, offset_m, (filename_m, m)),
                (start_m_1, end_m_1, offset_m, (filename_m, m)),
                ...
            ],
            ...
        ]

    """
    last = 0
    itlenght = len(iterable)
    for i in range(1, n_chunks + 1):
        cur = int(round(i * (itlenght / n_chunks)))
        yield iterable[last:cur]
        last = cur


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


class DistBackend(Base.BaseBackend):
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

    def __init__(self):
        """Creates an instance of Dist."""
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

        self.npartitions = None

        self.supported_operations = [op for op in self.supported_operations
                                     if op not in operations_not_supported]

        self.friend_info = FriendInfo()

        self.headers = set()
        self.shared_libraries = set()

    def get_clusters(self, treename, filelist):
        """
        Extract a list of cluster boundaries for the given tree and files

        Args:
            treename (str): Name of the TTree split into one or more files.

            filelist (list): List of one or more ROOT files.

        Returns:
            list: List of tuples defining the cluster boundaries. Each tuple
            contains four elements: first entry of a cluster, last entry of
            cluster (exclusive), offset of the cluster and file where the
            cluster belongs to::

                [
                    (0, 100, 0, ("filename_1.root", 0)),
                    (100, 200, 0, ("filename_1.root", 0)),
                    ...,
                    (10000, 10100, 10000, ("filename_2.root", 1)),
                    (10100, 10200, 10000, ("filename_2.root", 1)),
                    ...,
                    (n, n+100, n, ("filename_n.root", n)),
                    (n+100, n+200, n, ("filename_n.root", n)),
                    ...
                ]
        """

        clusters = []
        cluster = collections.namedtuple(
            "cluster", ["start", "end", "offset", "filetuple"])
        fileandindex = collections.namedtuple("fileandindex",
                                              ["filename", "index"])
        offset = 0
        fileindex = 0

        for filename in filelist:
            f = ROOT.TFile.Open(filename)
            t = f.Get(treename)

            entries = t.GetEntriesFast()
            it = t.GetClusterIterator(0)
            start = it()
            end = 0

            while start < entries:
                end = it()
                clusters.append(cluster(start + offset, end + offset, offset,
                                        fileandindex(filename, fileindex)))
                start = end

            fileindex += 1
            offset += entries

        logger.debug("Returning files with their clusters:\n%s",
                     "\n\n".join(map(str, clusters)))

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

            ranges.append(Range(start, end, None, None))

        return ranges

    def _get_clustered_ranges(self, treename, filelist,
                              friend_info=FriendInfo()):
        """
        Builds ``Range`` objects taking into account the clusters of the
        dataset. Each range will represent the entries processed within a single
        partition of the distributed dataset.

        Args:
            treename (str): Name of the tree.

            filelist (list): List of ROOT files.

            friend_info (FriendInfo): Information about friend trees.

        Returns:
            list[collections.namedtuple]: List containinig the ranges in which
            the dataset has been split for distributed execution. Each ``Range``
            contains a starting entry, an ending entry, the list of files
            that are traversed to get all the entries and information about
            friend trees::

                [
                    Range(start=0,
                        end=42287856,
                        filelist=['Run2012B_TauPlusX.root',
                                  'Run2012C_TauPlusX.root'],
                        friend_info=None),
                    Range(start=6640348,
                        end=51303171,
                        filelist=['Run2012C_TauPlusX.root'],
                        friend_info=None)
                ]

        """

        # Retrieve a list of clusters for all files of the tree
        clustersinfiles = self.get_clusters(treename, filelist)
        numclusters = len(clustersinfiles)

        # Restrict `npartitions` if it's greater than clusters of the dataset
        if self.npartitions > numclusters:
            msg = ("Number of partitions is greater than number of clusters "
                   "in the dataset. Using {} partition(s)".format(numclusters))
            warnings.warn(msg, UserWarning, stacklevel=2)
            self.npartitions = numclusters

        logger.debug("%s clusters will be split along %s partitions.",
                     numclusters, self.npartitions)

        """
        This list comprehension builds ``Range`` tuples with the following
        elements:

        1. ``start``: The minimum entry among all the clusters considered in a
           given partition. The offset of the first cluster of the list is
           subtracted. This is useful to keep the reference of the range with
           respect to the current files (see below).
        2. ``end``: The maximum entry among all the clusters considered in a
           given partition. The offset of the first cluster of the list is
           subtracted. This is useful to keep the reference of the range with
           respect to the current files (see below).
        3. ``filelist``: The list of files that are span between entries
           ``start`` and ``end``::

                Filelist: [file_1,file_2,file_3,file_4]

                Clustered range: [0,150]
                file_1 holds entries [0, 100]
                file_2 holds entries [101, 200]
                Then the clustered range should open [file_1, file_2]

                Clustered range: [150,350]
                file_3 holds entris [201, 300]
                file_4 holds entries [301, 400]
                Then the clustered range should open [file_2, file_3, file_4]

           Each ``cluster`` namedtuple has a ``fileandindex`` namedtuple. The
           second element of this tuple corresponds to the index of the file in
           the input `TChain`. This way all files can be uniquely identified,
           even if there is some repetition (e.g. when building a TChain with
           multiple instances of the same file). The algorithm to retrieve the
           correct files for each range takes the unique filenames from the list
           of clusters and sorts them by their index to keep the original order.

           In each file only the clusters needed to process the clustered range
           will be read.
        4. ``friend_info``: Information about friend trees.

        In each range, the offset of the first file is always subtracted to the
        ``start`` and ``end`` entries. This is needed to maintain a reference of
        the entries of the range with respect to the list of files that hold
        them. For example, given the following files::

            tree10000entries10clusters.root --> 10000 entries, 10 clusters
            tree20000entries10clusters.root --> 20000 entries, 10 clusters
            tree30000entries10clusters.root --> 30000 entries, 10 clusters

        Building 2 ranges will lead to the following tuples::

            Range(start=0,
                  end=20000,
                  filelist=['tree10000entries10clusters.root',
                            'tree20000entries10clusters.root'],
                  friend_info=None)

            Range(start=10000,
                  end=50000,
                  filelist=['tree20000entries10clusters.root',
                            'tree30000entries10clusters.root'],
                  friend_info=None)

        The first ``Range`` will read the first 10000 entries from the first
        file, then switch to the second file and read the first 10000 entries.
        The second ``Range`` will start from entry number 10000 of the second
        file up until the end of that file (entry number 20000), then switch to
        the third file and read the whole 30000 entries there.
        """
        clustered_ranges = [
            Range(
                min(clusters)[0] - clusters[0].offset,  # type: int
                max(clusters)[1] - clusters[0].offset,  # type: int
                [
                    filetuple.filename
                    for filetuple in sorted(set([
                        cluster.filetuple for cluster in clusters
                    ]), key=lambda curtuple: curtuple[1])
                ],  # type: list[str]
                friend_info  # type: FriendInfo
            )  # type: collections.namedtuple
            for clusters in _n_even_chunks(clustersinfiles, self.npartitions)
        ]

        logger.debug("Created following clustered ranges:\n%s",
                     "\n\n".join(map(str, clustered_ranges)))

        return clustered_ranges

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
            logger.debug("Building clustered ranges for tree %s with the "
                         "following input files:\n%s",
                         self.treename,
                         list(self.files)
                         )
            return self._get_clustered_ranges(self.treename, filelist,
                                              self.friend_info)
        else:
            logger.debug(
                "Building balanced ranges for %d entries.", self.nentries)
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
        initialization = Base.BaseBackend.initialization

        def mapper(current_range):
            """
            Triggers the event-loop and executes all
            nodes in the computational graph using the
            callable.

            Args:
                current_range (Range): A Range named tuple, representing the
                    range of entries to be processed, their input files and
                    information about friend trees.

            Returns:
                list: This respresents the list of (mergeable)values of all
                action nodes in the computational graph.
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

            mergeables = [
                resultptr  # Here resultptr is already the result value
                if isinstance(resultptr, (dict, list))
                else ROOT.ROOT.Detail.RDF.GetMergeableValue(resultptr)
                for resultptr in output
            ]
            return mergeables

        def reducer(mergeables_out, mergeables_in):
            """
            Merges two given lists of values that were
            returned by the mapper function for two different
            ranges.

            Args:
                mergeables_out (list): A list of computed (mergeable)values for
                    a given entry range in a dataset. The elements of this list
                    will be updated with the information contained in the
                    elements of the other argument list.

                mergeables_in (list): A list of computed (mergeable)values for
                    a given entry range in a dataset.

            Returns:
                list: The list of updated (mergeable)values.
            """

            import ROOT

            # We still need the list index to modify results of `Snapshot` and
            # `AsNumpy` in place.
            for index, (mergeable_out, mergeable_in) in enumerate(
                    zip(mergeables_out, mergeables_in)):
                # Create a global list with all the files of the partial
                # snapshots.
                if isinstance(mergeable_out, list):
                    mergeables_out[index].extend(mergeable_in)

                # Concatenate the partial numpy arrays along the same key of
                # the dictionary.
                elif isinstance(mergeable_out, dict):
                    mergeables_out[index] = {
                        key: numpy.concatenate([mergeable_out[key],
                                                mergeable_in[key]])
                        for key in mergeable_out
                    }

                # The `MergeValues` function modifies the arguments in place
                # so there's no need to access the list elements.
                else:
                    ROOT.ROOT.Detail.RDF.MergeValues(
                        mergeable_out, mergeable_in)

            return mergeables_out

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
            # Empty trees cannot be processed distributedly
            raise RuntimeError(
                "No entries in the TTree, distributed execution aborted!")

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
                node.value = self.make_dataframe(snapshot_chain)
            elif node.operation.name == "AsNumpy":
                node.value = value
            else:
                node.value = value.GetValue()

    @abstractmethod
    def ProcessAndMerge(self, mapper, reducer):
        """
        Subclasses must define how to run map-reduce functions on a given
        backend.
        """
        pass

    @abstractmethod
    def distribute_unique_paths(self, paths):
        """
        Subclasses must define how to send all files needed for the analysis
        (like headers and libraries) to the workers.
        """
        pass

    def distribute_files(self, files_paths):
        """
        Sends to the workers the generic files needed by the user.

        Args:
            files_paths (str, iter): Paths to the files to be sent to the
                distributed workers.
        """
        files_to_distribute = set()

        if isinstance(files_paths, str):
            files_to_distribute.update(
                Utils.get_paths_set_from_string(files_paths))
        else:
            for path_string in files_paths:
                files_to_distribute.update(
                    Utils.get_paths_set_from_string(path_string))

        self.distribute_unique_paths(files_to_distribute)

    def distribute_headers(self, headers_paths):
        """
        Includes the C++ headers to be declared before execution.

        Args:
            headers_paths (str, iter): A string or an iterable (such as a
                list, set...) containing the paths to all necessary C++ headers
                as strings. This function accepts both paths to the headers
                themselves and paths to directories containing the headers.
        """
        headers_to_distribute = set()

        if isinstance(headers_paths, str):
            headers_to_distribute.update(
                Utils.get_paths_set_from_string(headers_paths))
        else:
            for path_string in headers_paths:
                headers_to_distribute.update(
                    Utils.get_paths_set_from_string(path_string))

        # Distribute header files to the workers
        self.distribute_unique_paths(headers_to_distribute)

        # Declare headers locally
        Utils.declare_headers(headers_to_distribute)

        # Finally, add everything to the includes set
        self.headers.update(headers_to_distribute)

    def distribute_shared_libraries(self, shared_libraries_paths):
        """
        Includes the C++ shared libraries to be declared before execution. If
        any pcm file is present in the same folder as the shared libraries, the
        function will try to retrieve them and distribute them.

        Args:
            shared_libraries_paths (str, iter): A string or an iterable (such as
                a list, set...) containing the paths to all necessary C++ shared
                libraries as strings. This function accepts both paths to the
                libraries themselves and paths to directories containing the
                libraries.
        """
        libraries_to_distribute = set()
        pcm_to_distribute = set()

        if isinstance(shared_libraries_paths, str):
            pcm_to_distribute, libraries_to_distribute = (
                Utils.check_pcm_in_library_path(shared_libraries_paths))
        else:
            for path_string in shared_libraries_paths:
                pcm, libraries = Utils.check_pcm_in_library_path(
                    path_string
                )
                libraries_to_distribute.update(libraries)
                pcm_to_distribute.update(pcm)

        # Distribute shared libraries and pcm files to the workers
        self.distribute_unique_paths(libraries_to_distribute)
        self.distribute_unique_paths(pcm_to_distribute)

        # Include shared libraries locally
        Utils.declare_shared_libraries(libraries_to_distribute)

        # Finally, add everything to the includes set
        self.shared_libraries.update(libraries_to_distribute)

    @abstractmethod
    def make_dataframe(self, *args, **kwargs):
        """
        Distributed backends have to take care of creating an RDataFrame object
        that can run distributedly.
        """
