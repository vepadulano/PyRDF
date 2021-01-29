from __future__ import print_function

import logging
from abc import abstractmethod

import numpy
import ROOT
from PyRDF.Backends import Base
from PyRDF.Backends import Utils

logger = logging.getLogger(__name__)


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

        self.supported_operations = [op for op in self.supported_operations
                                     if op not in operations_not_supported]

        self.headers = set()
        self.shared_libraries = set()

    def execute(self, generator):
        """
        Executes the current RDataFrame graph
        in the given distributed environment.

        Args:
            generator (PyRDF.CallableGenerator): An instance of
                :obj:`CallableGenerator` that is responsible for generating
                the callable function.
        """
        headnode = generator.headnode
        callable_function = generator.get_callable()
        # Arguments needed to create PyROOT RDF object
        rdf_args = headnode.args
        treename = headnode.get_treename()
        selected_branches = headnode.get_branches()

        # Avoid having references to the instance inside the mapper
        initialization = Base.BaseBackend.initialization

        # Build the ranges for the current dataset
        headnode.npartitions = self.optimize_npartitions(headnode.npartitions)
        ranges = headnode.build_ranges()

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

        # Values produced after Map-Reduce
        values = self.ProcessAndMerge(ranges, mapper, reducer)
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
    def ProcessAndMerge(self, ranges, mapper, reducer):
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

    def optimize_npartitions(self, npartitions):
        """
        Distributed backends may optimize the number of partitions of the
        current dataset or leave it as it is.
        """
        return npartitions

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
