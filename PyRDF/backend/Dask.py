from __future__ import print_function

import logging
from pprint import pformat

from PyRDF.backend.Dist import Dist

import dask
from dask.distributed import Client

logger = logging.getLogger(__name__)


class Dask(Dist):
    """Dask backend for PyRDF."""

    MIN_NPARTITIONS = 2

    def __init__(self, config={}):
        """Init function."""
        super(Dask, self).__init__(config)

        self.config = config
        self.client = None
        self.npartitions = self._get_partitions()

        logger.debug("Creating {} instance with {} partitions".format(
            type(self), self.npartitions))
        logger.debug("Dask configuration:\n{}".format(
            pformat(dask.config.config)))

    def _get_partitions(self):
        """Estimate partitions of the dataset."""
        npartitions = (self.npartitions or Dask.MIN_NPARTITIONS)
        return int(npartitions)

    def ProcessAndMerge(self, mapper, reducer):
        """
        Performs map-reduce using Dask framework.

        Args:
            mapper (function): A function that runs the computational graph
                and returns a list of values.

            reducer (function): A function that merges two lists that were
                returned by the mapper.

        Returns:
            list: A list representing the values of action nodes returned
            after computation (Map-Reduce).
        """

        ranges = self.build_ranges()  # Get range pairs

        # The Dask client has to be initialized inside some context and not on
        # global scope since it's using Python Multiprocessing and each process
        # fork needs independent environment (e.g. otherwise each process would
        # try recreating a connection to the Dask client).
        if self.client is None:
            logger.debug("Connecting to Dask client.")
            if self.config.get("scheduler_address"):
                self.client = Client(self.config["scheduler_address"])
            else:
                # TODO: Investigate the case where processes=True
                # On my laptop multiprocessing triggers some segfault
                self.client = Client(processes=False)
            logger.debug(
                "Succesfully connected to client {}".format(self.client))

        dmapper = dask.delayed(mapper)
        dreducer = dask.delayed(reducer)

        mergeables_lists = [dmapper(range) for range in ranges]

        while len(mergeables_lists) > 1:
            mergeables_lists.append(
                dreducer(mergeables_lists.pop(0), mergeables_lists.pop(0)))

        if self.config.get("visualize_dask_graph"):
            dask.visualize(mergeables_lists[0])

        return mergeables_lists.pop().compute()

    def distribute_files(self, includes_list):
        """
        TODO: Implement file distribution to Dask workers.

        Args:
            includes_list (list): A list consisting of all necessary C++
                files as strings, created one of the `include` functions of
                the PyRDF API.
        """
        pass
