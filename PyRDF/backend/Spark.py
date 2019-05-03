from __future__ import print_function
from PyRDF.backend.Dist import Dist
from PyRDF.backend.Utils import Utils
from pyspark import SparkConf, SparkContext
from pyspark import SparkFiles
import ntpath


class Spark(Dist):
    """
    Backend that executes the computational graph using using `Spark` framework
    for distributed execution.

    """

    MIN_NPARTITIONS = 2

    def __init__(self, config={}):
        """
        Creates an instance of the Spark backend class.

        Parameters
        ----------
        config : dict (optional)
            The config options for Spark backend. The default value is an empty
            Python dictionary `{}`. `config` should be a dictionary of Spark
            configuration options and their values with 'npartitions' as the
            only allowed extra parameter.

            For example :-

            config = {
                'npartitions':20,
                'spark.master':'myMasterURL',
                'spark.executor.instances':10,
                'spark.app.name':'mySparkAppName'
            }

            IMPORTANT NOTE :- If a SparkContext is already set in the current
            environment, the Spark configuration parameters from 'config' will
            be ignored and the already existing SparkContext would be used.

        """
        super(Spark, self).__init__(config)

        # Remove the value of 'npartitions' from config dict
        self.npartitions = config.pop('npartitions', None)

        sparkConf = SparkConf().setAll(config.items())
        self.sparkContext = SparkContext.getOrCreate(sparkConf)

        # Set the value of 'npartitions' if it doesn't exist
        self.npartitions = self._get_partitions()

    def _get_partitions(self):
        npart = (self.npartitions or
                 self.sparkContext.getConf().get('spark.executor.instances') or
                 Spark.MIN_NPARTITIONS)
        # getConf().get('spark.executor.instances') could return a string
        return int(npart)

    def ProcessAndMerge(self, mapper, reducer):
        """
        Performs map-reduce using Spark framework.

        Parameters
        ----------
        mapper : function
            A function that runs the computational graph
            and returns a list of values.

        reducer : function
            A function that merges two lists that were
            returned by the mapper.

        Returns
        -------
        list
            This list represents the values of action nodes
            returned after computation (Map-Reduce).
        """
        from .. import includes
        def mapSpark(current_range):
            files_on_executor = [
                SparkFiles.get(ntpath.basename(filepath))
                for filepath in includes
            ]
            Utils.declare_headers(files_on_executor)
            return mapper(current_range)

        ranges = self.build_ranges(self.npartitions)  # Get range pairs

        # Build parallel collection
        sc = self.sparkContext
        parallel_collection = sc.parallelize(ranges, self.npartitions)

        # Map-Reduce using Spark
        return parallel_collection.map(mapSpark).treeReduce(reducer)

    def distribute_files(self, includes_list):
        """
        Spark supports sending files to the executors via the
        `SparkContext.addFile` method. This method receives in input the path
        to the file (relative to the path of the current python session). The
        file is initially added to the Spark driver and then sent to the
        workers when they are initialized.

        Parameters
        ----------
        includes_list : list
            A list consisting of all necessary C++ headers as strings, created
            via the `PyRDF.include()` method.
        """
        for file in includes_list:
            self.sparkContext.addFile(file)

    def get_distributed_files(self, distributed_file_paths):
        """
        To get the specific path of the file(s) that have been sent to each
        worker, one can use the `SparkFiles.get()` method. It takes the file
        name (not the path) as input and then returns the path of the file on
        the current executor.

        Parameters
        ----------
        distributed_file_paths : list
            A list with the paths to all necessary C++ headers as strings,
            precedently added to the workers via the `distribute_files()`
            method.
        """
        files_on_executor = [
            SparkFiles.get(ntpath.basename(filepath))
            for filepath in distributed_file_paths
        ]
        return files_on_executor

    def __getstate__(self):
        """Docstring missing."""
        state = self.__dict__.copy()
        del state["sparkContext"]
        print("Getstate called")
        print("Dictionary of Spark instance")
        print(state)
        return None