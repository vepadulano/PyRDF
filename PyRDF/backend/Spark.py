from __future__ import print_function
from .Dist import Dist
from pyspark import SparkConf, SparkContext
from .Utils import Utils


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

        Utils.declare_headers(includes)  # Declare headers if any

        ranges = self.BuildRanges(self.npartitions)  # Get range pairs

        # Build parallel collection
        sc = self.sparkContext
        parallel_collection = sc.parallelize(ranges, self.npartitions)

        # Map-Reduce using Spark
        return parallel_collection.map(mapper).treeReduce(reducer)
