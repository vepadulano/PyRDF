from __future__ import print_function
from .Dist import Dist
from pyspark import SparkConf, SparkContext

class Spark(Dist):
    """
    Backend that executes the computational graph
    using using `Spark` framework for distributed
    execution.

    """
    def __init__(self, config = {}):
        """
        Creates an instance of the Spark
        backend class.

        Parameters
        ----------
        config : dict (optional)
            The config options for Spark backend. The default
            value is an empty Python dictionary `{}`. Config
            should be a dictionary of Spark configuration
            options and their values with 'npartitions' as
            the only allowed extra parameter.
            For example :-

            config = {
            'npartitions':20,
            'spark.master':'myMasterURL',
            'spark.executor.instances':10,
            'spark.app.name':'mySparkAppName'
            }

            IMPORTANT NOTE :- If a SparkContext is already set
            in the current environment, the Spark configuration
            parameters from 'config' will be ignored and the already
            existing SparkContext would be used.

        """
        super(Spark, self).__init__(config)

        # Remove the value of 'npartitions' from config dict
        self.npartitions = config.pop('npartitions', None)

        sparkConf = SparkConf().setAll(config.items())
        self.sparkContext = SparkContext.getOrCreate(sparkConf)

        # Set the value of 'npartitions' if it doesn't exist
        self.npartitions = self.npartitions or self.sparkContext.getConf().get('spark.executor.instances') or 2

        # getConf().get('spark.executor.instances') could return a string
        self.npartitions = int(self.npartitions)

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
        ranges = self.BuildRanges(self.nentries, self.npartitions) # Get range pairs
        # Build parallel collection
        parallel_collection = self.sparkContext.parallelize(ranges, self.npartitions)

        return parallel_collection.map(mapper).treeReduce(reducer) # Map-Reduce using Spark