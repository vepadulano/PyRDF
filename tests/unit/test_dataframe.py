import unittest

import pyspark
from PyRDF import DataFrame
from PyRDF.Backends.Spark import Backend


class IncorrectArgsTest(unittest.TestCase):
    """Test early check in DistDataFrame"""

    @classmethod
    def tearDownClass(cls):
        """Stop SparkContext"""
        pyspark.SparkContext.getOrCreate().stop()

    def test_incorrect_args(self):
        """Constructor with incorrect arguments"""
        with self.assertRaises(TypeError):
            # Incorrect first argument in 2-argument case
            back = Backend.SparkBackend()
            DataFrame.DistDataFrame(back, 10, "file.root")

        with self.assertRaises(TypeError):
            # Incorrect third argument in 3-argument case
            back = Backend.SparkBackend()
            DataFrame.DistDataFrame(back, "treename", "file.root", "column1")

        with self.assertRaises(TypeError):
            # No argument case
            back = Backend.SparkBackend()
            DataFrame.DistDataFrame(back)
