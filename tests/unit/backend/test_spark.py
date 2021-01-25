import unittest
import PyRDF
from PyRDF.Backends.Spark import Backend
import pyspark


class SparkBackendInitTest(unittest.TestCase):
    """
    Tests to ensure that the instance variables
    of `Spark` class are set according to the
    input `config` dict.
    """

    @classmethod
    def tearDown(cls):
        """Clean up the `SparkContext` objects that were created."""
        pyspark.SparkContext.getOrCreate().stop()

    def test_set_spark_context_default(self):
        """
        Check that a `SparkContext` object is created with default options for
        the current system.
        """
        backend = Backend.SparkBackend()

        self.assertIsInstance(backend.sc, pyspark.SparkContext)

    def test_set_spark_context_with_conf(self):
        """
        Check that a `SparkContext` object is correctly created for a given
        `SparkConf` object in the config dictionary.
        """
        conf = {"spark.app.name": "my-pyspark-app1"}
        sconf = pyspark.SparkConf().setAll(conf.items())
        sc = pyspark.SparkContext(conf=sconf)

        backend = Backend.SparkBackend(sparkcontext=sc)

        self.assertIsInstance(backend.sc, pyspark.SparkContext)
        appname = backend.sc.getConf().get("spark.app.name")
        self.assertEqual(appname, "my-pyspark-app1")

    def test_npartitions_with_num_executors(self):
        """
        Check that the number of partitions is correctly set to number of
        executors when no input value is given in the config dictionary.
        """
        conf = {'spark.executor.instances': 10}
        sconf = pyspark.SparkConf().setAll(conf.items())
        sc = pyspark.SparkContext(conf=sconf)
        backend = Backend.SparkBackend(sparkcontext=sc)

        self.assertEqual(backend.npartitions, 10)

    def test_npartitions_with_already_existing_spark_context(self):
        """
        Check that the number of partitions is correctly set when a Spark
        Context already exists.
        """
        sparkconf = pyspark.SparkConf().set('spark.executor.instances', 15)
        pyspark.SparkContext(conf=sparkconf)
        backend = Backend.SparkBackend()
        self.assertEqual(backend.npartitions, 15)

    def test_npartitions_default(self):
        """
        Check that the default number of partitions is correctly set when no
        input value is given in the config dictionary.
        """
        backend = Backend.SparkBackend()
        self.assertEqual(backend.npartitions,
                         Backend.SparkBackend.MIN_NPARTITIONS)


class OperationSupportTest(unittest.TestCase):
    """
    Ensure that incoming operations are classified accurately in distributed
    environment.
    """

    @classmethod
    def tearDown(cls):
        """Clean up the `SparkContext` objects that were created."""
        pyspark.SparkContext.getOrCreate().stop()

    def test_action(self):
        """Check that action nodes are classified accurately."""
        backend = Backend.SparkBackend()
        backend.check_supported("Histo1D")

    def test_transformation(self):
        """Check that transformation nodes are classified accurately."""
        backend = Backend.SparkBackend()
        backend.check_supported("Define")

    def test_unsupported_operations(self):
        """Check that unsupported operations raise an Exception."""
        backend = Backend.SparkBackend()
        with self.assertRaises(Exception):
            backend.check_supported("Take")

        with self.assertRaises(Exception):
            backend.check_supported("Foreach")

        with self.assertRaises(Exception):
            backend.check_supported("Range")

    def test_none(self):
        """Check that incorrect operations raise an Exception."""
        backend = Backend.SparkBackend()
        with self.assertRaises(Exception):
            backend.check_supported("random")

    def test_range_operation_single_thread(self):
        """
        Check that 'Range' operation works in single-threaded mode and raises an
        Exception in multi-threaded mode.
        """
        backend = Backend.SparkBackend()
        with self.assertRaises(Exception):
            backend.check_supported("Range")


class InitializationTest(unittest.TestCase):
    """Check initialization method in the Spark backend"""

    def test_initialization_method(self):
        """
        Check initialization method in Spark backend.
        Define a method in the ROOT interpreter called getValue which returns
        the value defined by the user on the python side.
        """
        def init(value):
            import ROOT
            cpp_code = '''int userValue = %s ;''' % value
            ROOT.gInterpreter.ProcessLine(cpp_code)

        PyRDF.initialize(init, 123)
        # Spark backend has a limited list of supported methods, so we use
        # Histo1D which is a supported action.
        # The code below creates an RDataFrame instance with one single entry
        # and defines a column 'u' whose value is taken from the variable
        # 'userValue'.
        # This variable is only declared inside the ROOT interpreter, however
        # the value of the variable is passed by the user from the python side.
        # If the init function defined by the user is properly propagated to the
        # Spark backend, each workers will run the init function as a first step
        # and hence the variable 'userValue' will be defined at runtime.
        # As a result the define operation should read the variable 'userValue'
        # and assign it to the entries of the column 'u' (only one entry).
        # Finally, Histo1D returns a histogram filled with one value. The mean
        # of this single value has to be the value itself, independently of
        # the number of spawned workers.
        df = PyRDF.make_spark_dataframe(1).Define(
            "u", "userValue").Histo1D("u")
        h = df.GetValue()
        self.assertEqual(h.GetMean(), 123)


class EmptyTreeErrorTest(unittest.TestCase):
    """
    Distributed execution fails when the tree has no entries.
    """

    def test_histo_from_empty_root_file(self):
        """
        Check that when performing operations with the distributed backend on
        an RDataFrame without entries, PyRDF raises an error.
        """

        # Create an RDataFrame from a file with an empty tree
        rdf = PyRDF.make_spark_dataframe(
            "NOMINAL", "tests/unit/backend/emptytree.root")
        histo = rdf.Histo1D(("empty", "empty", 10, 0, 10), "mybranch")

        # Get entries in the histogram, raises error
        with self.assertRaises(RuntimeError):
            histo.GetEntries()


class ChangeAttributeTest(unittest.TestCase):
    """Tests that check correct changes in the class attributes"""

    def test_change_attribute_when_npartitions_greater_than_clusters(self):
        """
        Check that the `npartitions class attribute is changed when it is
        greater than the number of clusters in the ROOT file.
        """

        treename = "TotemNtuple"
        filelist = ["tests/unit/backend/Slimmed_ntuple.root"]
        df = PyRDF.make_spark_dataframe(treename, filelist, npartitions=10)

        self.assertEqual(df._headnode.backend.npartitions, 10)
        histo = df.Histo1D("track_rp_3.x")
        nentries = histo.GetEntries()

        self.assertEqual(nentries, 10)
        self.assertEqual(df._headnode.backend.npartitions, 1)


if __name__ == "__main__":
    unittest.main()
