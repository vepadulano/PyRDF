import array
import os
import unittest

import PyRDF
import pyspark
import ROOT
from PyRDF.backend.Utils import Utils


class SelectionTest(unittest.TestCase):
    """Check 'PyRDF.use' method."""

    def test_future_env_select(self):
        """Non implemented backends throw a NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            PyRDF.use("dask")


class BackendInitTest(unittest.TestCase):
    """Backend abstract class cannot be instantiated."""

    def test_backend_init_error(self):
        """
        Any attempt to instantiate the `Backend` abstract class results in
        a `TypeError`.

        """
        with self.assertRaises(TypeError):
            PyRDF.backend.Backend.Backend()

    def test_subclass_without_method_error(self):
        """
        Creation of a subclass without implementing `execute` method throws
        a `TypeError`.

        """
        class TestBackend(PyRDF.backend.Backend.Backend):
            pass

        with self.assertRaises(TypeError):
            TestBackend()


class IncludeHeadersTest(unittest.TestCase):
    """Tests to check the working of 'PyRDF.include' function."""

    def tearDown(self):
        """remove included headers after analysis"""
        PyRDF.includes_headers.clear()

    def test_default_empty_list_include(self):
        """
        'PyRDF.include' function raises a TypeError if no parameter is
        given.

        """
        with self.assertRaises(TypeError):
            PyRDF.include_headers()

    def test_string_include(self):
        """'PyRDF.include' with a single string."""
        PyRDF.include_headers("tests/unit/backend/test_headers/header1.hxx")

        required_header = ["tests/unit/backend/test_headers/header1.hxx"]
        # Feature detection: first try Python 3 function, then Python 2
        try:
            self.assertCountEqual(PyRDF.includes_headers, required_header)
        except AttributeError:
            self.assertItemsEqual(PyRDF.includes_headers, required_header)

    def test_list_include(self):
        """'PyRDF.include' with a list of strings."""
        PyRDF.include_headers(["tests/unit/backend/test_headers/header1.hxx"])

        required_header = ["tests/unit/backend/test_headers/header1.hxx"]
        # Feature detection: first try Python 3 function, then Python 2
        try:
            self.assertCountEqual(PyRDF.includes_headers, required_header)
        except AttributeError:
            self.assertItemsEqual(PyRDF.includes_headers, required_header)

    def test_list_extend_include(self):
        """
        Test case to check the working of 'PyRDF.include'
        function when different lists of strings are passed
        to it multiple times.

        """
        PyRDF.include_headers(["tests/unit/backend/test_headers/header1.hxx"])
        PyRDF.include_headers(["tests/unit/backend/test_headers/header2.hxx"])

        required_list = ["tests/unit/backend/test_headers/header1.hxx",
                         "tests/unit/backend/test_headers/header2.hxx"]
        # Feature detection: first try Python 3 function, then Python 2
        try:
            self.assertCountEqual(PyRDF.includes_headers, required_list)
        except AttributeError:
            self.assertItemsEqual(PyRDF.includes_headers, required_list)


class DeclareHeadersTest(unittest.TestCase):
    """Static method 'declare_headers' in Backend class."""

    def tearDown(self):
        """remove included headers after analysis"""
        PyRDF.includes_headers.clear()

    def test_single_header_declare(self):
        """'declare_headers' with a single header to be included."""
        Utils.declare_headers(["tests/unit/backend/test_headers/header1.hxx"])

        self.assertEqual(ROOT.f(1), True)

    def test_multiple_headers_declare(self):
        """'declare_headers' with multiple headers to be included."""
        Utils.declare_headers(["tests/unit/backend/test_headers/header2.hxx",
                               "tests/unit/backend/test_headers/header3.hxx"])

        self.assertEqual(ROOT.a(1), True)
        self.assertEqual(ROOT.f1(2), 2)
        self.assertEqual(ROOT.f2("myString"), "myString")

    def test_header_declaration_on_current_session(self):
        """Header has to be declared on the current session"""
        # Before the header declaration the function f is not present on the
        # ROOT interpreter
        with self.assertRaises(AttributeError):
            self.assertRaises(ROOT.b(1))
        PyRDF.include_headers("tests/unit/backend/test_headers/header4.hxx")
        self.assertEqual(ROOT.b(1), True)


class InitializationTest(unittest.TestCase):
    """Check the initialize method"""

    def test_initialization(self):
        """
        Check that the user initialization method is assigned to the current
        backend.

        """
        def returnNumber(n):
            return n

        PyRDF.initialize(returnNumber, 123)
        f = PyRDF.current_backend.initialization
        self.assertEqual(f(), 123)

    def test_initialization_runs_in_current_environment(self):
        """
        User initialization method should be executed on the current user
        session, so actions applied by the user initialization function are
        also visible in the current scenario.
        """
        def defineIntVariable(name, value):
            import ROOT
            ROOT.gInterpreter.ProcessLine("int %s = %s;" % (name, value))

        varvalue = 2
        PyRDF.initialize(defineIntVariable, "myInt", varvalue)
        self.assertEqual(ROOT.myInt, varvalue)


class RunGraphsTest(unittest.TestCase):
    """Tests for the concurrent submission of distributed jobs in PyRDF"""

    def tearDown(self):
        """Clean up the `SparkContext` object that was created."""
        pyspark.SparkContext.getOrCreate().stop()

    def ttree_write(self, treename, filename, mean, std_dev):
        """Create a TTree and write it to file."""
        f = ROOT.TFile(filename, "recreate")
        t = ROOT.TTree(treename, "ConcurrentSparkJobsTest")

        x = array.array("f", [0])
        t.Branch("x", x, "x/F")

        r = ROOT.TRandom()
        # Fill the branch with a gaussian distribution
        for _ in range(10000):
            x[0] = r.Gaus(mean, std_dev)
            t.Fill()

        f.Write()
        f.Close()

    def test_rungraphs_local(self):
        """Test RunGraphs with Local backend"""
        PyRDF.use("local")

        counts = [PyRDF.RDataFrame(10).Count() for _ in range(4)]

        Utils.RunGraphs(counts)

        for count in counts:
            self.assertEqual(count.GetValue(), 10)

    def test_rungraphs_spark_3histos(self):
        """
        Create three datasets to run some simple analysis on, then submit them
        concurrently as Spark jobs from different threads.
        """
        PyRDF.use("spark")

        treenames = ["tree{}".format(i) for i in range(1, 4)]
        filenames = ["file{}.root".format(i) for i in range(1, 4)]
        means = [10, 20, 30]
        std_devs = [1, 2, 3]

        for treename, filename, mean, std_dev in zip(
                treenames, filenames, means, std_devs):
            self.ttree_write(treename, filename, mean, std_dev)

        histoproxies = [PyRDF.RDataFrame(treename, filename)
                             .Histo1D(("x", "x", 100, 0, 50), "x")
                        for treename, filename in zip(treenames, filenames)]

        Utils.RunGraphs(histoproxies)

        delta_equal = 0.1

        for histo, mean, std_dev in zip(histoproxies, means, std_devs):
            self.assertEqual(histo.GetEntries(), 10000)
            self.assertAlmostEqual(histo.GetMean(), mean, delta=delta_equal)
            self.assertAlmostEqual(
                histo.GetStdDev(), std_dev, delta=delta_equal)

        for filename in filenames:
            os.remove(filename)
