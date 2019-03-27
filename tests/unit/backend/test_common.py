import unittest
import ROOT
import PyRDF
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
        """Resets the module-level variable 'includes' to an empty list."""
        PyRDF.includes = []  # reset includes

    def test_default_empty_list_include(self):
        """
        'PyRDF.include' function raises a TypeError if no parameter is
        given.

        """
        with self.assertRaises(TypeError):
            PyRDF.include()

    def test_string_include(self):
        """'PyRDF.include' with a single string."""
        PyRDF.include("header1")

        self.assertListEqual(PyRDF.includes, ["header1"])

    def test_list_include(self):
        """'PyRDF.include' with a list of strings."""
        PyRDF.include(["header1"])

        self.assertListEqual(PyRDF.includes, ["header1"])

    def test_list_extend_include(self):
        """
        Test case to check the working of 'PyRDF.include'
        function when different lists of strings are passed
        to it multiple times.

        """
        PyRDF.include(["header1", "header2"])
        PyRDF.include(["header3", "header4", "header5"])

        required_list = ["header1", "header2", "header3", "header4", "header5"]
        self.assertListEqual(PyRDF.includes, required_list)


class DeclareHeadersTest(unittest.TestCase):
    """Static method 'declare_headers' in Backend class."""

    def test_single_header_declare(self):
        """'declare_headers' with a single header to be included."""
        Utils.declare_headers(["tests/unit/backend/test_headers/header1.hxx"])

        self.assertEqual(ROOT.f(1), True)

    def test_multiple_headers_declare(self):
        """'declare_headers' with multiple headers to be included."""
        Utils.declare_headers(["tests/unit/backend/test_headers/header1.hxx",
                               "tests/unit/backend/test_headers/header2.hxx"])

        self.assertEqual(ROOT.f(1), True)
        self.assertEqual(ROOT.f1(2), 2)
        self.assertEqual(ROOT.f2("myString"), "myString")

    def test_header_declaration_on_current_session(self):
        """Header has to be declared on the current session"""
        # Before the header declaration the function f is not present on the
        # ROOT interpreter
        with self.assertRaises(AttributeError):
            self.assertRaises(ROOT.f(1))
        PyRDF.include("tests/unit/backend/test_headers/header1.hxx")
        self.assertEqual(ROOT.f(1), True)


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
