import PyRDF, unittest, ROOT
from PyRDF.backend.Utils import Utils

class SelectionTest(unittest.TestCase):
    """
    A series of tests to check the
    accuracy of 'PyRDF.use' method.

    """

    def test_future_env_select(self):
        """
        Test to check if a future environment
        throws a NotImplementedError.

        """

        with self.assertRaises(NotImplementedError):
            PyRDF.use("dask")

class BackendInitTest(unittest.TestCase):
    """
    Tests to ensure that the Backend abstract
    class cannot be instantiated.

    """
    def test_backend_init_error(self):
        """
        Test case to check that any attempt to
        instantiate the `Backend` abstract class
        results in a `TypeError`.

        """
        with self.assertRaises(TypeError):
            PyRDF.backend.Backend.Backend()

    def test_subclass_without_method_error(self):
        """
        Test case to check that creation of a
        subclass without implementing `execute`
        method throws a `TypeError`.

        """
        class TestBackend(PyRDF.backend.Backend.Backend):
            pass

        with self.assertRaises(TypeError):
            TestBackend()

class IncludeHeadersTest(unittest.TestCase):
    """
    Tests to check the working of 'PyRDF.include' function.

    """
    def tearDown(self):
        """
        Resets the module-level variable
        'includes' to an empty list.

        """
        PyRDF.includes = [] # reset includes

    def test_default_empty_list_include(self):
        """
        Test case to ensure that 'PyRDF.include' function
        raises a TypeError if no parameter is given.

        """
        with self.assertRaises(TypeError):
            PyRDF.include()

    def test_string_include(self):
        """
        Test case to check the working of 'PyRDF.include'
        function when a single string is passed to it.

        """
        PyRDF.include("header1")

        self.assertListEqual(PyRDF.includes, ["header1"])

    def test_list_include(self):
        """
        Test case to check the working of 'PyRDF.include'
        function when a list of strings is passed to it.

        """
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

        self.assertListEqual(PyRDF.includes, ["header1", "header2", "header3", "header4", "header5"])

class DeclareHeadersTest(unittest.TestCase):
    """
    Tests to check the working of the static
    method 'declare_headers' in Backend class.

    """
    def test_single_header_declare(self):
        """
        Test case to check the working of 'declare_headers'
        function when a single header needs to be included.

        """
        Utils.declare_headers(["tests/unit/backend/test_headers/header1.hxx"])

        self.assertEqual(ROOT.f(1), True)

    def test_multiple_headers_declare(self):
        """
        Test case to check the working of 'declare_headers'
        function when multiple headers need to be included.

        """
        Utils.declare_headers(["tests/unit/backend/test_headers/header1.hxx",
                               "tests/unit/backend/test_headers/header2.hxx"
                              ])

        self.assertEqual(ROOT.f(1), True)
        self.assertEqual(ROOT.f1(2), 2)
        self.assertEqual(ROOT.f2("myString"), "myString")
