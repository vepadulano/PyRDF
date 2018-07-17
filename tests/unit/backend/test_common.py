import PyRDF, unittest, ROOT

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
