import PyRDF
from PyRDF import Proxy, Local, Dist
import unittest

class SelectionTest(unittest.TestCase):
    """
    A series of tests to check the
    accuracy of 'PyRDF.use' method.

    """

    def test_local_select(self):
        """
        Test to check if 'local'
        environment gets set correctly.

        """

        PyRDF.use("local")
        self.assertIsInstance(Proxy.backend, Local)

    def test_dist_select(self):
        """
        Test to check if 'spark'
        environment gets set correctly.

        """

        PyRDF.use("spark")
        self.assertIsInstance(Proxy.backend, Dist)

    def test_future_env_select(self):
        """
        Test to check if a future environment
        throws a NotImplementedError.

        """

        with self.assertRaises(NotImplementedError):
            PyRDF.use("dask")
