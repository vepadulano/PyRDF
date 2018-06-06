import PyRDF
from PyRDF import Proxy, Local, Dist
import unittest

class SelectionTest(unittest.TestCase):
    def test_local_select(self):
        PyRDF.use("local")
        self.assertIsInstance(Proxy.backend, Local)

    def test_dist_select(self):
        PyRDF.use("spark")
        self.assertIsInstance(Proxy.backend, Dist)

    def test_future_env_select(self):
        with self.assertRaises(NotImplementedError):
            PyRDF.use("dask")

    def test_local_select(self):
        PyRDF.use("local")
        self.assertIsInstance(Proxy.backend, Local)
