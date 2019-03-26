import unittest
import ROOT
import PyRDF


class ReducerMergeTest(unittest.TestCase):
    """Check the working of merge operations in the reducer function."""

    @classmethod
    def setUpClass(cls):
        """Select Spark backend before running all the tests."""
        PyRDF.use("spark", {'npartitions': 2, 'spark.executor.instances': 2})

    @classmethod
    def tearDownClass(cls):
        """
        Restore global current_backend to default Local backend after running
        all tests

        """
        PyRDF.use("local")

    def assertHistoOrProfile(self, obj_1, obj_2):
        """Asserts equality between two 'ROOT.TH1' or 'ROOT.TH2' objects."""
        # Compare the sizes of equivalent objects
        self.assertEqual(obj_1.GetEntries(), obj_2.GetEntries())

        # Compare the means of equivalent objects
        self.assertEqual(obj_1.GetMean(), obj_2.GetMean())

        # Compare the standard deviations of equivalent objects
        self.assertEqual(obj_1.GetStdDev(), obj_2.GetStdDev())

    def define_two_columns(self, rdf):
        """
        Helper method that Defines and returns two columns with definitions
        "x = rdfentry_" and "y = rdfentry_ * rdfentry_".

        """
        return rdf.Define("x", "rdfentry_").Define("y", "rdfentry_*rdfentry_")

    def define_three_columns(self, rdf):
        """
        Helper method that Defines and returns three columns with definitions
        "x = rdfentry_", "y = rdfentry_ * rdfentry_" and
        "z = rdfentry_ * rdfentry_ * rdfentry_".

        """
        return rdf.Define("x", "rdfentry_")\
                  .Define("y", "rdfentry_*rdfentry_")\
                  .Define("z", "rdfentry_*rdfentry_*rdfentry_")

    def test_histo1d_merge(self):
        """Check the working of Histo1D merge operation in the reducer."""
        # Operations with PyRDF
        rdf_py = PyRDF.RDataFrame(10)
        histo_py = rdf_py.Histo1D("rdfentry_")

        # Operations with PyROOT
        rdf_cpp = ROOT.ROOT.RDataFrame(10)
        histo_cpp = rdf_cpp.Histo1D("rdfentry_")

        # Compare the 2 histograms
        self.assertHistoOrProfile(histo_py, histo_cpp)

    def test_histo2d_merge(self):
        """Check the working of Histo2D merge operation in the reducer."""
        modelTH2D = ("", "", 64, -4, 4, 64, -4, 4)

        # Operations with PyRDF
        rdf_py = PyRDF.RDataFrame(10)
        columns_py = self.define_two_columns(rdf_py)
        histo_py = columns_py.Histo2D(modelTH2D, "x", "y")

        # Operations with PyROOT
        rdf_cpp = ROOT.ROOT.RDataFrame(10)
        columns_cpp = self.define_two_columns(rdf_cpp)
        histo_cpp = columns_cpp.Histo2D(modelTH2D, "x", "y")

        # Compare the 2 histograms
        self.assertHistoOrProfile(histo_py, histo_cpp)

    def test_histo3d_merge(self):
        """Check the working of Histo3D merge operation in the reducer."""
        modelTH3D = ("", "", 64, -4, 4, 64, -4, 4, 64, -4, 4)
        # Operations with PyRDF
        rdf_py = PyRDF.RDataFrame(10)
        columns_py = self.define_three_columns(rdf_py)
        histo_py = columns_py.Histo3D(modelTH3D, "x", "y", "z")

        # Operations with PyROOT
        rdf_cpp = ROOT.ROOT.RDataFrame(10)
        columns_cpp = self.define_three_columns(rdf_cpp)
        histo_cpp = columns_cpp.Histo3D(modelTH3D, "x", "y", "z")

        # Compare the 2 histograms
        self.assertHistoOrProfile(histo_py, histo_cpp)

    def test_profile1d_merge(self):
        """Check the working of Profile1D merge operation in the reducer."""
        # Operations with PyRDF
        rdf_py = PyRDF.RDataFrame(10)
        columns_py = self.define_two_columns(rdf_py)
        profile_py = columns_py.Profile1D(("", "", 64, -4, 4), "x", "y")

        # Operations with PyROOT
        rdf_cpp = ROOT.ROOT.RDataFrame(10)
        columns_cpp = self.define_two_columns(rdf_cpp)
        profile_cpp = columns_cpp.Profile1D(("", "", 64, -4, 4), "x", "y")

        # Compare the 2 profiles
        self.assertHistoOrProfile(profile_py, profile_cpp)

    def test_profile2d_merge(self):
        """Check the working of Profile2D merge operation in the reducer."""
        model = ("", "", 64, -4, 4, 64, -4, 4)

        # Operations with PyRDF
        rdf_py = PyRDF.RDataFrame(10)
        columns_py = self.define_three_columns(rdf_py)
        profile_py = columns_py.Profile2D(model, "x", "y", "z")

        # Operations with PyROOT
        rdf_cpp = ROOT.ROOT.RDataFrame(10)
        columns_cpp = self.define_three_columns(rdf_cpp)
        profile_cpp = columns_cpp.Profile2D(model, "x", "y", "z")

        # Compare the 2 profiles
        self.assertHistoOrProfile(profile_py, profile_cpp)

    @unittest.skipIf(ROOT.gROOT.GetVersion() < '6.16',
                     "Graph featured included in ROOT-6.16 for the first time")
    def test_tgraph_merge(self):
        """Check the working of TGraph merge operation in the reducer."""
        # Operations with PyRDF
        rdf_py = PyRDF.RDataFrame(10)
        columns_py = self.define_two_columns(rdf_py)
        graph_py = columns_py.Graph("x", "y")

        # Operations with PyROOT
        rdf_cpp = ROOT.ROOT.RDataFrame(10)
        columns_cpp = self.define_two_columns(rdf_cpp)
        graph_cpp = columns_cpp.Graph("x", "y")

        # Sort the graphs to make sure corresponding points are same
        graph_py.Sort()
        graph_cpp.Sort()

        # Compare the X co-ordinates of the graphs
        self.assertListEqual(list(graph_py.GetX()), list(graph_cpp.GetX()))

        # Compare the Y co-ordinates of the graphs
        self.assertListEqual(list(graph_py.GetY()), list(graph_cpp.GetY()))
