import unittest
from collections import namedtuple

import PyRDF
import pyspark
import ROOT


class SparkHistogramsTest(unittest.TestCase):
    """Integration tests to check the working of PyRDF."""

    def build_pyrdf_graph(self):
        """Create a PyRDF graph with a fixed set of operations and return it."""
        treename = "data"
        files = ["https://root.cern/files/teaching/CMS_Open_Dataset.root", ]
        rdf = PyRDF.make_spark_dataframe(treename, files, npartitions=5)

        # Define the analysis cuts
        chargeCutStr = "C1 != C2"
        etaCutStr = "fabs(eta1) < 2.3 && fabs(eta2) < 2.3"
        ptCutStr = "pt1 > 2 && pt2 > 2"
        rdf_f = rdf.Filter(chargeCutStr, "Opposite Charge") \
                   .Filter(etaCutStr, "Central Muons") \
                   .Filter(ptCutStr, "Sane Pt")

        # Create the invariant mass column
        invMassFormulaStr = ("sqrt(pow(E1+E2, 2) - (pow(px1+px2, 2) +"
                             "pow(py1+py2, 2) + pow(pz1+pz2, 2)))")
        rdf_fd = rdf_f.Define("invMass", invMassFormulaStr)

        # Create the histograms
        pt1_h = rdf.Histo1D(("", "", 128, 1, 1200), "pt1")
        pt2_h = rdf.Histo1D(("", "", 128, 1, 1200), "pt2")
        model = ("invMass", "CMS Opendata;#mu#mu mass[GeV];Events", 512, 5, 110)
        invMass_h = rdf_fd.Histo1D(model, "invMass")
        pi = ROOT.TMath.Pi()
        model = ("", "", 64, -pi, pi, 64, -pi, pi)
        phis_h = rdf_fd.Histo2D(model, "phi1", "phi2")

        return pt1_h, pt2_h, invMass_h, phis_h

    def build_rootrdf_graph(self):
        """Create an RDF graph with a fixed set of operations and return it."""
        treename = "data"
        files = ["https://root.cern/files/teaching/CMS_Open_Dataset.root", ]
        rdf = ROOT.RDataFrame(treename, files)

        # Define the analysis cuts
        chargeCutStr = "C1 != C2"
        etaCutStr = "fabs(eta1) < 2.3 && fabs(eta2) < 2.3"
        ptCutStr = "pt1 > 2 && pt2 > 2"
        rdf_f = rdf.Filter(chargeCutStr, "Opposite Charge") \
                   .Filter(etaCutStr, "Central Muons") \
                   .Filter(ptCutStr, "Sane Pt")

        # Create the invariant mass column
        invMassFormulaStr = ("sqrt(pow(E1+E2, 2) - (pow(px1+px2, 2) +"
                             "pow(py1+py2, 2) + pow(pz1+pz2, 2)))")
        rdf_fd = rdf_f.Define("invMass", invMassFormulaStr)

        # Create the histograms
        pt1_h = rdf.Histo1D(("", "", 128, 1, 1200), "pt1")
        pt2_h = rdf.Histo1D(("", "", 128, 1, 1200), "pt2")
        model = ("invMass", "CMS Opendata;#mu#mu mass[GeV];Events", 512, 5, 110)
        invMass_h = rdf_fd.Histo1D(model, "invMass")
        pi = ROOT.TMath.Pi()
        model = ("", "", 64, -pi, pi, 64, -pi, pi)
        phis_h = rdf_fd.Histo2D(model, "phi1", "phi2")

        return pt1_h, pt2_h, invMass_h, phis_h

    def test_spark_histograms(self):
        """Check that Spark backend works the same way as ROOT RDF."""
        physics_variables = ["pt1_h", "pt2_h", "invMass_h", "phis_h"]

        SparkResult = namedtuple("SparkResult", physics_variables)
        spark = SparkResult(*self.build_pyrdf_graph())

        spark.pt1_h.Draw("PL PLC PMC")  # Trigger Event-loop, Spark

        # ROOT RDataFrame execution
        RDFResult = namedtuple("RDFResult", physics_variables)
        rootrdf = RDFResult(*self.build_rootrdf_graph())

        rootrdf.pt1_h.Draw("PL PLC PMC")  # Trigger Event-loop, RDF-only

        # Assert 'pt1_h' histogram
        self.assertEqual(spark.pt1_h.GetEntries(), rootrdf.pt1_h.GetEntries())
        # Assert 'pt2_h' histogram
        self.assertEqual(spark.pt2_h.GetEntries(), rootrdf.pt2_h.GetEntries())
        # Assert 'invMass_h' histogram
        self.assertEqual(spark.invMass_h.GetEntries(),
                         rootrdf.invMass_h.GetEntries())
        # Assert 'phis_h' histogram
        self.assertEqual(spark.phis_h.GetEntries(), rootrdf.phis_h.GetEntries())

        # Stop the SparkContext before exiting
        pyspark.SparkContext.getOrCreate().stop()


if __name__ == "__main__":
    unittest.main()
