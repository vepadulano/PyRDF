import unittest, ROOT
from PyRDF import RDataFrame, RDataFrameException

class RDataFrameConstructorTests(unittest.TestCase):

    def test_integer_arg(self):
        RDF = RDataFrame(10)

        self.assertListEqual(RDF.args, [10])

    def test_two_args(self):
        rdf_2_files = ["file1.root", "file2.root"]

        reqd_vec = ROOT.std.vector('string')()
        for elem in rdf_2_files:
            reqd_vec.push_back(elem)

        RDF_1 = RDataFrame("treename", "file.root")
        RDF_2 = RDataFrame("treename", rdf_2_files)
        RDF_3 = RDataFrame("treename", reqd_vec)

        self.assertListEqual(RDF_1.args, ["treename", "file.root"])
        self.assertListEqual(RDF_2.args, ["treename", reqd_vec])
        self.assertListEqual(RDF_3.args, ["treename", reqd_vec])

    def test_three_args_with_single_file(self):
        rdf_branches = ["branch1", "branch2"]

        reqd_vec = ROOT.std.vector('string')()
        for elem in rdf_branches:
            reqd_vec.push_back(elem)

        RDF_1 = RDataFrame("treename", "file.root", rdf_branches)
        RDF_2 = RDataFrame("treename", "file.root", reqd_vec)

        self.assertListEqual(RDF_1.args, ["treename", "file.root", reqd_vec])
        self.assertListEqual(RDF_2.args, ["treename", "file.root", reqd_vec])

    def test_three_args_with_multiple_files(self):
        rdf_branches = ["branch1", "branch2"]
        rdf_files = ["file1.root", "file2.root"]

        reqd_files_vec = ROOT.std.vector('string')()
        for elem in rdf_files:
            reqd_files_vec.push_back(elem)

        reqd_branches_vec = ROOT.std.vector('string')()
        for elem in rdf_branches:
            reqd_branches_vec.push_back(elem)

        RDF_1 = RDataFrame("treename", rdf_files, rdf_branches)
        RDF_2 = RDataFrame("treename", rdf_files, reqd_branches_vec)
        RDF_3 = RDataFrame("treename", reqd_files_vec, rdf_branches)
        RDF_4 = RDataFrame("treename", reqd_files_vec, reqd_branches_vec)

        reqd_args_list = ["treename", reqd_files_vec, reqd_branches_vec]
        self.assertListEqual(RDF_1.args, reqd_args_list)
        self.assertListEqual(RDF_2.args, reqd_args_list)
        self.assertListEqual(RDF_3.args, reqd_args_list)
        self.assertListEqual(RDF_4.args, reqd_args_list)

    def test_incorrect_args(self):

        with self.assertRaises(RDataFrameException):
            RDF = RDataFrame(10, "file.root")

        with self.assertRaises(RDataFrameException):
            RDF = RDataFrame("treename", "file.root", "column1")

        with self.assertRaises(RDataFrameException):
            RDF = RDataFrame()
