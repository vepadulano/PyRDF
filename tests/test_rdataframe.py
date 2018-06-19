import unittest, ROOT
from PyRDF import RDataFrame, RDataFrameException

class RDataFrameConstructorTests(unittest.TestCase):
    """
    Tests to check the working of
    various constructors of PyRDF's
    RDataFrame

    """

    def test_integer_arg(self):
        """
        Test case to check the
        working of a single
        integer argument
        (one argument constructor)

        """
        RDF = RDataFrame(10)

        self.assertListEqual(RDF.args, [10])

    def test_two_args(self):
        """
        Test cases to check the
        working of the two
        argument contructor

        """
        rdf_2_files = ["file1.root", "file2.root"]

        # Convert RDF files list to ROOT CPP vector
        reqd_vec = ROOT.std.vector('string')()
        for elem in rdf_2_files:
            reqd_vec.push_back(elem)

        # RDataFrame constructor with 2nd argument as string
        RDF_1 = RDataFrame("treename", "file.root")

        # RDataFrame constructor with 2nd argument as Python list
        RDF_2 = RDataFrame("treename", rdf_2_files)

        # RDataFrame constructor with 2nd argument as ROOT CPP Vector
        RDF_3 = RDataFrame("treename", reqd_vec)

        self.assertListEqual(RDF_1.args, ["treename", "file.root"])
        self.assertListEqual(RDF_2.args, ["treename", reqd_vec])
        self.assertListEqual(RDF_3.args, ["treename", reqd_vec])

    def test_three_args_with_single_file(self):
        """
        Test cases to check the
        working of the three
        argument contructor
        with the second argument
        as a string

        """
        rdf_branches = ["branch1", "branch2"]

        # Convert RDF branches list to ROOT CPP Vector
        reqd_vec = ROOT.std.vector('string')()
        for elem in rdf_branches:
            reqd_vec.push_back(elem)

        # RDataFrame constructor with 3rd argument as Python list
        RDF_1 = RDataFrame("treename", "file.root", rdf_branches)

        # RDataFrame constructor with 3rd argument as ROOT CPP Vector
        RDF_2 = RDataFrame("treename", "file.root", reqd_vec)

        self.assertListEqual(RDF_1.args, ["treename", "file.root", reqd_vec])
        self.assertListEqual(RDF_2.args, ["treename", "file.root", reqd_vec])

    def test_three_args_with_multiple_files(self):
        """
        Test cases to check the
        working of the three
        argument contructor
        with the second argument
        as a List or a Vector
        of strings

        """
        rdf_branches = ["branch1", "branch2"]
        rdf_files = ["file1.root", "file2.root"]

        # Convert RDF files list to ROOT CPP Vector
        reqd_files_vec = ROOT.std.vector('string')()
        for elem in rdf_files:
            reqd_files_vec.push_back(elem)

        # Convert RDF files list to ROOT CPP Vector
        reqd_branches_vec = ROOT.std.vector('string')()
        for elem in rdf_branches:
            reqd_branches_vec.push_back(elem)

        # RDataFrame constructor with 2nd argument as Python List
        # and 3rd argument as Python List
        RDF_1 = RDataFrame("treename", rdf_files, rdf_branches)

        # RDataFrame constructor with 2nd argument as Python List
        # and 3rd argument as ROOT CPP Vector
        RDF_2 = RDataFrame("treename", rdf_files, reqd_branches_vec)

        # RDataFrame constructor with 2nd argument as ROOT CPP Vector
        # and 3rd argument as Python List
        RDF_3 = RDataFrame("treename", reqd_files_vec, rdf_branches)

        # RDataFrame constructor with 2nd and 3rd arguments as ROOT
        # CPP Vectors
        RDF_4 = RDataFrame("treename", reqd_files_vec, reqd_branches_vec)

        reqd_args_list = ["treename", reqd_files_vec, reqd_branches_vec]
        self.assertListEqual(RDF_1.args, reqd_args_list)
        self.assertListEqual(RDF_2.args, reqd_args_list)
        self.assertListEqual(RDF_3.args, reqd_args_list)
        self.assertListEqual(RDF_4.args, reqd_args_list)

    def test_incorrect_args(self):
        """
        Test cases to check the
        exceptions raised for
        incorrect arguments

        """
        with self.assertRaises(RDataFrameException):
            # Incorrect first argument in 2-argument case
            RDF = RDataFrame(10, "file.root")

        with self.assertRaises(RDataFrameException):
            # Incorrect third argument in 3-argument case
            RDF = RDataFrame("treename", "file.root", "column1")

        with self.assertRaises(RDataFrameException):
            # No argument case
            RDF = RDataFrame()
