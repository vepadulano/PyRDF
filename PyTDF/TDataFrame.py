from __future__ import print_function
from .Node import Node
import ROOT

class TDataFrame(Node):
    
    """
    The Python equivalent
    of ROOT C++'s TDataFrame 
    data structure

    """

    def __init__(self, treename, filelist):
        # TODO (shravan97) : Implement different types of constructors

        super(TDataFrame, self).__init__(None, None)
        self.filelist = filelist
        self.treename = treename

    def create_tdf(self):
        """
        Method to create a TDataFrame object

        """

        self.Tchain = ROOT.TChain(self.treename)

        for f in self.filelist:
            self.Tchain.Add(f)

        self._tdf = ROOT.Experimental.TDataFrame(self.Tchain)