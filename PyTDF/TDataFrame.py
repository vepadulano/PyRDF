from __future__ import print_function
from .Node import Node
import ROOT

class TDataFrame(Node):
    
    """
    The Python equivalent
    of ROOT C++'s TDataFrame 
    data structure

    """

    def __init__(self, treename, filelist, npartitions=None):
        # TODO (shravan97) : Implement different types of constructors

        super(TDataFrame, self).__init__(None, None)
        self.filelist = filelist
        self.treename = treename
        self.npartitions = npartitions
        self.chain = ROOT.TChain(treename)
        self._tdf = self.get_tdf_temp()
        self.value = self._tdf

    def get_tdf_temp(self):
        # TODO (shravan97) : shift this to mapper generator
        # Temporary function to get 
        # TDF object until mapper is done

        for f in self.filelist:
            self.chain.Add(f)

        return ROOT.Experimental.TDataFrame(self.chain)