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