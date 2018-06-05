from __future__ import print_function
from .Node import Node

class RDataFrame(Node):
    
    """
    The Python equivalent
    of ROOT C++'s RDataFrame 
    data structure

    """

    def __init__(self, treename, filelist):
        # TODO (shravan97) : Implement different types of constructors

        super(RDataFrame, self).__init__(None, None)
        self.filelist = filelist
        self.treename = treename