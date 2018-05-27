from __future__ import print_function
from enum import Enum

class OpTypes(Enum):
    ACTION = 1
    TRANSFORMATION = 2

class Operation(object):
    
    """
    A Generic representation
    of an operation such as 
    transformations and actions

    """

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.op_type = self._classify_operation(name)

    def _classify_operation(self, name):
        if name.lower() in ("range", "take"):
            raise NotImplementedError("This operation ({}) isn't supported yet ! ".format(name))

        operations_dict = {
        'Define':OpTypes.TRANSFORMATION,
        'Filter':OpTypes.TRANSFORMATION,
        'Histo1D':OpTypes.ACTION,
        'Histo2D':OpTypes.ACTION,
        'Histo3D':OpTypes.ACTION,
        'Profile1D':OpTypes.ACTION,
        'Profile2D':OpTypes.ACTION,
        'Profile3D':OpTypes.ACTION,
        'Count':OpTypes.ACTION,
        'Min':OpTypes.ACTION,
        'Max':OpTypes.ACTION,
        'Mean':OpTypes.ACTION,
        'Sum':OpTypes.ACTION,
        'Fill':OpTypes.ACTION,
        }
        
        op_type = operations_dict.get(name)

        if not op_type:
            raise Exception("Invalid operation \"{}\"".format(name))
        return op_type
