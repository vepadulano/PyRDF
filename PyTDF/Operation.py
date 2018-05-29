from __future__ import print_function
from enum import Enum

class Operation(object):
    """
    A Generic representation
    of an operation such as 
    transformations and actions

    """

    TYPES = Enum("TYPES", "ACTION TRANSFORMATION")

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.op_type = self._classify_operation(name)

    def _classify_operation(self, name):
        if name.lower() in ("range", "take"):
            raise NotImplementedError("This operation ({}) isn't supported yet ! ".format(name))

        operations_dict = {
        'Define':Operation.TYPES.TRANSFORMATION,
        'Filter':Operation.TYPES.TRANSFORMATION,
        'Histo1D':Operation.TYPES.ACTION,
        'Histo2D':Operation.TYPES.ACTION,
        'Histo3D':Operation.TYPES.ACTION,
        'Profile1D':Operation.TYPES.ACTION,
        'Profile2D':Operation.TYPES.ACTION,
        'Profile3D':Operation.TYPES.ACTION,
        'Count':Operation.TYPES.ACTION,
        'Min':Operation.TYPES.ACTION,
        'Max':Operation.TYPES.ACTION,
        'Mean':Operation.TYPES.ACTION,
        'Sum':Operation.TYPES.ACTION,
        'Fill':Operation.TYPES.ACTION,
        }
        
        op_type = operations_dict.get(name)

        if not op_type:
            raise Exception("Invalid operation \"{}\"".format(name))
        return op_type
