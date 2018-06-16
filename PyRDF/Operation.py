from __future__ import print_function
from enum import Enum

class Operation(object):
    """
    A Generic representation
    of an operation such as 
    transformations and actions

    """

    Types = Enum("Types", "ACTION TRANSFORMATION")

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.op_type = self._classify_operation(name)

    def _classify_operation(self, name):
        """
        Method to classify the given
        operation as action or 
        transformation
        """

        future_ops = [
        "Range",
        "Take",
        "Snapshot",
        "Foreach"
        ]

        if name in future_ops:
            raise NotImplementedError("This operation ({}) isn't supported yet ! ".format(name))

        ops = Operation.Types

        operations_dict = {
        'Define':ops.TRANSFORMATION,
        'Filter':ops.TRANSFORMATION,
        'Histo1D':ops.ACTION,
        'Histo2D':ops.ACTION,
        'Histo3D':ops.ACTION,
        'Profile1D':ops.ACTION,
        'Profile2D':ops.ACTION,
        'Profile3D':ops.ACTION,
        'Count':ops.ACTION,
        'Min':ops.ACTION,
        'Max':ops.ACTION,
        'Mean':ops.ACTION,
        'Sum':ops.ACTION,
        'Fill':ops.ACTION,
        'Report':ops.ACTION
        }
        
        op_type = operations_dict.get(name)

        if not op_type:
            raise Exception("Invalid operation \"{}\"".format(name))
        return op_type
