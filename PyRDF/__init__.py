from .RDataFrame import RDataFrame
from .Node import Node
from .Proxy import Proxy
from .Operation import Operation
from .CallableGenerator import CallableGenerator
from .backend import Local, Dist

Proxy.backend = Local({})

def use(backend, conf = {}):

    future_backends = [
    "dask"
    ]

    if backend in future_backends:
        raise NotImplementedError(" This backend environment will be considered in the future !")
    elif backend == "local":
        Proxy.backend = Local(conf)
    elif backend == "spark":
        Proxy.backend = Dist(conf)
    else:
        raise Exception(" Incorrect backend environment \"{}\"".format(backend))