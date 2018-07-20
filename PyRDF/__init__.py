from .RDataFrame import RDataFrame, RDataFrameException
from .CallableGenerator import CallableGenerator
from .backend import Local, Spark

current_backend = Local()

def use(backend_name, conf = {}):

    future_backends = [
    "dask"
    ]

    global current_backend

    if backend_name in future_backends:
        raise NotImplementedError(" This backend environment will be considered in the future !")
    elif backend_name == "local":
        current_backend = Local(conf)
    elif backend_name == "spark":
        current_backend = Spark(conf)
    else:
        raise Exception(" Incorrect backend environment \"{}\"".format(backend))