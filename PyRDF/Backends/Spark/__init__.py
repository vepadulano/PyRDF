import PyRDF


def make_spark_dataframe(*args, **kwargs):
    """
    Create a SparkDataFrame object
    """

    from PyRDF.Backends.Spark import Backend
    sparkcontext = kwargs.get("sparkcontext", None)
    spark = Backend.SparkBackend(sparkcontext=sparkcontext)

    return spark.make_dataframe(*args, **kwargs)


PyRDF.make_spark_dataframe = make_spark_dataframe
