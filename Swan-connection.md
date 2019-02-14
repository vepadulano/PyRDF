## SWAN Setup

Clone repo:

```
git clone https://github.com/JavierCvilla/PyRDF.git
```

Compress on zip format the `PyRDF/PyRDF` folder (the inner folder)

in python:

```
import os
import shutil

shutil.make_archive('PyRDF', 'zip', os.path.abspath('PyRDF'))
```



Start notebook and connect to spark with the followign

## Spark options

Since `PyRDF` is not yet part of any CVMFS release, it is not present on the Spark workers at
run time. Thus we need to provide it by sending the module to each worker prior to start the execution.

```
"sparkconnect": {
    "list_of_options": [
      {
        "name": "spark.executor.extraLibraryPath",
        "value": "{LD_LIBRARY_PATH},PyRDF"
      },
      {
        "name": "spark.yarn.dist.archives",
        "value": "../PyRDF.zip#PyRDF"
      }
    ],
    "bundled_options": []
  }
```
