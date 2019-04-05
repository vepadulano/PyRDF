# PyRDF Demos

The following demos are available in SWAN:

| Title  | Content  | Link to SWAN |
|--------|----------|--------------|
| [ROOT df102 Tutorial](df102_NanoAODDimuonAnalysis.ipynb) |  NanoAOD Dimuon Analysis |  <a href="https://cern.ch/swanserver/cgi-bin/go?projurl=https://raw.githubusercontent.com/JavierCVilla/PyRDF/new-demo/demos/df102_NanoAODDimuonAnalysis.ipynb" target="_blank"><img src="http://swanserver.web.cern.ch/swanserver/images/badge_swan_white_150.png" alt="Open in SWAN" style="height:1.3em"></a> |
| [RDF Demo](RDF_demo.ipynb) | Dimuon invariant mass spectrum plot | <a href="https://cern.ch/swanserver/cgi-bin/go?projurl=https://raw.githubusercontent.com/JavierCVilla/PyRDF/new-demo/demos/RDF_demo.ipynb" target="_blank"><img src="http://swanserver.web.cern.ch/swanserver/images/badge_swan_white_150.png" alt="Open in SWAN" style="height:1.3em"></a> |

## How to use PyRDF with Spark in SWAN

**Disclaimer**: This process will be much simpler once we integrate PyRDF as part of the LCG Releases. Our goal is to add the next [0.1.0 release version](https://github.com/JavierCVilla/PyRDF/projects/2).

#### 1. Enter to [SWAN](swan.cern.ch)

#### 2. Select the *Cloud Containers* Spark cluster in your configuration and the *Bleeding Edge* Software stack.

<p align="center"><img src ="images/swan-tutorial-0.png" /></p>

#### 3. Open a terminal in SWAN:

  ![](images/swan-tutorial-1.png)

#### 4. Go to your project folder:

  ```
  cd SWAN_projects/<Project_PATH>
  ```

#### 5. Clone the PyRDF repository from Github:

  ```
  git clone https://github.com/JavierCVilla/PyRDF
  ```

#### 6. Since PyRDF is not distributed yet through CVMFS, we need to send it to our remote workers. The next command creates a zip file with the content of the module:

  ```
  python PyRDF/demos/swan-setup.py
  ```

#### 7. Go back to your notebook and open the menu to connect to a Spark cluster:

 ![](images/swan-tutorial-2.png)

#### 8. Before starting the connection, let's add some configuration to the SparkContext:

  ```
  {
    "name": "spark.executor.extraLibraryPath",
    "value": "{LD_LIBRARY_PATH},PyRDF"
  },
  {
    "name": "spark.yarn.dist.archives",
    "value": "PyRDF.zip#PyRDF"
  }
  ```

  This can be added using the SWAN interface as follows:

  - Enter the name of the parameter on the _Add new option_ field:

      <p align="center"><img src ="images/swan-tutorial-3.png" /></p>

  - And add the value:

      <p align="center"><img src ="images/swan-tutorial-4.png" /></p>

  - Follow the same steps for the second parameter:

      <p align="center"><img src ="images/swan-tutorial-5.png" /></p>

      <p align="center"><img src ="images/swan-tutorial-6.png" /></p>

  - Once both parameters have been configured, the menu should look like this:

      <p align="center"><img src ="images/swan-tutorial-7.png" /></p>

#### 9. Now we are ready to connect to the cluster.

#### 10. Select the Spark backend in PyRDF, by default PyRDF will use the `Local` backend which is equivalent to RDataFrame running in a local machine.

  ```
  PyRDF.use("spark", {'npartitions': '32'})
  ```

  The second parameter of `PyRDF.use` allows us to add some configuration to the Spark context such as the number of partitions, modify this number to suit your needs.

#### 11. Have a look at the [demos](#pyrdf-demos) to see examples of use.
