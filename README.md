MRToolbox
=========

Export feature classes from ArcMap to Hadoop, run MapReduce jobs and import result back into ArcMap as features classes.

A use case will be something like the following, a [GeoEventProcessor](http://www.esri.com/esri-news/arcnews/spring13articles/arcgis-enables-real-time-gis) is streaming data
points into HDFS or into [S3](http://aws.amazon.com/s3/).  A Geo-Data Scientist that is using ArcMap has a set of polygons that needs data aggregation from that streaming data.
He can launch a [Hadoop cluster from ArcMap](http://thunderheadxpler.blogspot.com/2013/05/bigdata-launch-cdh-on-ec2-from-arcmap.html), export the polygons, run a MapReduce job
that points to the streaming data as input for spatial analysis.  The result is joined back to the input polygons for symbolization and visualization.

## GIS Tools for Hadoop

This project depends on the [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java) and borrows code from the [Spatial Framework for Hadoop](https://github.com/Esri/spatial-framework-for-hadoop).

You must first [git clone](http://gitref.org/creating/#clone) and compile the Esri Geometry API.

    $ git clone https://github.com/Esri/geometry-api-java.git
    $ cd geometry-api-java
    $ mvn install

## Compiling and packaging

Make sure to install **arcobjects.jar** in your local [maven repo](http://maven.apache.org/guides/introduction/introduction-to-repositories.html). You can typically find it in C:\Program Files (x86)\ArcGIS\Desktop10.1\java\lib.

    $ mvn install:install-file -Dfile=arcobjects.jar -DgroupId=com.esri -DartifactId=arcobjects -Dversion=10.1 -Dpackaging=jar -DgeneratePom=true

clone and package:

    $ mvn clean package

## Installing the extension in ArcMap
Copy from the **target** folder the **MRToolbox-1.1-SNAPSHOT.jar** file and the **libs** folder into the C:\Program Files (x86)\ArcGIS\Desktop10.1\java\lib\ext folder.

Before starting ArcMap, you have to adjust the ArcGIS JVM Heap values. Run as **administrator** JavaConfigTool located in C:\Program Files (x86)\ArcGIS\Desktop10.1\bin

![JavaConfigTool](https://dl.dropboxusercontent.com/u/2193160/JavaConfigTool.png)

Check out [this](http://help.arcgis.com/en/arcgisdesktop/10.0/help/index.html#/A_quick_tour_of_managing_tools_and_toolboxes/003q00000001000000/) to see how to add a Toolbox and a Tool to ArcMap.

Start ArcMap. Create a toolbox named *MRToolbox*, and add to it the ExportToHDFSTool and the JobRunnerTool. You should have something like the following:

![MRToolbox](https://dl.dropboxusercontent.com/u/2193160/MRToolbox.png "MR Toolbox")

## ExportToHDFSTool
![ExportToHDFSTool](https://dl.dropboxusercontent.com/u/2193160/ExportToHDFSTool.png "Export To HDFS Tool")

This GP tool exports a feature class from ArcMap into a Hadoop File System path in [Esri JSON format](http://help.arcgis.com/EN/arcgisserver/10.0/apis/rest/index.html).

Here is a sample hadoop.properties file content:

    hadoop.job.ugi=root,root
    fs.default.name=hdfs\://ec2-xx-xx-xx-xx.compute-1.amazonaws.com\:8020/
    mapred.job.tracker=ec2-xx-xx-xx-xx.compute-1.amazonaws.com\:8021
    hadoop.socks.server=localhost\:6666
    fs.s3n.awsAccessKeyId=my-access-key-id
    fs.s3n.awsSecretAccessKey=my-secret-access-key
    fs.s3.awsAccessKeyId=my-access-key-id
    fs.s3.awsSecretAccessKey=my-secret-access-key
    hadoop.rpc.socket.factory.class.default=org.apache.hadoop.net.SocksSocketFactory
    dfs.client.use.legacy.blockreader=true

## JobRunnerTool
![JobRunnerTool](https://dl.dropboxusercontent.com/u/2193160/JobRunnerTool.png "Job Runnner Tool")

This GP tool runs a map reduce job.  It performs a spatial join between a very large set of points and a set of exported polygons.
The result of the job is a table *in ArcMap* where each row has two fields.  The first field is the polygon name identifier, and the second field is the number of points in that polygon.
