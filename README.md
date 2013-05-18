MRToolbox
=========

Export / import feature classes from ArcMap to Hadoop and run map reduce jobs

Make sure to first install in your local maven repo arcobjects.jar. You can get a copy from **Your ArcGIS Desktop Folder**\java\lib folder

    $ mvn install:install-file -Dfile=arcobjects.jar -DgroupId=com.esri -DartifactId=arcobjects -Dversion=10.1 -Dpackaging=jar -DgeneratePom=true

## GIS Tools for Hadoop

This project depends on the [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java) and borrows code from the [Spatial Framework for Hadoop](https://github.com/Esri/spatial-framework-for-hadoop).

You must first clone and compile the Esri Geometry API

    $ git clone https://github.com/Esri/geometry-api-java.git
    $ cd geometry-api-java
    $ mvn install

## Compiling and packaging
    $ mvn clean package

## Installing the extension in ArcMap
Copy from the **target** folder the file **MRToolbox-1.1-SNAPSHOT.jar** and the folder **libs** into **Your ArcGIS Desktop Folder**\java\lib\ext.

Check out [this](http://help.arcgis.com/en/arcgisdesktop/10.0/help/index.html#/A_quick_tour_of_managing_tools_and_toolboxes/003q00000001000000/) to see how to add a Toolbox and a Tool to ArcMap.

You should have something like the following:

![MRToolbox](https://dl.dropboxusercontent.com/u/2193160/MRToolbox.png "MR Toolbox")

## ExportToHDFSTool
This GP tool exports a feature class from ArcMap into a Hadoop File System path in Esri JSON format.

![ExportToHDFSTool](https://dl.dropboxusercontent.com/u/2193160/ExportToHDFSTool.png "Export To HDFS Tool")

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

