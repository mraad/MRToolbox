package com.esri;

import com.esri.arcgis.geodatabase.Feature;
import com.esri.arcgis.geodatabase.FeatureClass;
import com.esri.arcgis.geodatabase.IFeature;
import com.esri.arcgis.geodatabase.IFeatureClass;
import com.esri.arcgis.geodatabase.IFeatureClassProxy;
import com.esri.arcgis.geodatabase.IFeatureCursor;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geodatabase.IGPValue;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.Cleaner;
import com.esri.arcgis.server.json.JSONObject;
import com.esri.arcgis.system.Array;
import com.esri.arcgis.system.IArray;
import com.esri.arcgis.system.ServerUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 */
public class ExportToHDFSTool extends AbstractTool
{
    public final static String NAME = ExportToHDFSTool.class.getSimpleName();

    @Override
    protected void doExecute(
            final IArray parameters,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws Exception
    {
        final IGPValue hadoopConfValue = gpUtilities.unpackGPValue(parameters.getElement(0));
        final IGPValue hadoopUserValue = gpUtilities.unpackGPValue(parameters.getElement(1));
        final IGPValue featureClassValue = gpUtilities.unpackGPValue(parameters.getElement(2));
        final IGPValue outputValue = gpUtilities.unpackGPValue(parameters.getElement(3));

        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserValue.getAsText());
        final int count = ugi.doAs(new PrivilegedExceptionAction<Integer>()
        {
            public Integer run() throws Exception
            {
                return doExport(hadoopConfValue, featureClassValue, outputValue);
            }
        });
        messages.addMessage(String.format("Exported %d features.", count));
    }

    private Configuration createConfiguration(
            final String propertiesPath) throws IOException
    {
        final Configuration configuration = new Configuration();
        configuration.setClassLoader(ClassLoader.getSystemClassLoader());
        loadProperties(configuration, propertiesPath);
        return configuration;
    }

    private int doExport(
            final IGPValue hadoopPropValue,
            final IGPValue featureClassValue,
            final IGPValue outputValue) throws Exception
    {
        final IFeatureClass[] featureClasses = new IFeatureClass[]{new IFeatureClassProxy()};
        gpUtilities.decodeFeatureLayer(featureClassValue, featureClasses, null);
        final FeatureClass featureClass = new FeatureClass(featureClasses[0]);

        final Configuration configuration = createConfiguration(hadoopPropValue.getAsText());
        final Path path = new Path(outputValue.getAsText());
        final FileSystem fileSystem = path.getFileSystem(configuration);
        int count;
        try
        {
            if (fileSystem.exists(path))
            {
                fileSystem.delete(path, true);
            }
            count = 0;
            final FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
            try
            {
                final IFeatureCursor cursor = featureClass.search(null, false);
                try
                {
                    IFeature feature = cursor.nextFeature();
                    try
                    {
                        while (feature != null)
                        {
                            final JSONObject jsonObject = ServerUtilities.getJSONFromFeature((Feature) feature);
                            fsDataOutputStream.writeBytes(jsonObject.toString() + "\n"); // TODO - use jsonObject.write
                            feature = cursor.nextFeature();
                            count++;
                        }
                    }
                    finally
                    {
                        Cleaner.release(feature);
                    }
                }
                finally
                {
                    Cleaner.release(cursor);
                }
            }
            finally
            {
                IOUtils.closeStream(fsDataOutputStream);
            }
        }
        finally
        {
            fileSystem.close();
        }
        Cleaner.release(featureClass);
        return count;
    }


    @Override
    public IArray getParameterInfo() throws IOException, AutomationException
    {
        final String prefix = System.getProperty("user.home") + File.separator;
        final String username = System.getProperty("user.name");

        final IArray parameters = new Array();

        addParamFile(parameters, "Hadoop properties file", "in_hadoop_prop", prefix + "hadoop.properties");
        addParamString(parameters, "Hadoop user", "in_hadoop_user", username);
        addParamFeatureLayer(parameters, "Input features", "in_features");
        addParamString(parameters, "Remote output path", "in_output_path", "/user/" + username + "/world.json");

        return parameters;
    }

    @Override
    public String getName() throws IOException, AutomationException
    {
        return NAME;
    }

    @Override
    public String getDisplayName() throws IOException, AutomationException
    {
        return NAME;
    }
}
