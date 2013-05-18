package com.esri;

import com.esri.arcgis.datasourcesfile.DEFile;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geoprocessing.GPString;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.geoprocessing.IGPParameter;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.system.Array;
import com.esri.arcgis.system.IArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 */
public class JobRunnerTool extends AbstractTool
{
    public final static String NAME = JobRunnerTool.class.getSimpleName();

    @Override
    protected void doExecute(
            final IArray parameters,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws Exception
    {
        final DEFile configFile = (DEFile) (((IGPParameter) (parameters.getElement(0))).getValue());
        final GPString userString = (GPString) (((IGPParameter) (parameters.getElement(1))).getValue());
        final GPString inputString = (GPString) (((IGPParameter) (parameters.getElement(2))).getValue());
        final GPString joinString = (GPString) (((IGPParameter) (parameters.getElement(3))).getValue());
        final GPString outputString = (GPString) (((IGPParameter) (parameters.getElement(4))).getValue());
        final GPString libjarsString = (GPString) (((IGPParameter) (parameters.getElement(5))).getValue());

        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userString.getAsText());
        ugi.doAs(new PrivilegedExceptionAction<Void>()
        {
            public Void run() throws Exception
            {
                final String catalogPath = configFile.getCatalogPath();
                final Configuration configuration = createConfiguration(catalogPath, libjarsString.getAsText());
                configuration.set("jobrunner.featurePath", joinString.getAsText());

                final Job job = Job.getInstance(configuration);

                job.setJobName(NAME);
                job.setJarByClass(JobRunnerTool.class);

                job.setMapperClass(JobMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);

                job.setCombinerClass(JobReducer.class);

                job.setReducerClass(JobReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                FileInputFormat.addInputPath(job, new Path(inputString.getAsText()));
                final Path outputPath = new Path(outputString.getAsText());
                FileOutputFormat.setOutputPath(job, outputPath);

                final FileSystem fileSystem = outputPath.getFileSystem(configuration);
                if (fileSystem.exists(outputPath))
                {
                    fileSystem.delete(outputPath, true);
                }

                final boolean success = job.waitForCompletion(true);
                if (!success)
                {
                    messages.addAbort("Check logs for JobTracker " + job.getJobID().getJtIdentifier());
                }
                return null;
            }
        });
    }

    private Configuration createConfiguration(
            final String propertiesPath,
            final String libjars) throws IOException
    {
        final String[] args = new String[]{"-libjars", toURI(libjars)};
        final GenericOptionsParser genericOptionsParser = new GenericOptionsParser(args);
        final Configuration configuration = genericOptionsParser.getConfiguration();
        loadProperties(configuration, propertiesPath);
        return configuration;
    }

    private String toURI(final String libjars)
    {
        final String[] tokens = libjars.split(",");
        int count = tokens.length;
        final StringBuilder stringBuilder = new StringBuilder();
        for (final String token : tokens)
        {
            final File file = new File(token);
            stringBuilder.append(file.toURI().toString());
            if (--count > 0)
            {
                stringBuilder.append(",");
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public IArray getParameterInfo() throws IOException, AutomationException
    {
        final String prefix = System.getProperty("user.home") + File.separator;
        final String username = System.getProperty("user.name");

        final IArray parameters = new Array();

        addParamFile(parameters, "Hadoop properties file", "in_hadoop_properties", prefix + "hadoop.properties");
        addParamString(parameters, "Hadoop user", "in_user", username);
        addParamString(parameters, "Remote input path", "in_input", "s3n://" + username + "/earthquakes.csv");
        addParamString(parameters, "Remote join path", "in_join", "/user/" + username + "/world.json");
        addParamString(parameters, "Remote output path", "in_output", "/user/" + username + "/output");
        addParamString(parameters, "libjars", "in_libjars", "C:\\Program Files (x86)\\ArcGIS\\Desktop10.1\\java\\lib\\ext\\libs\\esri-geometry-api-1.1-SNAPSHOT.jar");

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
