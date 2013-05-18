package com.esri;

import com.esri.arcgis.datasourcesGDB.FileGDBWorkspaceFactory;
import com.esri.arcgis.datasourcesfile.DEFile;
import com.esri.arcgis.datasourcesfile.ShapefileWorkspaceFactory;
import com.esri.arcgis.geodatabase.Field;
import com.esri.arcgis.geodatabase.Fields;
import com.esri.arcgis.geodatabase.IFeatureWorkspace;
import com.esri.arcgis.geodatabase.IFields;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geodatabase.IGPValue;
import com.esri.arcgis.geodatabase.IRow;
import com.esri.arcgis.geodatabase.IWorkspaceFactory;
import com.esri.arcgis.geodatabase.Table;
import com.esri.arcgis.geodatabase.Workspace;
import com.esri.arcgis.geoprocessing.GPString;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.geoprocessing.IGPParameter;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.Cleaner;
import com.esri.arcgis.system.Array;
import com.esri.arcgis.system.IArray;
import com.esri.json.EsriFieldType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.security.PrivilegedExceptionAction;
import java.util.regex.Pattern;

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
        final IGPValue tableValue = gpUtilities.unpackGPValue(parameters.getElement(6));

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
                if (success)
                {
                    createTable(messages, environmentManager, configuration, outputPath, tableValue);
                }
                else
                {
                    messages.addAbort("Check logs for JobTracker " + job.getJobID().getJtIdentifier());
                }
                return null;
            }
        });
    }

    private void createTable(
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager,
            final Configuration configuration,
            final Path outputPath,
            final IGPValue tableValue) throws IOException
    {
        if (gpUtilities.exists(tableValue))
        {
            messages.addMessage("Output already exists. Overwriting it...");
            gpUtilities.delete(tableValue);
        }

        final Fields fields = new Fields();

        final Field fieldName = new Field();
        fieldName.setName("NAME");
        fieldName.setAliasName("NAME");
        fieldName.setLength(64);
        fieldName.setType(EsriFieldType.esriFieldTypeString.ordinal());
        fields.addField(fieldName);

        final Field fieldNume = new Field();
        fieldNume.setName("NUME");
        fieldNume.setAliasName("NUME");
        fieldNume.setLength(8);
        fieldNume.setType(EsriFieldType.esriFieldTypeInteger.ordinal());
        fields.addField(fieldNume);

        final Table table = createTable(messages, environmentManager, tableValue.getAsText(), fields);
        if (table != null)
        {
            try
            {
                final int count = importFromHDFS(messages, configuration, outputPath, table);
                messages.addMessage(String.format("Wrote %d rows", count));
            }
            finally
            {
                Cleaner.release(table);
            }
        }
    }

    private int importFromHDFS(
            final IGPMessages messages,
            final Configuration configuration,
            final Path path,
            final Table table
    ) throws IOException
    {
        int count = 0;
        boolean commit = false;
        final Workspace workspace = new Workspace(table.getWorkspace());
        try
        {
            workspace.startEditing(false);
            workspace.startEditOperation();
            try
            {
                final FileSystem fileSystem = path.getFileSystem(configuration);
                final FileStatus fileStatus = fileSystem.getFileStatus(path);
                if (fileStatus.isDirectory())
                {
                    for (final FileStatus childStatus : fileSystem.listStatus(path))
                    {
                        final Path childPath = childStatus.getPath();
                        if (!childPath.getName().startsWith("_"))
                        {
                            messages.addMessage(childPath.getName());
                            count += storeFeatures(table, fileSystem, childPath);
                        }
                    }
                }
                else
                {
                    count += storeFeatures(table, fileSystem, path);
                }
                commit = true;
            }
            finally
            {
                if (workspace.isBeingEdited())
                {
                    workspace.stopEditOperation();
                    workspace.stopEditing(commit);
                }
            }
        }
        finally
        {
            Cleaner.release(workspace);
        }
        return count;
    }

    private int storeFeatures(
            final Table table,
            final FileSystem fileSystem,
            final Path path) throws IOException
    {
        final int name = table.findField("NAME");
        final int nume = table.findField("NUME");
        int count = 0;
        IRow row = null;
        try
        {
            final FSDataInputStream dataInputStream = fileSystem.open(path);
            try
            {
                final Pattern pattern = Pattern.compile("\t");
                final LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(dataInputStream));
                for (String line = lineNumberReader.readLine(); line != null; line = lineNumberReader.readLine())
                {
                    final String[] tokens = pattern.split(line);
                    row = table.createRow();
                    row.setValue(name, tokens[0]);
                    row.setValue(nume, Integer.parseInt(tokens[1]));
                    row.store();
                    count++;
                }
            }
            finally
            {
                dataInputStream.close();
            }
        }
        finally
        {
            Cleaner.release(row);
        }
        return count;
    }

    private Table createTable(
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager,
            final String tablePath,
            final IFields fields) throws IOException
    {
        final File outputFCFile = new File(tablePath);
        File outputWSFile = outputFCFile.getParentFile();

        if (outputWSFile == null)
        {
            final String scratchWorkspace = environmentManager.findEnvironment("scratchWorkspace").getValue().getAsText();
            if (scratchWorkspace != null && !scratchWorkspace.isEmpty())
            {
                outputWSFile = new File(scratchWorkspace);
            }
            else
            {
                messages.addError(500, "No output folder or scratch workspace specified.");
                return null;
            }
        }

        String outputWSName = outputWSFile.getName();
        IWorkspaceFactory workspaceFactory = null;
        IFeatureWorkspace featureWorkspace = null;

        if (outputWSFile.exists() && outputWSFile.isDirectory())
        {
            // Now that output location is known, determine type of workspace, FileGDB or shapefile. SDE is not supported.
            if (outputWSName.endsWith(".gdb"))
            {
                workspaceFactory = new FileGDBWorkspaceFactory();
                outputWSName = outputWSName.substring(0, outputWSName.indexOf(".gdb"));
            }
            else
            {
                workspaceFactory = new ShapefileWorkspaceFactory();
            }
            // Create the output workspace if it's not created yet. If the output file gdb exists, then workspace exists too.
            if (!workspaceFactory.isWorkspace(outputWSFile.getAbsolutePath()))
            {
                workspaceFactory.create(outputWSFile.getParentFile().getAbsolutePath(), outputWSName, null, 0);
            }
            // Retrieve the output workspace
            featureWorkspace = new Workspace(workspaceFactory.openFromFile(outputWSFile.getAbsolutePath(), 0));
        }
        else
        {
            messages.addError(500, "Output or scratch workspace directory does not exist.");
            return null;
        }

        return new Table(featureWorkspace.createTable(outputFCFile.getName(), fields, null, null, "default"));
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
        final String username = System.getProperty("user.name");
        final String userhome = System.getProperty("user.home") + File.separator;

        final IArray parameters = new Array();

        addParamFile(parameters, "Hadoop properties file", "in_hadoop_properties", userhome + "hadoop.properties");
        addParamString(parameters, "Hadoop user", "in_user", username);
        addParamString(parameters, "Remote input path", "in_input", "s3n://" + username + "/earthquakes.csv");
        addParamString(parameters, "Remote join path", "in_join", "/user/" + username + "/features.json");
        addParamString(parameters, "Remote output path", "in_output", "/user/" + username + "/output");
        addParamString(parameters, "libjars", "in_libjars", "C:\\Program Files (x86)\\ArcGIS\\Desktop10.1\\java\\lib\\ext\\libs\\esri-geometry-api-1.1-SNAPSHOT.jar");
        addParamTable(parameters, "Output table", "out_table", userhome + "table.dbf");

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
