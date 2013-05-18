package com.esri;

import com.esri.arcgis.datasourcesGDB.FileGDBWorkspaceFactory;
import com.esri.arcgis.datasourcesfile.ShapefileWorkspaceFactory;
import com.esri.arcgis.geodatabase.DETable;
import com.esri.arcgis.geodatabase.DETableType;
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
import com.esri.arcgis.geoprocessing.GPParameter;
import com.esri.arcgis.geoprocessing.GPTableSchema;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.geoprocessing.esriGPParameterDirection;
import com.esri.arcgis.geoprocessing.esriGPParameterType;
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
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.security.PrivilegedExceptionAction;
import java.util.regex.Pattern;

/**
 */
public class ImportFromHDFSTool extends AbstractTool
{
    public final static String NAME = ImportFromHDFSTool.class.getSimpleName();

    @Override
    protected void doExecute(
            final IArray parameters,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws Exception
    {
        final IGPValue hadoopConfValue = gpUtilities.unpackGPValue(parameters.getElement(0));
        final IGPValue hadoopUserValue = gpUtilities.unpackGPValue(parameters.getElement(1));
        final IGPValue hadoopPathValue = gpUtilities.unpackGPValue(parameters.getElement(2));
        final IGPValue outputValue = gpUtilities.unpackGPValue(parameters.getElement(3));

        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserValue.getAsText());
        final int count = ugi.doAs(new PrivilegedExceptionAction<Integer>()
        {
            public Integer run() throws Exception
            {
                return doImport(hadoopConfValue, hadoopPathValue, outputValue, messages, environmentManager);
            }
        });
        messages.addMessage(String.format("Imported %d features.", count));
    }

    private Configuration createConfiguration(
            final String propertiesPath) throws IOException
    {
        final Configuration configuration = new Configuration();
        configuration.setClassLoader(ClassLoader.getSystemClassLoader());
        loadProperties(configuration, propertiesPath);
        return configuration;
    }

    private int doImport(
            final IGPValue configValue,
            final IGPValue inputValue,
            final IGPValue tableValue,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws Exception
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
        fieldName.setLength(32);
        fieldName.setType(EsriFieldType.esriFieldTypeString.ordinal());
        fields.addField(fieldName);

        final Field fieldNume = new Field();
        fieldNume.setName("NUME");
        fieldNume.setAliasName("NUME");
        fieldNume.setLength(8);
        fieldNume.setType(EsriFieldType.esriFieldTypeInteger.ordinal());
        fields.addField(fieldNume);

        int count = 0;
        final Table table = createTable(tableValue.getAsText(), fields, messages, environmentManager);
        if (table != null)
        {
            try
            {
                count = importFromHDFS(table, configValue.getAsText(), inputValue.getAsText(), messages);
            }
            finally
            {
                Cleaner.release(table);
            }
        }
        return count;
    }

    private int importFromHDFS(
            final Table table,
            final String configPath,
            final String inputPath,
            final IGPMessages messages
    ) throws Exception
    {
        final Configuration configuration = createConfiguration(configPath);
        int count = 0;
        boolean commit = false;
        final Workspace workspace = new Workspace(table.getWorkspace());
        try
        {
            workspace.startEditing(false);
            workspace.startEditOperation();
            try
            {
                final Path path = new Path(inputPath);
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
            final Path path) throws Exception
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
            String tablePath,
            IFields fields,
            IGPMessages messages,
            IGPEnvironmentManager environmentManager) throws Exception
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

    @Override
    public IArray getParameterInfo() throws IOException, AutomationException
    {
        final String prefix = System.getProperty("user.home") + File.separator;
        final String username = System.getProperty("user.name");

        final IArray parameters = new Array();

        addParamFile(parameters, "Hadoop properties file", "in_hadoop_prop", prefix + "hadoop.properties");
        addParamString(parameters, "Hadoop user", "in_hadoop_user", username);
        addParamString(parameters, "Remote input path", "in_path", "/user/" + username + "/output");
        addParamTable(parameters, "Output table", "out_table", "C:\\temp\\table.dbf");

        return parameters;
    }

    private void addParamTable(
            final IArray parameters,
            final String displayName,
            final String name,
            final String value) throws IOException
    {
        final GPTableSchema tableSchema = new GPTableSchema();
        tableSchema.setCloneDependency(true);

        final GPParameter parameterOut = new GPParameter();
        parameterOut.setDirection(esriGPParameterDirection.esriGPParameterDirectionOutput);
        parameterOut.setDisplayName(displayName);
        parameterOut.setName(name);
        parameterOut.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameterOut.setDataTypeByRef(new DETableType());
        final DETable table = new DETable();
        table.setAsText(value);
        parameterOut.setValueByRef(table);
        parameterOut.setSchemaByRef(tableSchema);

        parameters.add(parameterOut);
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
