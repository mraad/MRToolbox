package com.esri;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeature;
import com.esri.json.EsriObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 */
public class JobMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private final IntWritable ONE = new IntWritable(1);

    private final Pattern m_pattern = Pattern.compile(",");

    private List<EsriFeature> m_featureList = new ArrayList<EsriFeature>();
    private SpatialReference m_spatialReference;
    private String m_attribute;
    private int m_latIndex;
    private int m_lonIndex;
    private QuadTree m_quadTree;
    private QuadTree.QuadTreeIterator m_quadTreeIterator;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        final Configuration configuration = context.getConfiguration();

        m_spatialReference = SpatialReference.create(configuration.getInt("jobrunner.wkid", 4326));
        m_attribute = configuration.get("jobrunner.attribute", "NAME");
        m_latIndex = configuration.getInt("jobrunner.latIndex", 1);
        m_lonIndex = configuration.getInt("jobrunner.lonIndex", 2);

        loadQuadTree(configuration);
    }

    private void loadQuadTree(final Configuration configuration) throws IOException
    {
        int index = 0;
        m_quadTree = new QuadTree(new Envelope2D(-180, -90, 180, 90), 8);

        final EsriObjectMapper mapper = new EsriObjectMapper();
        final FileSystem hdfs = FileSystem.get(configuration);
        final InputStream inputStream = hdfs.open(new Path(configuration.get("jobrunner.featurePath")));
        try
        {
            final Envelope envelope = new Envelope();
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            while (bufferedInputStream.available() > 0)
            {
                final int ch = bufferedInputStream.read();
                if (ch == '\n')
                {
                    final EsriFeature feature = mapper.toFeature(byteArrayOutputStream.toByteArray());
                    m_featureList.add(feature);
                    feature.geometry.queryEnvelope(envelope);
                    m_quadTree.insert(index++, new Envelope2D(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
                    byteArrayOutputStream.reset();
                }
                else
                {
                    byteArrayOutputStream.write(ch & 0x7F); // Force to UTF8
                }
            }
        }
        finally
        {
            inputStream.close();
        }
        m_quadTreeIterator = m_quadTree.getIterator();
    }

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context) throws IOException, InterruptedException
    {
        if (key.get() > 0) // Skip first line (header)
        {
            final String[] tokens = m_pattern.split(value.toString());
            final double lat = Double.parseDouble(tokens[m_latIndex]);
            final double lon = Double.parseDouble(tokens[m_lonIndex]);
            final Point point = new Point(lon, lat);
            final String name = queryQuadTree(point);
            if (name != null)
            {
                context.write(new Text(name), ONE);
            }
        }
    }

    private String queryQuadTree(final Point point)
    {
        m_quadTreeIterator.resetIterator(point, 0);

        int elementIndex = m_quadTreeIterator.next();

        while (elementIndex >= 0)
        {
            final int featureIndex = m_quadTree.getElement(elementIndex);
            final EsriFeature feature = m_featureList.get(featureIndex);
            if (GeometryEngine.contains(feature.geometry, point, m_spatialReference))
            {
                return (String) feature.attributes.get(m_attribute);
            }
            elementIndex = m_quadTreeIterator.next();
        }
        return null;
    }
}
