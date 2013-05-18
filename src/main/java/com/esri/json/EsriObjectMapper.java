package com.esri.json;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.deserializer.GeometryJsonDeserializer;
import com.esri.json.deserializer.GeometryTypeJsonDeserializer;
import com.esri.json.deserializer.SpatialReferenceJsonDeserializer;
import com.esri.json.serializer.GeometryJsonSerializer;
import com.esri.json.serializer.GeometryTypeJsonSerializer;
import com.esri.json.serializer.SpatialReferenceJsonSerializer;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;

import java.io.IOException;

/**
 */
public class EsriObjectMapper
{
    private final ObjectMapper m_objectMapper = new ObjectMapper();

    public EsriObjectMapper()
    {
        final SimpleModule module = new SimpleModule("EsriModule", new Version(1, 0, 0, null));

        module.addDeserializer(Geometry.class, new GeometryJsonDeserializer());
        module.addDeserializer(SpatialReference.class, new SpatialReferenceJsonDeserializer());
        module.addDeserializer(Geometry.Type.class, new GeometryTypeJsonDeserializer());

        module.addSerializer(Geometry.class, new GeometryJsonSerializer());
        module.addSerializer(Geometry.Type.class, new GeometryTypeJsonSerializer());
        module.addSerializer(SpatialReference.class, new SpatialReferenceJsonSerializer());

        m_objectMapper.registerModule(module);
    }

    public EsriFeature toFeature(byte[] bytes) throws IOException
    {
        return m_objectMapper.readValue(bytes, EsriFeature.class);
    }

    public String toJSON(EsriFeature feature) throws IOException
    {
        return m_objectMapper.writeValueAsString(feature);
    }
}
