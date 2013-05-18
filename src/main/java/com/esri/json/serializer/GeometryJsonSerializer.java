package com.esri.json.serializer;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;

public class GeometryJsonSerializer extends JsonSerializer<Geometry>
{

    @Override
    public void serialize(
            Geometry geometry,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider) throws IOException,
            JsonProcessingException
    {
        jsonGenerator.writeRawValue(GeometryEngine.geometryToJson(null, geometry));
    }
}
