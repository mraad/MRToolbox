package com.esri.json.serializer;

import com.esri.core.geometry.Geometry.Type;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;

public class GeometryTypeJsonSerializer extends JsonSerializer<Type>
{

    @Override
    public void serialize(
            Type geometryType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException
    {
        jsonGenerator.writeString("esriGeometry" + geometryType);
    }
}
