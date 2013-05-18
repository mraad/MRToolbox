package com.esri.json.serializer;

import com.esri.core.geometry.SpatialReference;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;

public class SpatialReferenceJsonSerializer extends JsonSerializer<SpatialReference>
{

    @Override
    public void serialize(
            SpatialReference spatialReference,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider) throws IOException,
            JsonProcessingException
    {

        final int wkid = spatialReference.getID();

        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("wkid", wkid);
        jsonGenerator.writeEndObject();
    }

}
