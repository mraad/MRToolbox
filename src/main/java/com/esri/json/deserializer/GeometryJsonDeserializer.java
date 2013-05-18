package com.esri.json.deserializer;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

import java.io.IOException;

/**
 * Deserializes a JSON geometry definition into a Geometry instance
 */
public class GeometryJsonDeserializer extends JsonDeserializer<Geometry>
{

    public GeometryJsonDeserializer()
    {
    }

    @Override
    public Geometry deserialize(
            JsonParser jsonParser,
            DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException
    {
        return GeometryEngine.jsonToGeometry(jsonParser).getGeometry();
    }
}