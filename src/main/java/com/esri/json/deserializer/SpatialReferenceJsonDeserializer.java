package com.esri.json.deserializer;

import com.esri.core.geometry.SpatialReference;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

import java.io.IOException;

/**
 * Deserializes a JSON spatial reference definition into a SpatialReference instance
 */
public class SpatialReferenceJsonDeserializer extends JsonDeserializer<SpatialReference>
{

    public SpatialReferenceJsonDeserializer()
    {
    }

    @Override
    public SpatialReference deserialize(
            JsonParser jsonParser,
            DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException
    {
        try
        {
            return SpatialReference.fromJson(jsonParser);
        }
        catch (Exception e)
        {
            return null;
        }
    }
}
