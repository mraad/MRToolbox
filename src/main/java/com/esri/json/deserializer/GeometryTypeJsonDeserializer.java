package com.esri.json.deserializer;

import com.esri.core.geometry.Geometry;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

import java.io.IOException;

/**
 * Deserializes a JSON geometry type enumeration into a Geometry.Type.* enumeration
 */
public class GeometryTypeJsonDeserializer extends JsonDeserializer<Geometry.Type>
{

    public GeometryTypeJsonDeserializer()
    {
    }

    @Override
    public Geometry.Type deserialize(
            JsonParser jsonParser,
            DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException
    {

        String typeText = jsonParser.getText();

        // geometry type enumerations coming from the JSON are prepended with "esriGeometry" (i.e. esriGeometryPolygon)
        // while the geometry-java-api uses the form Geometry.Type.Polygon
        if (typeText.startsWith("esriGeometry"))
        {
            // cut out esriGeometry to match Geometry.Type enumeration values
            typeText = typeText.substring(12);
            try
            {
                return Enum.valueOf(Geometry.Type.class, typeText);
            }
            catch (Exception e)
            {
                // parsing failed, fall through to unknown geometry type
            }
        }


        return Geometry.Type.Unknown;
    }
}