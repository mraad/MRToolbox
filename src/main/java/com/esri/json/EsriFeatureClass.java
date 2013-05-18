package com.esri.json;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
public class EsriFeatureClass
{
    public String displayFieldName;

    /**
     * Map of field aliases for applicable fields in this feature class
     */
    public Map<String, Object> fieldAliases;

    /**
     * Esri geometry type (Polygon, Point, ...)
     */
    public Geometry.Type geometryType;

    /**
     * Spatial reference for the feature class (null, if undefined)
     */
    public SpatialReference spatialReference;

    /**
     * Array of field definitions (name, type, alias, ...)
     */
    public EsriField[] fields;

    /**
     * Array of features (attributes, geometry)
     */
    public EsriFeature[] features;


}