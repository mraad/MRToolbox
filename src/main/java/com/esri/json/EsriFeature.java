package com.esri.json;

import com.esri.core.geometry.Geometry;

import java.util.Map;


public class EsriFeature
{
    /**
     * Geometry associated with this feature
     */
    public Geometry geometry;

    /**
     * Map of attributes
     */
    public Map<String, Object> attributes;

}
