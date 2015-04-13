package de.uniko.sebschlicht.graphity.titan.model;

import com.tinkerpop.blueprints.Edge;

/**
 * Edge wrapper to versionize graph edges.
 * Edges have a time stamp to identify the latest version.
 * Versioned edges are used to avoid graph schema restrictions and locks.
 * 
 * @author sebschlicht
 * 
 */
public class VersionedEdge {

    /**
     * edge property key: time stamp of edge creation
     */
    public static final String PROP_TIMESTAMP = "timestamp";

    /**
     * underlying graph edge
     */
    protected Edge _edge;

    /**
     * (cached) time stamp of edge creation
     */
    protected long _timestamp;

    public VersionedEdge(
            Edge edge) {
        _edge = edge;
        _timestamp = -1;
    }

    /**
     * @return underlying graph edge
     */
    public Edge getEdge() {
        return _edge;
    }

    public void setTimestamp(long timestamp) {
        _timestamp = timestamp;
        _edge.setProperty(PROP_TIMESTAMP, timestamp);
    }

    /**
     * @return (cached) time stamp of edge creation
     */
    public long getTimestamp() {
        if (_timestamp == -1) {// uninitialized
            _timestamp = getProperty(PROP_TIMESTAMP, 0L);
        }
        return _timestamp;
    }

    /**
     * Retrieves an edge property.<br>
     * The default value is returned, if the property is missing.
     * 
     * @param key
     *            property key
     * @param defaultValue
     *            default value
     * @return 1. property value stored in the edge if the property is existing<br>
     *         2. <code>defaultValue</code> if the property is missing
     */
    public long getProperty(String key, long defaultValue) {
        Object value = _edge.getProperty(key);
        if (value != null) {// stored value
            return (long) value;
        } else {// default value
            return defaultValue;
        }
    }
}
