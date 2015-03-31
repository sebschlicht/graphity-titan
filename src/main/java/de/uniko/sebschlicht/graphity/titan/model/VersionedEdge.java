package de.uniko.sebschlicht.graphity.titan.model;

import com.tinkerpop.blueprints.Edge;

public class VersionedEdge {

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
        if (_timestamp == -1) {
            _timestamp = _edge.getProperty(PROP_TIMESTAMP);
        }
        return _timestamp;
    }
}
