package de.uniko.sebschlicht.graphity.titan.model;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.graphity.titan.EdgeType;
import de.uniko.sebschlicht.graphity.titan.Walker;

public class UserProxy extends SocialItemProxy {

    /**
     * timestamp of last stream update
     */
    public static final String PROP_LAST_STREAM_UDPATE = "last_post";

    /**
     * replica vertex
     */
    protected Vertex _vReplica;

    /**
     * (optional) last recent status update posted by this user
     */
    protected StatusUpdateProxy lastPost;

    /**
     * (optional) timestamp of the last recent status update posted by this user
     */
    protected long _lastPostTimestamp;

    public UserProxy(
            Vertex vUser) {
        super(vUser);
        _lastPostTimestamp = -1;
    }

    public void setReplicaVertex(Vertex vReplica) {
        _vReplica = vReplica;
    }

    public Vertex getReplicaVertex() {
        return _vReplica;
    }

    public void setLastPostTimestamp(long lastPostTimestamp) {
        vertex.setProperty(PROP_LAST_STREAM_UDPATE, lastPostTimestamp);
        _lastPostTimestamp = lastPostTimestamp;
    }

    public long getLastPostTimestamp() {
        if (_lastPostTimestamp == -1) {
            Long value = vertex.getProperty(PROP_LAST_STREAM_UDPATE);
            _lastPostTimestamp = (value == null) ? 0L : value;
        }
        return _lastPostTimestamp;
    }

    public void addStatusUpdate(StatusUpdateProxy statusUpdate) {
        statusUpdate.setAuthor(this);
        /**
         * update news item list
         */
        // get last recent news item
        Vertex lastUpdate =
                Walker.nextVertex(vertex, EdgeType.PUBLISHED.getLabel());
        // update references to previous news item (if existing)
        if (lastUpdate != null) {
            Walker.removeSingleEdge(vertex, Direction.OUT,
                    EdgeType.PUBLISHED.getLabel());
            statusUpdate.getVertex().addEdge(EdgeType.PUBLISHED.getLabel(),
                    lastUpdate);
        }
        // link from user to news item vertex
        vertex.addEdge(EdgeType.PUBLISHED.getLabel(), statusUpdate.getVertex());
        setLastPostTimestamp(statusUpdate.getPublished());
    }
}
