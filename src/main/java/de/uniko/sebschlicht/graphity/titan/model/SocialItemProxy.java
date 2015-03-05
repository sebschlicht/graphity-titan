package de.uniko.sebschlicht.graphity.titan.model;

import com.tinkerpop.blueprints.Vertex;

public abstract class SocialItemProxy {

    public static final String PROP_IDENTIFIER = "identifier";

    protected Vertex vertex;

    /**
     * item identifier
     */
    protected long identifier;

    public SocialItemProxy(
            Vertex vertex) {
        this.vertex = vertex;
    }

    public Vertex getVertex() {
        return vertex;
    }

    /**
     * @return cached item identifier
     */
    public long getIdentifier() {
        if (identifier == 0) {
            //TODO catch return value null
            identifier = vertex.getProperty(PROP_IDENTIFIER);
        }
        return identifier;
    }

    public void setIdentifier(long identifier) {
        vertex.setProperty(PROP_IDENTIFIER, identifier);
        this.identifier = identifier;
    }
}
