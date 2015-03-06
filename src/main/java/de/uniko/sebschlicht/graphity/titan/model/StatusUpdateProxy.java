package de.uniko.sebschlicht.graphity.titan.model;

import com.tinkerpop.blueprints.Vertex;

import de.metalcon.domain.UidType;
import de.metalcon.exceptions.ServiceOverloadedException;
import de.uniko.sebschlicht.graphity.titan.TitanGraphity;
import de.uniko.sebschlicht.socialnet.StatusUpdate;

/**
 * News item vertex wrapper.
 * 
 * @author sebschlicht
 * 
 */
public class StatusUpdateProxy extends SocialItemProxy {

    /**
     * date and time the news item was published
     */
    public static final String PROP_PUBLISHED = "published";

    /**
     * content message
     */
    public static final String PROP_MESSAGE = "message";

    /**
     * timestamp of publishing
     */
    protected long published;

    /**
     * author (proxy to user vertex)
     */
    protected UserProxy pAuthor;

    public StatusUpdateProxy(
            Vertex vStatusUpdate) {
        super(vStatusUpdate);
    }

    public void initVertex(long published, String message) {
        try {
            long identifier =
                    TitanGraphity.generateMuid(UidType.DISC).getValue();
            setIdentifier(identifier);
        } catch (ServiceOverloadedException e) {
            throw new IllegalStateException(e);
        }
        setPublished(published);
        setMessage(message);
    }

    public void setAuthor(UserProxy pAuthor) {
        this.pAuthor = pAuthor;
    }

    /**
     * @return cached timestamp of publishing
     */
    public long getPublished() {
        if (published == 0) {
            //TODO catch return value null
            published = vertex.getProperty(PROP_PUBLISHED);
        }
        return published;
    }

    /**
     * Stores the creation timestamp in vertex and cache.
     * 
     * @param published
     *            timestamp of creation
     */
    public void setPublished(long published) {
        vertex.setProperty(PROP_PUBLISHED, published);
        this.published = published;
    }

    public String getMessage() {
        return (String) vertex.getProperty(PROP_MESSAGE);
    }

    /**
     * Stores the content message in vertex.
     * 
     * @param message
     *            content message
     */
    public void setMessage(String message) {
        vertex.setProperty(PROP_MESSAGE, message);
    }

    public StatusUpdate getStatusUpdate() {
        return new StatusUpdate(String.valueOf(pAuthor.getIdentifier()),
                getPublished(), getMessage());
    }
}
