package de.uniko.sebschlicht.graphity.titan.model;

import com.tinkerpop.blueprints.Vertex;

import de.metalcon.domain.UidType;
import de.metalcon.exceptions.ServiceOverloadedException;
import de.uniko.sebschlicht.graphity.titan.TitanGraphity;
import de.uniko.sebschlicht.socialnet.StatusUpdate;

public class StatusUpdateProxy extends SocialItemProxy {

    /**
     * date and time the activity was published
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

    public boolean init() {
        try {
            long identifier =
                    TitanGraphity.generateMuid(UidType.DISC).getValue();
            setIdentifier(identifier);
            return true;
        } catch (ServiceOverloadedException e) {
            throw new IllegalStateException(e);
        }
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

    public void setPublished(long published) {
        vertex.setProperty(PROP_PUBLISHED, published);
        this.published = published;
    }

    public String getMessage() {
        return (String) vertex.getProperty(PROP_MESSAGE);
    }

    public void setMessage(String message) {
        vertex.setProperty(PROP_MESSAGE, message);
    }

    public StatusUpdate getStatusUpdate() {
        return new StatusUpdate(String.valueOf(pAuthor.getIdentifier()),
                getPublished(), getMessage());
    }
}
