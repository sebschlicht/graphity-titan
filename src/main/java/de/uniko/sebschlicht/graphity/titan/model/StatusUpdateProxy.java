package de.uniko.sebschlicht.graphity.titan.model;

import com.tinkerpop.blueprints.Vertex;

import de.metalcon.exceptions.ServiceOverloadedException;
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
     * news item identifier generator
     */
    protected static IdGenerator ID_GENERATOR;

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
        //long identifier =
        //        GraphityExtension.generateMuid(UidType.DISC).getValue();
        try {
            long identifier = ID_GENERATOR.generate();
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

    public static void setIdGenerator(IdGenerator idGenerator) {
        ID_GENERATOR = idGenerator;
    }

    public interface IdGenerator {

        /**
         * Generates an unique news item identifier.
         * 
         * @return unique news item identifier
         * @throws ServiceOverloadedException
         *             if creating news items too frequently
         */
        long generate() throws ServiceOverloadedException;
    }
}
