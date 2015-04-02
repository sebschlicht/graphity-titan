package de.uniko.sebschlicht.graphity.titan.requests;

import de.uniko.sebschlicht.socialnet.requests.Request;
import de.uniko.sebschlicht.socialnet.requests.RequestType;

public abstract class AbstractServiceRequest extends Request {

    /**
     * time stamp of the request accept on server side
     */
    protected long _timestamp;

    protected AbstractServiceRequest(
            RequestType type) {
        super(type);
    }

    /**
     * @return time stamp of the request accept on server side
     */
    public long getTimestamp() {
        return _timestamp;
    }

    /**
     * not implemented
     * 
     * @return null
     */
    @Override
    public String[] toStringArray() {
        return null;
    }
}
