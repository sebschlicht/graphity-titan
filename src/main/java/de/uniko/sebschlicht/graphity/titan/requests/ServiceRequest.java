package de.uniko.sebschlicht.graphity.titan.requests;

import de.uniko.sebschlicht.socialnet.requests.RequestType;

/**
 * Basic service request class.<br>
 * Service requests hold all information necessary to execute a request on
 * (service) server side.
 * 
 * @author sebschlicht
 * 
 */
public abstract class ServiceRequest {

    /**
     * request type
     */
    protected RequestType _type;

    /**
     * time stamp of the request accept on server side
     */
    protected long _timestamp;

    /**
     * Instantiates a service request with the current timestamp.
     * 
     * @param type
     *            request type
     */
    protected ServiceRequest(
            RequestType type) {
        _type = type;
        setTimestamp(System.currentTimeMillis());
    }

    /**
     * @param timestamp
     *            time stamp of the request accept on server side
     */
    public void setTimestamp(long timestamp) {
        _timestamp = timestamp;
    }

    /**
     * @return time stamp of the request accept on server side
     */
    public long getTimestamp() {
        return _timestamp;
    }
}
