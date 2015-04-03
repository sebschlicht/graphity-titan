package de.uniko.sebschlicht.graphity.titan.requests;

import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.socialnet.requests.RequestType;

public class FeedServiceRequest extends ServiceRequest {

    protected Vertex _vUser;

    protected int _feedLength;

    public FeedServiceRequest() {
        super(RequestType.FEED);
    }

    public void setUserVertex(Vertex vUser) {
        _vUser = vUser;
    }

    public Vertex getUserVertex() {
        return _vUser;
    }

    public void setFeedLength(int feedLength) {
        _feedLength = feedLength;
    }

    public int getFeedLength() {
        return _feedLength;
    }
}
