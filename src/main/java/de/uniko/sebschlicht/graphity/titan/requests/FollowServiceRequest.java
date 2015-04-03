package de.uniko.sebschlicht.graphity.titan.requests;

import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.socialnet.requests.RequestType;

public class FollowServiceRequest extends ServiceRequest {

    protected Vertex _vSubscriber;

    protected Vertex _vFollowed;

    public FollowServiceRequest() {
        super(RequestType.FOLLOW);
    }

    public void setSubscriberVertex(Vertex vSubscriber) {
        _vSubscriber = vSubscriber;
    }

    public Vertex getSubscriberVertex() {
        return _vSubscriber;
    }

    public void setFollowedVertex(Vertex vFollowed) {
        _vFollowed = vFollowed;
    }

    public Vertex getFollowedVertex() {
        return _vFollowed;
    }
}
