package de.uniko.sebschlicht.graphity.titan.requests;

import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.socialnet.requests.RequestType;

public class ServiceRequestFollow extends AbstractServiceRequest {

    protected Vertex _vSubscriber;

    protected Vertex _vFollowed;

    protected ServiceRequestFollow(
            RequestType type) {
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

    @Override
    public String[] toStringArray() {
        return null;
    }
}
