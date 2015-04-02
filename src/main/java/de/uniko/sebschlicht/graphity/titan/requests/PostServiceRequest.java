package de.uniko.sebschlicht.graphity.titan.requests;

import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.socialnet.StatusUpdate;
import de.uniko.sebschlicht.socialnet.requests.RequestType;

public class PostServiceRequest extends ServiceRequest {

    protected Vertex _vAuthor;

    protected StatusUpdate _statusUpdate;

    public PostServiceRequest() {
        super(RequestType.POST);
    }

    public void setAuthorVertex(Vertex vAuthor) {
        _vAuthor = vAuthor;
    }

    public Vertex getAuthorVertex() {
        return _vAuthor;
    }
}
