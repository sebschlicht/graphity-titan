package de.uniko.sebschlicht.graphity.titan.model;

import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.graphity.titan.EdgeType;
import de.uniko.sebschlicht.graphity.titan.Walker;

public class UserPostIterator implements PostIterator {

    protected UserProxy pUser;

    protected StatusUpdateProxy pCrrStatusUpdate;

    protected Vertex _vReplica;

    public UserPostIterator(
            UserProxy pUser) {
        this.pUser = pUser;
        pCrrStatusUpdate = getLastUserPost(pUser);
    }

    public UserProxy getUser() {
        return pUser;
    }

    public void setReplicaVertex(Vertex vReplica) {
        _vReplica = vReplica;
    }

    public Vertex getReplicaVertex() {
        return _vReplica;
    }

    protected static StatusUpdateProxy getLastUserPost(UserProxy pUser) {
        Vertex vLastPost =
                Walker.nextVertex(pUser.getVertex(),
                        EdgeType.PUBLISHED.getLabel());
        if (vLastPost != null) {
            StatusUpdateProxy pStatusUpdate = new StatusUpdateProxy(vLastPost);
            pStatusUpdate.setAuthor(pUser);
            return pStatusUpdate;
        } else {
            return null;
        }
    }

    @Override
    public boolean hasNext() {
        return (pCrrStatusUpdate != null);
    }

    @Override
    public StatusUpdateProxy next() {
        StatusUpdateProxy pOldStatusUpdate = pCrrStatusUpdate;
        if (pOldStatusUpdate != null) {
            Vertex vNextStatusUpdate =
                    Walker.nextVertex(pOldStatusUpdate.getVertex(),
                            EdgeType.PUBLISHED.getLabel());
            if (vNextStatusUpdate != null) {
                pCrrStatusUpdate = new StatusUpdateProxy(vNextStatusUpdate);
                pCrrStatusUpdate.setAuthor(pUser);
            } else {
                pCrrStatusUpdate = null;
            }
        }
        return pOldStatusUpdate;
    }

    @Override
    public void remove() {
        if (hasNext()) {
            next();
        }
    }

    @Override
    public long getCrrPublished() {
        if (hasNext()) {
            return pCrrStatusUpdate.getPublished();
        }
        return 0;
    }
}
