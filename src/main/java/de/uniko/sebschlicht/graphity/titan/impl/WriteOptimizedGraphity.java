package de.uniko.sebschlicht.graphity.titan.impl;

import java.util.TreeSet;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.graphity.titan.EdgeType;
import de.uniko.sebschlicht.graphity.titan.TitanGraphity;
import de.uniko.sebschlicht.graphity.titan.model.PostIteratorComparator;
import de.uniko.sebschlicht.graphity.titan.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.titan.model.UserPostIterator;
import de.uniko.sebschlicht.graphity.titan.model.UserProxy;
import de.uniko.sebschlicht.graphity.titan.requests.FeedServiceRequest;
import de.uniko.sebschlicht.graphity.titan.requests.FollowServiceRequest;
import de.uniko.sebschlicht.graphity.titan.requests.PostServiceRequest;
import de.uniko.sebschlicht.graphity.titan.requests.UnfollowServiceRequest;
import de.uniko.sebschlicht.socialnet.StatusUpdateList;

public class WriteOptimizedGraphity extends TitanGraphity {

    public WriteOptimizedGraphity(
            TitanGraph graphDb) {
        super(graphDb);
    }

    @Override
    public boolean addFollowship(FollowServiceRequest request) {
        // try to find the vertex of the user followed
        for (Vertex vIsFollowed : request.getSubscriberVertex().getVertices(
                Direction.OUT, EdgeType.FOLLOWS.getLabel())) {
            if (vIsFollowed.equals(request.getFollowedVertex())) {
                return false;
            }
        }

        // create star topology
        request.getSubscriberVertex().addEdge(EdgeType.FOLLOWS.getLabel(),
                request.getFollowedVertex());
        return true;
    }

    @Override
    public boolean removeFollowship(UnfollowServiceRequest request) {
        // delete the followship if existing
        Edge followship = null;
        for (Edge follows : request.getSubscriberVertex().getEdges(
                Direction.OUT, EdgeType.FOLLOWS.getLabel())) {
            if (follows.getVertex(Direction.IN).equals(
                    request.getFollowedVertex())) {
                followship = follows;
                break;
            }
        }

        // there is no such followship existing
        if (followship == null) {
            return false;
        }

        followship.remove();
        return true;
    }

    @Override
    protected long addStatusUpdate(PostServiceRequest request) {
        // create new status update vertex and fill via proxy
        Vertex crrUpdate = graphDb.addVertex(null);
        StatusUpdateProxy pStatusUpdate = new StatusUpdateProxy(crrUpdate);
        //TODO handle service overload
        pStatusUpdate.initVertex(request.getStatusUpdate().getPublished(),
                request.getStatusUpdate().getMessage());

        // add status update to user (link vertex, update user)
        UserProxy pAuthor = new UserProxy(request.getAuthorVertex());
        pAuthor.addStatusUpdate(pStatusUpdate);

        return pStatusUpdate.getIdentifier();
    }

    @Override
    protected StatusUpdateList readStatusUpdates(FeedServiceRequest request) {
        StatusUpdateList statusUpdates = new StatusUpdateList();
        if (request.getUserVertex() == null) {
            return statusUpdates;
        }
        final TreeSet<UserPostIterator> postIterators =
                new TreeSet<UserPostIterator>(new PostIteratorComparator());

        // loop through users followed
        UserProxy pCrrUser;
        UserPostIterator postIterator;
        for (Vertex vFollowed : request.getUserVertex().getVertices(
                Direction.OUT, EdgeType.FOLLOWS.getLabel())) {
            // add post iterator
            pCrrUser = new UserProxy(vFollowed);
            postIterator = new UserPostIterator(pCrrUser);

            if (postIterator.hasNext()) {
                postIterators.add(postIterator);
            }
        }

        // handle queue
        int numStatusUpdates = request.getFeedLength();
        while ((statusUpdates.size() < numStatusUpdates)
                && !postIterators.isEmpty()) {
            // add last recent status update
            postIterator = postIterators.pollLast();
            statusUpdates.add(postIterator.next().getStatusUpdate());

            // re-add iterator if not empty
            if (postIterator.hasNext()) {
                postIterators.add(postIterator);
            }
        }
        return statusUpdates;
    }
}
