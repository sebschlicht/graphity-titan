package de.uniko.sebschlicht.graphity.titan.impl;

import java.util.TreeSet;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.graphity.titan.EdgeType;
import de.uniko.sebschlicht.graphity.titan.TitanGraphity;
import de.uniko.sebschlicht.graphity.titan.Walker;
import de.uniko.sebschlicht.graphity.titan.model.PostIteratorComparator;
import de.uniko.sebschlicht.graphity.titan.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.titan.model.UserPostIterator;
import de.uniko.sebschlicht.graphity.titan.model.UserProxy;
import de.uniko.sebschlicht.graphity.titan.model.UserProxyComparator;
import de.uniko.sebschlicht.graphity.titan.model.VersionedEdge;
import de.uniko.sebschlicht.graphity.titan.requests.FeedServiceRequest;
import de.uniko.sebschlicht.graphity.titan.requests.FollowServiceRequest;
import de.uniko.sebschlicht.graphity.titan.requests.PostServiceRequest;
import de.uniko.sebschlicht.graphity.titan.requests.UnfollowServiceRequest;
import de.uniko.sebschlicht.socialnet.StatusUpdateList;

public class ReadOptimizedECGraphity extends TitanGraphity {

    public ReadOptimizedECGraphity(
            TitanGraph graphDb) {
        super(graphDb);
    }

    @Override
    public boolean addFollowship(FollowServiceRequest request) {
        // try to find the replica node of the user followed
        Vertex vUserFollowed;
        for (Vertex vFollowedReplica : request.getSubscriberVertex()
                .getVertices(Direction.OUT, EdgeType.FOLLOWS.getLabel())) {
            /*
             * In an EC storage backend there may be FOLLOWS edges that point to
             * multiple replicas of the same user.
             * <br>
             * We could fix them when adding a FOLLOWS edge or when accessing
             * the news stream of the user/a follower.
             * <br>
             * This happens when two request try to add the same subscription
             * concurrently. Actually we don't expect this to happen.
             */
            /*
             * A replica will never have multiple REPLICA edges.
             */
            vUserFollowed =
                    Walker.nextVertex(vFollowedReplica,
                            EdgeType.REPLICA.getLabel());

            /*
             * Another request may add a subscription to the subscriber
             * concurrently.
             * We don't need to care, because the worst case is that it is the
             * same subscription we want to add. How to resolve such issues is
             * described above.
             * In production, we don't expect this to happen.
             * In our evaluation this case is rare but there is a chance.
             */
            if (vUserFollowed == null) {
                continue;
            }
            if (vUserFollowed.equals(request.getSubscriberVertex())) {
                // user is already following this user
                return false;
            }
        }

        // create replica for user followed
        /*
         * The replica creation won't collide with any other write request.
         * However, during the replica creation we enter a state where the
         * replica is followed, but not yet linked to its target user. The
         * read and write algorithms are aware of this state and can handle it.
         */
        final Vertex rFollowed = graphDb.addVertex(null);
        request.getSubscriberVertex().addEdge(EdgeType.FOLLOWS.getLabel(),
                rFollowed);
        rFollowed.addEdge(EdgeType.REPLICA.getLabel(),
                request.getFollowedVertex());

        // insert replica in subscriber's ego network
        repairReplicaLayer(request.getSubscriberVertex(),
                request.getTimestamp());
        return true;
    }

    /**
     * Fully repairs the replica layer of a user.
     * 
     * @param vUser
     *            user vertex
     * @param timestamp
     *            request timestamp
     */
    protected static void repairReplicaLayer(Vertex vUser, long timestamp) {
        TreeSet<UserProxy> replicaLayer =
                new TreeSet<>(new UserProxyComparator());

        Vertex vFollowed;
        for (Vertex vFollowedReplica : vUser.getVertices(Direction.OUT,
                EdgeType.FOLLOWS.getLabel())) {
            vFollowed =
                    Walker.nextVertex(vFollowedReplica,
                            EdgeType.REPLICA.getLabel());
            if (vFollowed != null) {
                // remove Graphity edges
                for (Edge graphityEdge : vFollowed.getEdges(Direction.OUT,
                        EdgeType.GRAPHITY.getLabel())) {
                    graphityEdge.remove();
                }
                for (Edge graphityEdge : vFollowed.getEdges(Direction.IN,
                        EdgeType.GRAPHITY.getLabel())) {
                    graphityEdge.remove();
                }

                // sort the users represented by the replica layer vertices
                UserProxy followed = new UserProxy(vFollowed);
                followed.setReplicaVertex(vFollowedReplica);
                replicaLayer.add(followed);
            }
        }

        // rebuild replica layer
        Vertex prev = vUser;
        while (!replicaLayer.isEmpty()) {
            UserProxy followed = replicaLayer.pollLast();
            prev.addEdge(EdgeType.GRAPHITY.getLabel(),
                    followed.getReplicaVertex());
            prev = followed.getReplicaVertex();
        }
    }

    @Override
    public boolean removeFollowship(UnfollowServiceRequest request) {
        // try to find the replica node of the user followed
        Vertex vUserFollowed, rFollowed = null;
        for (Vertex vFollowedReplica : request.getSubscriberVertex()
                .getVertices(Direction.OUT, EdgeType.FOLLOWS.getLabel())) {
            /*
             * In an EC storage backend there may be FOLLOWS edges that point to
             * multiple replicas of the same user.
             * <br>
             * In the subscription removal process we should be aware of replica
             * duplicates and remove all subscriptions to a user at once.
             */
            /*
             * A replica will never have multiple REPLICA edges.
             */
            vUserFollowed =
                    Walker.nextVertex(vFollowedReplica,
                            EdgeType.REPLICA.getLabel());

            /*
             * Another request may add a subscription to the subscriber
             * concurrently.
             * We don't need to care, because the worst case is that it is the
             * same subscription we want to remove.
             * In production, we don't expect this to happen.
             * In our evaluation this case is rare but there is a chance.
             */
            if (vUserFollowed == null) {
                continue;
            }
            if (vUserFollowed.equals(request.getSubscriberVertex())) {
                rFollowed = vFollowedReplica;
                break;
            }
        }
        if (rFollowed == null) {// there is no such followship existing
            return false;
        }

        // remove the followship
        Walker.removeSingleEdge(rFollowed, Direction.OUT,
                EdgeType.REPLICA.getLabel());
        Walker.removeSingleEdge(rFollowed, Direction.IN,
                EdgeType.FOLLOWS.getLabel());
        // remove the replica node
        rFollowed.remove();

        // repair the replica layer of the ex subscriber
        repairReplicaLayer(request.getSubscriberVertex(),
                request.getTimestamp());
        return true;
    }

    @Override
    public long addStatusUpdate(PostServiceRequest request) {
        // create new status update vertex and fill via proxy
        Vertex crrUpdate = graphDb.addVertex(null);
        StatusUpdateProxy pStatusUpdate = new StatusUpdateProxy(crrUpdate);
        //TODO handle service overload
        pStatusUpdate.initVertex(request.getStatusUpdate().getPublished(),
                request.getStatusUpdate().getMessage());

        // add status update to author (link vertex, update user)
        UserProxy pAuthor = new UserProxy(request.getAuthorVertex());

        /**
         * update author's news item list
         */
        // get last recent news item
        Vertex lastUpdate =
                Walker.nextMostRecentVertex(pAuthor.getVertex(),
                        EdgeType.PUBLISHED.getLabel());
        // update references to previous news item (if existing)
        if (lastUpdate != null) {
            Walker.removeMostRecentEdge(pAuthor.getVertex(), Direction.OUT,
                    EdgeType.PUBLISHED.getLabel(), request.getTimestamp());
            crrUpdate.addEdge(EdgeType.PUBLISHED.getLabel(), lastUpdate);
        }
        // link from user to news item vertex
        Edge edge =
                pAuthor.getVertex().addEdge(EdgeType.PUBLISHED.getLabel(),
                        crrUpdate);
        VersionedEdge verEdge = new VersionedEdge(edge);
        verEdge.setTimestamp(request.getTimestamp());
        pAuthor.setLastPostTimestamp(request.getStatusUpdate().getPublished());
        pAuthor.addStatusUpdate(pStatusUpdate);
        pStatusUpdate.setAuthor(pAuthor);

        // update replica layers of the author's followers
        updateReplicaLayers(request.getAuthorVertex(), request.getTimestamp());
        return pStatusUpdate.getIdentifier();
    }

    /**
     * Updates the replica layers of all subscribers of a user.
     * After the update the user is at the first position of all of its
     * subscriber's replica layers.
     * 
     * @param vUser
     *            user vertex
     * @param timestamp
     *            request timestamp
     */
    protected static void updateReplicaLayers(Vertex vUser, long timestamp) {
        // loop through subscribers
        /*
         * There may be multiple subscriptions to the same user.
         */
        Vertex vSubscriber;
        for (Vertex rFollowed : vUser.getVertices(Direction.IN,
                EdgeType.REPLICA.getLabel())) {
            // load each replica and the user corresponding
            /*
             * A replica can't have multiple FOLLOWS edges.
             */
            vSubscriber =
                    Walker.previousVertex(rFollowed,
                            EdgeType.FOLLOWS.getLabel());
            /*
             * The replica might be unconnected from its user, if another
             * request removes this subscription concurrently.
             * In this case we don't need to care for this user at this time.
             */
            if (vSubscriber == null) {
                continue;
            }

            // repair the replica layer of the subscriber
            repairReplicaLayer(vSubscriber, timestamp);
        }
    }

    @Override
    public StatusUpdateList readStatusUpdates(FeedServiceRequest request) {
        StatusUpdateList statusUpdates = new StatusUpdateList();
        if (request.getUserVertex() == null) {
            return statusUpdates;
        }
        final TreeSet<UserPostIterator> postIterators =
                new TreeSet<UserPostIterator>(new PostIteratorComparator());

        // load first user by replica
        UserProxy pCrrUser = null;
        UserPostIterator userPostIterator;
        /*
         * walk along the most recent Graphity edge
         */
        Vertex vReplica =
                Walker.nextMostRecentVertex(request.getUserVertex(),
                        EdgeType.GRAPHITY.getLabel());
        if (vReplica != null) {
            Vertex vUser =
                    Walker.nextVertex(vReplica, EdgeType.REPLICA.getLabel());

            /*
             * The replica node may be unconnected to the user, see
             * {@link #getLastUpdateByReplica}.
             * This would leave the user post iterator set empty.
             */
            if (vUser != null) {
                pCrrUser = new UserProxy(vUser);
                userPostIterator = new UserPostIterator(pCrrUser);
                userPostIterator.setReplicaVertex(vReplica);

                if (userPostIterator.hasNext()) {
                    postIterators.add(userPostIterator);
                }
            }
        }

        // handle user queue
        UserProxy pPrevUser = pCrrUser;
        int numStatusUpdates = request.getFeedLength();
        while (statusUpdates.size() < numStatusUpdates
                && !postIterators.isEmpty()) {
            // add last recent status update
            userPostIterator = postIterators.pollLast();
            statusUpdates.add(userPostIterator.next().getStatusUpdate());

            // re-add iterator if not empty
            if (userPostIterator.hasNext()) {
                postIterators.add(userPostIterator);
            }

            // load additional user if necessary
            if (userPostIterator.getUser() == pPrevUser) {
                vReplica =
                        Walker.nextMostRecentVertex(
                                userPostIterator.getReplicaVertex(),
                                EdgeType.GRAPHITY.getLabel());
                // check if additional user existing
                if (vReplica != null) {
                    Vertex vCrrUser =
                            Walker.nextVertex(vReplica,
                                    EdgeType.REPLICA.getLabel());
                    if (vCrrUser != null) {
                        pCrrUser = new UserProxy(vCrrUser);
                        userPostIterator = new UserPostIterator(pCrrUser);
                        userPostIterator.setReplicaVertex(vReplica);
                        // check if user has status updates
                        if (userPostIterator.hasNext()) {
                            postIterators.add(userPostIterator);
                            pPrevUser = pCrrUser;
                        } else {
                            // further users do not need to be loaded
                            pPrevUser = null;
                        }
                    } else {//TODO the replica is unconnected, just ignore or keep breaking out?
                        // further users do not need to be loaded
                        pPrevUser = null;
                    }
                }
            }
        }

        return statusUpdates;
    }
}
