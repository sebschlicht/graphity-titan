package de.uniko.sebschlicht.graphity.titan.impl;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.graphity.titan.EdgeType;
import de.uniko.sebschlicht.graphity.titan.TitanGraphity;
import de.uniko.sebschlicht.graphity.titan.Walker;
import de.uniko.sebschlicht.graphity.titan.model.UserProxy;
import de.uniko.sebschlicht.graphity.titan.model.VersionedEdge;
import de.uniko.sebschlicht.graphity.titan.requests.FollowServiceRequest;
import de.uniko.sebschlicht.graphity.titan.requests.PostServiceRequest;
import de.uniko.sebschlicht.graphity.titan.requests.UnfollowServiceRequest;

public class ReadOptimizedECGraphity extends TitanGraphity {

    public ReadOptimizedECGraphity(
            TitanGraph graphDb) {
        super(graphDb);
    }

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

        // insert replica in subsriber's ego network
        insertIntoReplicaLayer(request.getSubscriberVertex(), rFollowed,
                request.getTimestamp());
        return true;
    }

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
        if (removeFromReplicaLayer(rFollowed, request.getTimestamp())) {
            // remove the followship
            Walker.removeSingleEdge(rFollowed, Direction.OUT,
                    EdgeType.REPLICA.getLabel());
            Walker.removeSingleEdge(rFollowed, Direction.IN,
                    EdgeType.FOLLOWS.getLabel());
            // remove the replica node
            rFollowed.remove();
        }
        return true;
    }

    public long addStatusUpdate(PostServiceRequest request) {

    }

    /**
     * Inserts a replica into the replica layer of a user.
     * After the insertion, the replica will be linked properly in the replica
     * layer, according to the Graphity index.
     * 
     * @param vSubscriber
     *            user vertex
     * @param rFollowed
     *            replica vertex
     * @param timestamp
     *            request timestamp
     */
    protected static void insertIntoReplicaLayer(
            Vertex vSubscriber,
            Vertex rFollowed,
            long timestamp) {
        if (Walker.nextVertex(vSubscriber, EdgeType.GRAPHITY.getLabel()) == null) {
            // very first subscription of the subscriber
            /*
             * Concurrent first subscription creation for the same user may
             * create two GRAPHITY edges starting from the subscriber to two
             * replicas.<br>
             * We can easily fix this problem by removing every but the most
             * recent GRAPHITY edge of a user whenever we see more than one.
             * We could either
             * 1. fix this when reading or
             * 2. read only the most recent edge and fix this on the next
             * write request/cleanup process.
             */
            vSubscriber.addEdge(EdgeType.GRAPHITY.getLabel(), rFollowed);
        } else {
            // search for insertion index within subscriber's replica layer
            final long followedTimestamp = getLastUpdateByReplica(rFollowed);
            long crrTimestamp;
            Vertex prevReplica = null;
            Vertex crrReplica = vSubscriber;
            do {
                // get next user in subscriber's replica layer
                prevReplica = crrReplica;
                crrReplica =
                        Walker.nextMostRecentVertex(prevReplica,
                                EdgeType.GRAPHITY.getLabel());
                // TODO we repair on-read
                if (crrReplica != null) {// there is still a user in the replica layer
                    crrTimestamp = getLastUpdateByReplica(crrReplica);
                } else {
                    crrTimestamp = 0;
                }
            } while (crrTimestamp > followedTimestamp);

            // re-link next replica in replica layer, if existing
            if (crrReplica != null) {
                /*
                 * Previous replica (following user or a replica of its replica
                 * layer) might have multiple GRAPHITY edges starting.
                 * <br>
                 * We remove the most recent one and repair on-read.
                 */
                Walker.removeMostRecentEdge(prevReplica, Direction.OUT,
                        EdgeType.GRAPHITY.getLabel());
                Edge eCrr =
                        rFollowed.addEdge(EdgeType.GRAPHITY.getLabel(),
                                crrReplica);
                VersionedEdge verEdge = new VersionedEdge(eCrr);
                verEdge.setTimestamp(timestamp);
            }
            // previous replica is the predecessor of the followed replica
            Edge ePrev =
                    prevReplica
                            .addEdge(EdgeType.GRAPHITY.getLabel(), rFollowed);
            VersionedEdge verEdge = new VersionedEdge(ePrev);
            verEdge.setTimestamp(timestamp);
        }
    }

    /**
     * Removes a replica from the replica layer of a user.
     * After the removal, other replicas will still be linked properly in the
     * replica layer, according to the Graphity index.
     * 
     * @param rFollowed
     *            replica vertex
     * @param timestamp
     *            request timestamp
     */
    protected static boolean removeFromReplicaLayer(
            Vertex rFollowed,
            long timestamp) {
        /*
         * There may be more than one Graphity edge leading in, if previous
         * requests modified the graph concurrently. We are aware of this
         * possible state and work with the most recent one.
         */
        final Vertex prev =
                Walker.previousMostRecentVertex(rFollowed,
                        EdgeType.GRAPHITY.getLabel());

        /*
         * Two states can cause, that the previous replica isn't existing:<br>
         * 1. The subscription has just been created and the replica was not yet
         * inserted in the subscriber's replica layer.
         * 2. [?] Another subscription removal process is removing the same
         * subscription and decided to remove the replica from the replica layer
         * before disconnecting it.
         * However, we will just exit and let the concurrent request win.
         */
        if (prev == null) {
            //TODO this actually never happened! let's see if it occurs
            //return false;
        }

        final Vertex next =
                Walker.nextMostRecentVertex(rFollowed,
                        EdgeType.GRAPHITY.getLabel());

        // bridge the user replica in the replica layer
        Walker.removeMostRecentEdge(prev, Direction.OUT,
                EdgeType.GRAPHITY.getLabel());
        if (next != null) {
            Walker.removeMostRecentEdge(next, Direction.IN,
                    EdgeType.GRAPHITY.getLabel());
            Edge edge = prev.addEdge(EdgeType.GRAPHITY.getLabel(), next);
            VersionedEdge verEdge = new VersionedEdge(edge);
            verEdge.setTimestamp(timestamp);
        }
        return true;
    }

    /**
     * Retrieves the timestamp of the last recent status update of the user
     * specified.
     * 
     * @param rUser
     *            replica of the user
     * @return timestamp of the user's last recent status update
     */
    protected static long getLastUpdateByReplica(final Vertex rUser) {
        final Vertex user =
                Walker.nextVertex(rUser, EdgeType.REPLICA.getLabel());
        /*
         * We have no idea why a replica node can be unconnected to the user
         * and be in any user's replica layer at the same time.
         * Nevertheless, we frequently observe this state, causing NPEs.
         */
        if (user == null) {
            // replica is unconnected, thus can not have status updates now
            return 0;
        }
        UserProxy pUser = new UserProxy(user);
        return pUser.getLastPostTimestamp();
    }
}
