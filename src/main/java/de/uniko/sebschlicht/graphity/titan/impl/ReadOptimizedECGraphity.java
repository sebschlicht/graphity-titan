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
import de.uniko.sebschlicht.graphity.titan.requests.ServiceRequestFollow;

public class ReadOptimizedECGraphity extends TitanGraphity {

    public ReadOptimizedECGraphity(
            TitanGraph graphDb) {
        super(graphDb);
    }

    public boolean addFollowship(ServiceRequestFollow request) {
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
        insertIntoEgoNetwork(request.getSubscriberVertex(), rFollowed,
                request.getTimestamp());
        return true;
    }

    protected void insertIntoEgoNetwork(
            Vertex vSubscriber,
            Vertex rFollowed,
            long timestamp) {
        if (Walker.nextVertex(vSubscriber, EdgeType.GRAPHITY.getLabel()) == null) {
            // very first subscription of the subscriber
            /*
             * Concurrent subscription creation for the same user may create two
             * GRAPHITY edges starting from the subscriber to two replicas.
             * <br>
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
         * We have no idea why a replica node can be unconnected to the user,
         * but be in any user's replica layer.
         * Nevertheless, it can.
         */
        if (user == null) {
            return 0;
        }
        UserProxy pUser = new UserProxy(user);
        return pUser.getLastPostTimestamp();
    }
}
