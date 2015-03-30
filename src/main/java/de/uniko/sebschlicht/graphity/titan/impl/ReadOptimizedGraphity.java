package de.uniko.sebschlicht.graphity.titan.impl;

import java.util.TreeSet;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.graphity.titan.EdgeType;
import de.uniko.sebschlicht.graphity.titan.TitanGraphity;
import de.uniko.sebschlicht.graphity.titan.Walker;
import de.uniko.sebschlicht.graphity.titan.model.PostIteratorComparator;
import de.uniko.sebschlicht.graphity.titan.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.titan.model.UserPostIterator;
import de.uniko.sebschlicht.graphity.titan.model.UserProxy;
import de.uniko.sebschlicht.socialnet.StatusUpdate;
import de.uniko.sebschlicht.socialnet.StatusUpdateList;

public class ReadOptimizedGraphity extends TitanGraphity {

    public ReadOptimizedGraphity(
            TitanGraph graphDb) {
        super(graphDb);
    }

    @Override
    protected boolean addFollowship(Vertex vFollowing, Vertex vFollowed) {
        // try to find the replica node of the user followed
        for (Vertex vFollowedReplica : vFollowing.getVertices(Direction.OUT,
                EdgeType.FOLLOWS.getLabel())) {
            if (Walker
                    .nextVertex(vFollowedReplica, EdgeType.REPLICA.getLabel())
                    .equals(vFollowed)) {
                // user is already following this user
                return false;
            }
        }

        // create replica
        final Vertex newReplica = graphDb.addVertex(null);
        vFollowing.addEdge(EdgeType.FOLLOWS.getLabel(), newReplica);
        newReplica.addEdge(EdgeType.REPLICA.getLabel(), vFollowed);
        // check if followed user is the first in following's ego network
        if (Walker.nextVertex(vFollowing, EdgeType.GRAPHITY.getLabel()) == null) {
            vFollowing.addEdge(EdgeType.GRAPHITY.getLabel(), newReplica);
        } else {
            // search for insertion index within following replica layer
            final long followedTimestamp = getLastUpdateByReplica(newReplica);
            long crrTimestamp;
            Vertex prevReplica = vFollowing;
            Vertex nextReplica = null;
            while (true) {
                // get next user
                nextReplica =
                        Walker.nextVertex(prevReplica,
                                EdgeType.GRAPHITY.getLabel());
                if (nextReplica != null) {
                    crrTimestamp = getLastUpdateByReplica(nextReplica);
                    // step on if current user has newer status updates
                    if (crrTimestamp > followedTimestamp) {
                        prevReplica = nextReplica;
                        continue;
                    }
                }
                // insertion position has been found
                break;
            }
            // insert followed user's replica into following's ego network
            if (nextReplica != null) {
                Walker.removeSingleEdge(prevReplica, Direction.OUT,
                        EdgeType.GRAPHITY.getLabel());
                newReplica.addEdge(EdgeType.GRAPHITY.getLabel(), nextReplica);
            }
            prevReplica.addEdge(EdgeType.GRAPHITY.getLabel(), newReplica);
        }
        return true;
    }

    /**
     * remove a followed user from the replica layer
     * 
     * @param followedReplica
     *            replica of the user that will be removed
     */
    private void removeFromReplicaLayer(final Vertex followedReplica) {
        final Vertex prev =
                Walker.previousVertex(followedReplica,
                        EdgeType.GRAPHITY.getLabel());
        final Vertex next =
                Walker.nextVertex(followedReplica, EdgeType.GRAPHITY.getLabel());
        // bridge the user replica in the replica layer
        Walker.removeSingleEdge(prev, Direction.OUT,
                EdgeType.GRAPHITY.getLabel());
        if (next != null) {
            Walker.removeSingleEdge(next, Direction.IN,
                    EdgeType.GRAPHITY.getLabel());
            prev.addEdge(EdgeType.GRAPHITY.getLabel(), next);
        }
        // remove the followship
        Walker.removeSingleEdge(followedReplica, Direction.IN,
                EdgeType.FOLLOWS.getLabel());
        // remove the replica node itself
        Walker.removeSingleEdge(followedReplica, Direction.OUT,
                EdgeType.REPLICA.getLabel());
        followedReplica.remove();
    }

    @Override
    protected boolean removeFollowship(Vertex vFollowing, Vertex vFollowed) {
        // find the replica node of the user followed
        Vertex vReplica = null;
        for (Vertex vFollowedReplica : vFollowing.getVertices(Direction.OUT,
                EdgeType.FOLLOWS.getLabel())) {
            if (Walker
                    .nextVertex(vFollowedReplica, EdgeType.REPLICA.getLabel())
                    .equals(vFollowed)) {
                vReplica = vFollowedReplica;
                break;
            }
        }
        // there is no such followship existing
        if (vReplica == null) {
            return false;
        }
        removeFromReplicaLayer(vReplica);
        return true;
    }

    /**
     * update the ego networks of a user's followers
     * 
     * @param user
     *            user where changes have occurred
     */
    private void updateEgoNetworks(final Vertex user) {
        Vertex followingUser, lastPosterReplica;
        Vertex prevReplica, nextReplica;
        // loop through followers
        for (Vertex followedReplica : user.getVertices(Direction.IN,
                EdgeType.REPLICA.getLabel())) {
            // load each replica and the user corresponding
            followingUser =
                    Walker.previousVertex(followedReplica,
                            EdgeType.FOLLOWS.getLabel());
            if (followingUser == null) {// concurrent graph modifcation
                continue;
            }

            // bridge user node
            prevReplica =
                    Walker.previousVertex(followedReplica,
                            EdgeType.GRAPHITY.getLabel());
            if (prevReplica == null) {// concurrent graph modifcation
                continue;
            }

            if (!prevReplica.equals(followingUser)) {
                Walker.removeSingleEdge(followedReplica, Direction.IN,
                        EdgeType.GRAPHITY.getLabel());
                nextReplica =
                        Walker.nextVertex(followedReplica,
                                EdgeType.GRAPHITY.getLabel());
                if (nextReplica != null) {
                    Walker.removeSingleEdge(followedReplica, Direction.OUT,
                            EdgeType.GRAPHITY.getLabel());
                    prevReplica.addEdge(EdgeType.GRAPHITY.getLabel(),
                            nextReplica);
                }
            }
            // insert user's replica at its new position
            lastPosterReplica =
                    Walker.nextVertex(followingUser,
                            EdgeType.GRAPHITY.getLabel());
            if (!lastPosterReplica.equals(followedReplica)) {
                Walker.removeSingleEdge(followingUser, Direction.OUT,
                        EdgeType.GRAPHITY.getLabel());
                followingUser.addEdge(EdgeType.GRAPHITY.getLabel(),
                        followedReplica);
                followedReplica.addEdge(EdgeType.GRAPHITY.getLabel(),
                        lastPosterReplica);
            }
        }
    }

    @Override
    protected long addStatusUpdate(Vertex vAuthor, StatusUpdate statusUpdate) {
        // create new status update vertex and fill via proxy
        Vertex crrUpdate = graphDb.addVertex(null);
        StatusUpdateProxy pStatusUpdate = new StatusUpdateProxy(crrUpdate);
        //TODO handle service overload
        pStatusUpdate.initVertex(statusUpdate.getPublished(),
                statusUpdate.getMessage());

        // add status update to user (link vertex, update user)
        UserProxy pAuthor = new UserProxy(vAuthor);
        pAuthor.addStatusUpdate(pStatusUpdate);

        // update ego networks of status update author followers
        updateEgoNetworks(vAuthor);

        return pStatusUpdate.getIdentifier();
    }

    @Override
    protected StatusUpdateList readStatusUpdates(
            Vertex vReader,
            int numStatusUpdates) {
        StatusUpdateList statusUpdates = new StatusUpdateList();
        final TreeSet<UserPostIterator> postIterators =
                new TreeSet<UserPostIterator>(new PostIteratorComparator());

        // load first user by replica
        UserProxy pCrrUser = null;
        UserPostIterator userPostIterator;
        Vertex vReplica =
                Walker.nextVertex(vReader, EdgeType.GRAPHITY.getLabel());
        if (vReplica != null) {
            pCrrUser =
                    new UserProxy(Walker.nextVertex(vReplica,
                            EdgeType.REPLICA.getLabel()));
            userPostIterator = new UserPostIterator(pCrrUser);
            userPostIterator.setReplicaVertex(vReplica);

            if (userPostIterator.hasNext()) {
                postIterators.add(userPostIterator);
            }
        }

        // handle user queue
        UserProxy pPrevUser = pCrrUser;
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
                        Walker.nextVertex(userPostIterator.getReplicaVertex(),
                                EdgeType.GRAPHITY.getLabel());
                // check if additional user existing
                if (vReplica != null) {
                    pCrrUser =
                            new UserProxy(Walker.nextVertex(vReplica,
                                    EdgeType.REPLICA.getLabel()));
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
                }
            }
        }

        //            // access single stream only
        //            final UserProxy posterNode = new UserProxy(nReader);
        //            UserPostIterator postIterator = new UserPostIterator(posterNode);
        //
        //            while ((statusUpdates.size() < numStatusUpdates)
        //                    && postIterator.hasNext()) {
        //                statusUpdates.add(postIterator.next().getStatusUpdate());
        //            }

        return statusUpdates;
    }

    /**
     * Retrieves the timestamp of the last recent status update of the user
     * specified.
     * 
     * @param userReplica
     *            replica of the user
     * @return timestamp of the user's last recent status update
     */
    private static long getLastUpdateByReplica(final Vertex userReplica) {
        final Vertex user =
                Walker.nextVertex(userReplica, EdgeType.REPLICA.getLabel());
        UserProxy pUser = new UserProxy(user);
        return pUser.getLastPostTimestamp();
    }
}
