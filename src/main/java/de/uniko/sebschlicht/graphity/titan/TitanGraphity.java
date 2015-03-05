package de.uniko.sebschlicht.graphity.titan;

import java.util.concurrent.atomic.AtomicInteger;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;

import de.metalcon.domain.Muid;
import de.metalcon.domain.UidType;
import de.metalcon.domain.helper.UidConverter;
import de.metalcon.exceptions.ServiceOverloadedException;
import de.uniko.sebschlicht.graphity.Graphity;
import de.uniko.sebschlicht.graphity.exception.IllegalUserIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownFollowedIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownFollowingIdException;
import de.uniko.sebschlicht.graphity.exception.UnknownReaderIdException;
import de.uniko.sebschlicht.graphity.titan.model.UserProxy;
import de.uniko.sebschlicht.socialnet.StatusUpdate;
import de.uniko.sebschlicht.socialnet.StatusUpdateList;

/**
 * social graph for Graphity implementations
 * 
 * @author sebschlicht
 * 
 */
public abstract class TitanGraphity extends Graphity {

    /**
     * unique Titan instance identifier
     */
    protected static byte TITAN_ID;

    /**
     * timestamp of last news item MUID creation
     */
    protected static int LAST_MUID_CREATION_TIME = 0;

    /**
     * id of last news item MUID
     */
    protected static AtomicInteger LAST_MUID_ID = new AtomicInteger(0);

    /**
     * Titan graph database holding the social network graph
     */
    protected TitanGraph graphDb;

    protected TitanGraphIndex userIndex;

    /**
     * Creates a new Graphity instance using the Titan database provided.
     * 
     * @param graphDb
     *            Titan graph database holding any Graphity social network graph
     *            to operate on
     */
    public TitanGraphity(
            TitanGraph graphDb) {
        this.graphDb = graphDb;
    }

    /**
     * @return Titan graph database holding the social network graph
     */
    public TitanGraph getGraph() {
        return graphDb;
    }

    /**
     * Initializes the social graph instance in order to access and manipulate
     * the social network graph.
     */
    @Override
    public void init() {
        TitanManagement mgmt = graphDb.getManagementSystem();

        /**
         * Maybe this must not be done here when using eventually-consistent
         * storage backend.
         * */
        //          // create edge labels
        //          mgmt.makeEdgeLabel(EdgeType.PUBLISHED.getLabel())
        //          .multiplicity(Multiplicity.SIMPLE).make();
        //          mgmt.makeEdgeLabel(EdgeType.FOLLOWS.getLabel())
        //          .multiplicity(Multiplicity.MULTI).make();
        //          
        //          // create vertex properties and indices
        //          mgmt.makePropertyKey(StatusUpdateProxy.PROP_PUBLISHED)
        //          .dataType(Long.class).make();
        //          mgmt.makePropertyKey(StatusUpdateProxy.PROP_MESSAGE)
        //          .dataType(String.class).make();

        userIndex = mgmt.getGraphIndex(INDEX_USER_ID_NAME);
        //TODO will index ever exist on startup?
        if (userIndex == null) {
            //TODO will this exception get thrown? does this cause our problems?
            //throw new IllegalStateException("index offline");
            //            PropertyKey name =
            //                    mgmt.makePropertyKey(UserProxy.PROP_IDENTIFIER)
            //                            .dataType(Long.class).make();
            //            //TODO how to limit index to certain vertex type?
            //            userIndex =
            //                    mgmt.buildIndex(INDEX_USER_ID_NAME, Vertex.class)
            //                            .addKey(name).unique().buildCompositeIndex();
            //            mgmt.commit();
        }
        // we did not change anything
        mgmt.rollback();
    }

    /**
     * Creates a user that can act in the social network.
     * 
     * @param userIdentifier
     *            identifier of the new user
     * @return user vertex
     * @throws IllegalUserIdException
     *             if the user identifier is invalid
     */
    public Vertex createUser(String userIdentifier)
            throws IllegalUserIdException {
        if (userIdentifier == null) {
            throw new IllegalUserIdException(userIdentifier);
        }
        try {
            long idUser = Long.valueOf(userIdentifier);
            if (idUser > 0) {
                Vertex vUser = graphDb.addVertex(null);
                vUser.setProperty(UserProxy.PROP_IDENTIFIER, idUser);
                return vUser;
            }
        } catch (NumberFormatException e) {
            // exception gets thrown below
        }
        throw new IllegalUserIdException(userIdentifier);
    }

    /**
     * Searches the social network graph for an user.
     * 
     * @param userIdentifier
     *            identifier of the user searched
     * @return user node - if the user is existing in social network graph<br>
     *         <b>null</b> - if there is no vertex representing the user
     *         specified
     * @throws IllegalUserIdException
     *             if the user identifier is invalid
     */
    protected Vertex findUser(String userIdentifier)
            throws IllegalUserIdException {
        if (userIdentifier == null) {
            throw new IllegalUserIdException(userIdentifier);
        }
        Iterable<Vertex> vUsers =
                graphDb.getVertices(UserProxy.PROP_IDENTIFIER, userIdentifier);
        for (Vertex vUser : vUsers) {
            return vUser;
        }
        return null;
    }

    /**
     * Loads a user from social network or lazily creates a new one.
     * 
     * @param userIdentifier
     *            identifier of the user to interact with
     * @return user vertex - existing or created vertex representing the user
     * @throws IllegalUserIdException
     *             if the user must be created and the identifier is invalid
     */
    protected Vertex loadUser(String userIdentifier)
            throws IllegalUserIdException {
        Vertex vUser = findUser(userIdentifier);
        if (vUser != null) {
            // user is already existing
            return vUser;
        }
        return createUser(userIdentifier);
    }

    @Override
    public boolean addUser(String userIdentifier) throws IllegalUserIdException {
        Vertex vUser = findUser(userIdentifier);
        if (vUser == null) {
            // user identifier not in use yet
            createUser(userIdentifier);
            return true;
        }
        return false;
    }

    @Override
    public boolean addFollowship(String idFollowing, String idFollowed)
            throws IllegalUserIdException {
        return addFollowship(idFollowing, idFollowed, true);
    }

    public boolean addFollowship(
            String idFollowing,
            String idFollowed,
            boolean autoCommit) throws IllegalUserIdException {
        try {
            //TODO can not create locks manually, but we could force lock via write access
            Vertex vFollowing = loadUser(idFollowing);
            Vertex vFollowed = loadUser(idFollowed);
            if (addFollowship(vFollowing, vFollowed)) {
                if (autoCommit) {
                    graphDb.commit();
                }
                return true;
            }
            // no changes to commit/roll back
            return false;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Adds a followship between two user vertices to the social network graph.
     * 
     * @param vFollowing
     *            vertex of the user that wants to follow another user
     * @param vFollowed
     *            vertex of the user that will be followed
     * @return true - if the followship was successfully created<br>
     *         false - if this followship is already existing
     */
    abstract protected boolean
        addFollowship(Vertex vFollowing, Vertex vFollowed);

    @Override
    public boolean removeFollowship(String idFollowing, String idFollowed)
            throws UnknownFollowingIdException, UnknownFollowedIdException,
            IllegalUserIdException {
        return removeFollowship(idFollowing, idFollowed, true);
    }

    public boolean removeFollowship(
            String idFollowing,
            String idFollowed,
            boolean autoCommit) throws UnknownFollowingIdException,
            UnknownFollowedIdException, IllegalUserIdException {
        try {
            Vertex vFollowing = findUser(idFollowing);
            if (vFollowing == null) {
                throw new UnknownFollowingIdException(idFollowing);
            }
            Vertex vFollowed = findUser(idFollowed);
            if (vFollowed == null) {
                throw new UnknownFollowedIdException(idFollowed);
            }

            //TODO can not create locks manually, but we could force lock via write access

            if (removeFollowship(vFollowing, vFollowed)) {
                if (autoCommit) {
                    graphDb.commit();
                }
                return true;
            }
            // no changes to commit/roll back
            return false;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Removes a followship between two user vertices from the social network
     * graph.
     * 
     * @param vFollowing
     *            vertex of the user that wants to unfollow a user
     * @param vFollowed
     *            vertex of the user that will be unfollowed
     * @return true - if the followship was successfully removed<br>
     *         false - if this followship is not existing
     */
    abstract protected boolean removeFollowship(
            Vertex vFollowing,
            Vertex vFollowed);

    @Override
    public long addStatusUpdate(String idAuthor, String message)
            throws IllegalUserIdException {
        return addStatusUpdate(idAuthor, message, true);
    }

    public long addStatusUpdate(
            String idAuthor,
            String message,
            boolean autoCommit) throws IllegalUserIdException {
        Vertex vAuthor = loadUser(idAuthor);
        //TODO can not create locks manually, but we could force lock via write access
        StatusUpdate statusUpdate =
                new StatusUpdate(idAuthor, System.currentTimeMillis(), message);
        long idStatusUpdate = addStatusUpdate(vAuthor, statusUpdate);
        if (autoCommit && idStatusUpdate != 0) {
            graphDb.commit();
        }
        // nothing to commit/roll back
        return idStatusUpdate;
    }

    /**
     * Adds a status update vertex to the social network.
     * 
     * @param vAuthor
     *            user vertex of the status update author
     * @param statusUpdate
     *            status update data
     * @return identifier of the status update vertex
     */
    abstract protected long addStatusUpdate(
            Vertex vAuthor,
            StatusUpdate statusUpdate);

    @Override
    public StatusUpdateList readStatusUpdates(
            String idReader,
            int numStatusUpdates) throws UnknownReaderIdException,
            IllegalUserIdException {
        Vertex vReader = findUser(idReader);
        if (vReader != null) {
            return readStatusUpdates(vReader, numStatusUpdates);
        }
        throw new UnknownReaderIdException(idReader);
    }

    abstract protected StatusUpdateList readStatusUpdates(
            Vertex vReader,
            int numStatusUpdates);

    public static void setTitanId(byte titanId) {
        TITAN_ID = titanId;
    }

    public static Muid generateMuid(UidType type)
            throws ServiceOverloadedException {
        int timestamp = (int) (System.currentTimeMillis() / 1000);
        short id = 0;
        if (timestamp == LAST_MUID_CREATION_TIME) {
            if (LAST_MUID_ID.intValue() == UidConverter.getMaximumMuidID()) {
                // more than 65535 MUIDs / second
                // we could separate this number by type
                throw new ServiceOverloadedException(
                        "Alreay created more then "
                                + UidConverter.getMaximumMuidID()
                                + " during the current second");
            }
            id = (short) LAST_MUID_ID.incrementAndGet();
        } else {
            LAST_MUID_ID.set(0);
            LAST_MUID_CREATION_TIME = timestamp;
        }
        return Muid.createFromID(UidConverter.calculateMuidWithoutChecking(
                type.getRawIdentifier(), TITAN_ID, timestamp, id));
    }
}
