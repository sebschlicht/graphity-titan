package de.uniko.sebschlicht.graphity.titan.bootstrap;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;

import de.uniko.sebschlicht.graphity.bootstrap.BootstrapClient;
import de.uniko.sebschlicht.graphity.bootstrap.User;
import de.uniko.sebschlicht.graphity.titan.EdgeType;
import de.uniko.sebschlicht.graphity.titan.model.StatusUpdateProxy;
import de.uniko.sebschlicht.graphity.titan.model.UserProxy;

public class TitanBootstrapper extends BootstrapClient {

    private long _vertexId;

    private long _edgeId;

    private BatchGraph<TitanGraph> _batchGraph;

    public TitanBootstrapper(
            String configPath) {
        TitanGraph graph = TitanFactory.open(configPath);
        TitanManagement mgmt = graph.getManagementSystem();

        // create edge labels
        mgmt.makeEdgeLabel(EdgeType.PUBLISHED.getLabel())
                .multiplicity(Multiplicity.SIMPLE).make();
        mgmt.makeEdgeLabel(EdgeType.FOLLOWS.getLabel())
                .multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel(EdgeType.GRAPHITY.getLabel())
                .multiplicity(Multiplicity.SIMPLE).make();
        mgmt.makeEdgeLabel(EdgeType.REPLICA.getLabel())
                .multiplicity(Multiplicity.MULTI).make();

        // create vertex properties and indices
        PropertyKey userIdKey =
                mgmt.makePropertyKey(UserProxy.PROP_IDENTIFIER)
                        .dataType(Long.class).make();
        mgmt.makePropertyKey(UserProxy.PROP_LAST_STREAM_UDPATE)
                .dataType(Long.class).make();
        mgmt.makePropertyKey(StatusUpdateProxy.PROP_PUBLISHED)
                .dataType(Long.class).make();
        mgmt.makePropertyKey(StatusUpdateProxy.PROP_MESSAGE)
                .dataType(String.class).make();

        // create user identifier index
        mgmt.buildIndex("user.id", Vertex.class).addKey(userIdKey).unique()
                .buildCompositeIndex();

        mgmt.commit();

        _vertexId = 1;
        _edgeId = 1;
        _batchGraph = new BatchGraph<>(graph, VertexIDType.NUMBER, 10000);
    }

    public void shutdown() {
        _batchGraph.shutdown();
    }

    @Override
    protected long createUsers() {
        long numUsers = 0, nodeId;
        Vertex vertex;
        Map<String, Object> userProperties;
        for (User user : _users.getUsers()) {
            userProperties = new HashMap<>();
            userProperties.put(UserProxy.PROP_IDENTIFIER, user.getId());
            vertex = _batchGraph.addVertex(_vertexId++, userProperties);
            nodeId = (long) vertex.getId();
            user.setNodeId(nodeId);
            numUsers += 1;
        }
        return numUsers;
    }

    @Override
    protected long createSubscriptions() {
        long numSubscriptions = 0;
        Vertex outVertex, inVertex;
        for (User user : _users.getUsers()) {
            long[] subscriptions = user.getSubscriptions();
            if (subscriptions == null) {// can this happen?
                continue;
            }
            if (!IS_GRAPHITY) {// WriteOptimizedGraphity
                for (long idFollowed : subscriptions) {
                    User followed = _users.getUser(idFollowed);
                    outVertex = _batchGraph.getVertex(user.getNodeId());
                    inVertex = _batchGraph.getVertex(followed.getNodeId());
                    if (outVertex == null || inVertex == null) {
                        throw new IllegalStateException(
                                "user vertex is missing");
                    }
                    _batchGraph.addEdge(_edgeId++, outVertex, inVertex,
                            EdgeType.FOLLOWS.getLabel());
                    numSubscriptions += 1;
                }
            } else {// ReadOptimizedGraphity
                throw new IllegalStateException("can not subscribe");
            }
        }
        return numSubscriptions;
    }

    @Override
    protected long createPosts() {
        long numTotalPosts = 0;
        Vertex vertex;
        Map<String, Object> postProperties;
        long tsLastPost = System.currentTimeMillis();
        long nodeId;
        for (User user : _users.getUsers()) {
            long[] userPostNodes = user.getPostNodeIds();
            for (int iPost = 0; iPost < userPostNodes.length; ++iPost) {
                postProperties = new HashMap<>();
                postProperties
                        .put(StatusUpdateProxy.PROP_PUBLISHED, tsLastPost);
                postProperties.put(StatusUpdateProxy.PROP_MESSAGE,
                        generatePostMessage(140));
                vertex = _batchGraph.addVertex(_vertexId++, postProperties);
                nodeId = (long) vertex.getId();
                userPostNodes[iPost] = nodeId;
                tsLastPost += 1;
                numTotalPosts += 1;
            }
            //TODO we could create posts first and could save the timestamp for user instead
            // update last_post
            _batchGraph.getVertex(user.getNodeId()).setProperty(
                    UserProxy.PROP_LAST_STREAM_UDPATE, tsLastPost - 1);
        }
        return numTotalPosts;
    }

    @Override
    protected long linkPosts() {
        long numTotalPosts = 0;
        Vertex outVertex, inVertex;
        for (User user : _users.getUsers()) {
            long[] postNodeIds = user.getPostNodeIds();
            if (postNodeIds == null) {// should not happen
                continue;
            }

            for (int iPost = 0; iPost < postNodeIds.length; ++iPost) {
                if (iPost + 1 < postNodeIds.length) {// newerPost -> olderPost
                    outVertex = _batchGraph.getVertex(postNodeIds[iPost + 1]);
                    inVertex = _batchGraph.getVertex(postNodeIds[iPost]);
                    if (outVertex == null || inVertex == null) {
                        throw new IllegalStateException(
                                "news item vertex is missing");
                    }
                    _batchGraph.addEdge(_edgeId++, outVertex, inVertex,
                            EdgeType.PUBLISHED.getLabel());
                } else {// user -> newestPost
                    outVertex = _batchGraph.getVertex(user.getNodeId());
                    inVertex = _batchGraph.getVertex(postNodeIds[iPost]);
                    if (outVertex == null || inVertex == null) {
                        throw new IllegalStateException(
                                "user or news item vertex is missing");
                    }
                    _batchGraph.addEdge(_edgeId++, outVertex, inVertex,
                            EdgeType.PUBLISHED.getLabel());
                }
            }
            numTotalPosts += postNodeIds.length;
        }
        return numTotalPosts;
    }

    private static boolean IS_SHUT_DOWN = false;

    private static void shutdown(TitanBootstrapper bootstrapClient) {
        if (!IS_SHUT_DOWN) {
            System.out.println("process finished. making persistent...");
            bootstrapClient.shutdown();
            System.out.println("exited.");
            IS_SHUT_DOWN = true;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out
                    .println("usage: TitanBootstrapper <pathBootstrapLog> <pathTitanConfig>");
            throw new IllegalArgumentException("invalid number of arguments");
        }
        File fBootstrapLog = new File(args[0]);
        File fConfiguration = new File(args[1]);
        // only one bootstrap client shall run at once!
        final TitanBootstrapper bootstrapClient =
                new TitanBootstrapper(fConfiguration.getAbsolutePath());

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                shutdown(bootstrapClient);
            }
        });
        System.out.println("database ready.");
        bootstrapClient.bootstrap(fBootstrapLog);
        shutdown(bootstrapClient);
    }
}
