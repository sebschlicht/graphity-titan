package de.uniko.sebschlicht.graphity.titan;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import de.uniko.sebschlicht.graphity.titan.model.VersionedEdge;

/**
 * graph walker for Titan
 * 
 * @author sebschlicht
 * 
 */
public abstract class Walker {

    /**
     * Walks along an edge type to the next vertex.
     * 
     * @param sourceVertex
     *            vertex to start from
     * @param edgeLabel
     *            label of the edge to walk along
     * @return next vertex the edge specified directs to<br>
     *         <b>null</b> - if the start vertex has no such edge directing out
     */
    public static Vertex nextVertex(Vertex sourceVertex, String edgeLabel) {
        for (Vertex destinationNode : sourceVertex.getVertices(Direction.OUT,
                edgeLabel)) {
            return destinationNode;
        }
        return null;
    }

    /**
     * Walks along the most recent edge of an edge type to the next vertex.
     * 
     * @param sourceVertex
     *            vertex to start from
     * @param edgeLabel
     *            label of the edge to walk along
     * @return vertex the most recent edge specified directs to<br>
     *         <b>null</b> - if the start vertex has no such edge directing out
     */
    public static Vertex nextMostRecentVertex(
            Vertex sourceVertex,
            String edgeLabel) {
        VersionedEdge mostRecentEdge =
                getMostRecentEdge(sourceVertex, Direction.OUT, edgeLabel);
        return (mostRecentEdge != null) ? mostRecentEdge.getEdge().getVertex(
                Direction.IN) : null;
    }

    /**
     * Walks backwards along an edge type to the previous vertex.
     * 
     * @param sourceVertex
     *            vertex to start from
     * @param edgeLabel
     *            label of the edge to walk along
     * @return previous vertex the edge specified directs from<br>
     *         <b>null</b> - if the start vertex has no such edge directing in
     */
    public static Vertex previousVertex(Vertex sourceVertex, String edgeLabel) {
        for (Vertex destinationNode : sourceVertex.getVertices(Direction.IN,
                edgeLabel)) {
            return destinationNode;
        }
        return null;
    }

    /**
     * Walks backwards along the most recent edge of an edge type to the
     * previous vertex.
     * 
     * @param sourceVertex
     *            vertex to start from
     * @param edgeLabel
     *            label of the edge to walk along
     * @return vertex the most recent edge specified directs from<br>
     *         <b>null</b> - if the start vertex has no such edge directing in
     */
    public static Vertex previousMostRecentVertex(
            Vertex sourceVertex,
            String edgeLabel) {
        VersionedEdge mostRecentEdge =
                getMostRecentEdge(sourceVertex, Direction.IN, edgeLabel);
        return (mostRecentEdge != null) ? mostRecentEdge.getEdge().getVertex(
                Direction.OUT) : null;
    }

    /**
     * Removes the first edge matching the given criteria retrieved by
     * <i>getEdges</i>.
     * 
     * @param sourceVertex
     *            vertex to start from
     * @param direction
     *            direction the edge has for source vertex
     * @param edgeLabel
     *            label of the edge to remove
     */
    public static void removeSingleEdge(
            Vertex sourceVertex,
            Direction direction,
            String edgeLabel) {
        for (Edge edge : sourceVertex.getEdges(direction, edgeLabel)) {
            edge.remove();
            break;
        }
    }

    /**
     * Removes the most recent edge matching the given criteria retrieved by
     * <i>getEdges</i>.
     * 
     * @param sourceVertex
     *            vertex to start from
     * @param direction
     *            direction the edge has for source vertex
     * @param edgeLabel
     *            label of the edge to remove
     */
    public static void removeMostRecentEdge(
            Vertex sourceVertex,
            Direction direction,
            String edgeLabel) {
        VersionedEdge mostRecentEdge =
                getMostRecentEdge(sourceVertex, direction, edgeLabel);
        if (mostRecentEdge != null) {
            mostRecentEdge.getEdge().remove();
        }
    }

    public static VersionedEdge getMostRecentEdge(
            Vertex sourceVertex,
            Direction direction,
            String edgeLabel) {
        //TODO should we use vertex-centric indices?
        VersionedEdge mostRecentEdge = null;
        for (Edge edge : sourceVertex.getEdges(direction, edgeLabel)) {
            if (edge == null) {//FIXME WTF!
                continue;
            }
            if (mostRecentEdge == null) {// first edge
                mostRecentEdge = new VersionedEdge(edge);
            } else {// n-th edge
                VersionedEdge crrEdge = new VersionedEdge(edge);
                if (crrEdge.getTimestamp() > mostRecentEdge.getTimestamp()) {
                    mostRecentEdge = crrEdge;
                }
            }
        }
        return mostRecentEdge;
    }
}
