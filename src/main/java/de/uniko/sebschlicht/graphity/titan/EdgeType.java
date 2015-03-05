package de.uniko.sebschlicht.graphity.titan;

public enum EdgeType {

    FOLLOWS("e_follows"),

    PUBLISHED("e_published"),

    GRAPHITY("e_graphity"),

    REPLICA("e_replica");

    protected String label;

    private EdgeType(
            String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return getLabel();
    }
}
