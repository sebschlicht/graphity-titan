package de.uniko.sebschlicht.graphity.titan.model;

import java.util.Iterator;

public interface PostIterator extends Iterator<StatusUpdateProxy> {

    long getCrrPublished();
}
