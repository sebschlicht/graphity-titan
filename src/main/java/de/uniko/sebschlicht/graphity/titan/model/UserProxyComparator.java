package de.uniko.sebschlicht.graphity.titan.model;

import java.util.Comparator;

public class UserProxyComparator implements Comparator<UserProxy> {

    @Override
    public int compare(UserProxy u1, UserProxy u2) {
        if (u1.getLastPostTimestamp() > u2.getLastPostTimestamp()) {
            return 1;
        } else {
            return -1;
        }
    }
}
