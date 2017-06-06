package ml;

import java.io.Serializable;
import java.util.Comparator;

class KMaxComparator implements Comparator<Integer>, Serializable {
    public int compare(Integer n1, Integer n2) {
        return n1 - n2;
    }
}