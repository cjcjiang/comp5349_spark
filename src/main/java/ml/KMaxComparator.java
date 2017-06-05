package ml;

import scala.Tuple2;
import java.io.Serializable;
import java.util.Comparator;

class KMaxComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
        return t1._2 - t2._2;
    }
}