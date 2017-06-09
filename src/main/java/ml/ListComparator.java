package ml;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Created by JIANG on 2017/6/9.
 */
public class ListComparator implements Comparator<List<String>>, Serializable {
    public int compare(List<String> l1, List<String> l2) {
        int size_1 = l1.size();
        int size_2 = l2.size();
        if(size_1!=size_2){
            return size_1-size_2;
        }else{
            int return_order = 0;
            boolean flag = true;
            int i = 0;
            while(i<size_1&&flag){
                if(l1.get(i).equals(l2.get(i))){
                    flag = true;
                }else{
                    flag = false;
                    return_order = Integer.parseInt(l1.get(i)) - Integer.parseInt(l2.get(i));
                }
                i++;
            }
            return return_order;
        }
    }
}
