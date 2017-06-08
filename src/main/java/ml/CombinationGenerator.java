package ml;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CombinationGenerator implements Serializable {
    public static List<List<Object>> getCombinations(List<Object> list, int k_max) {
        List<List<Object>> result = new ArrayList<List<Object>>();
        long n = (long)Math.pow(2,list.size());
        List<Object> combine;
        for (long l=0L; l<n; l++) {
            combine = new ArrayList<Object>();
            for (int i=0; i<list.size(); i++) {
                if ((l>>>i&1) == 1)
                    combine.add(list.get(i));
            }
            result.add(combine);
        }

        List<List<Object>> result_without_zero_one = new ArrayList<List<Object>>();

        for(List<Object> a : result){
            int a_size = a.size();
            if(a_size>1&&a_size<k_max){
                result_without_zero_one.add(a);
            }
        }

        return result_without_zero_one;
    }
}
