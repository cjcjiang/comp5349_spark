package ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class TaskThree {
    public static void main(String[] args) {
        // TODO: handle k and getcombination's k
        final Float confidence_default = 0.6f;
        final Float confidence_user;
        final String inputDataPath = args[0];
        final String outputDataPath = args[1];
        String valueOfK = args[2];
        final int k = Integer.parseInt(valueOfK);
//        confidence_user = Float.parseFloat(args[2]);
        SparkConf conf = new SparkConf();
        // we can set lots of information here
        conf.setAppName("LAB457-GP6-TaskThree-Association_Rule_Generation");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // get the results from task two
        JavaRDD<String> freqItemSet = sc.textFile(inputDataPath);
        // format <support, itemSet>
        JavaPairRDD<List<String>, Integer> suppItemSet = freqItemSet
                        // first split the support number and the itemSet
                .flatMapToPair(s->{
                    String[] array = s.split("\t");
                    Integer support = Integer.parseInt(array[0]);
                    List<Tuple2<List<String>, Integer>> return_list = new ArrayList<>();
                    for(int i=1;i<array.length;i++){
                        List<String> part_list_int = new ArrayList<>();
                        String[] gene_set_array = array[i].split(";");
                        for(String single_gene : gene_set_array){
                            part_list_int.add(single_gene);
                        }
                        return_list.add(new Tuple2<>(part_list_int, support));
                    }
                    return return_list.iterator();
                })
                .cache();

        List<Tuple2<Float, Tuple2<List<String>,List<String>>>> rule_result = new ArrayList<>();

        // Use suppItemSet to have the size k
        // Now, just let the user input this "k"
        for(int i=2;i<=k;i++){
            int k_this = i;
            int k_last = i-1;
            List<Tuple2<List<String>, Integer>> gene_set_size_k_last_support_pair_rdd = suppItemSet
                    .filter(tuple -> {
                        int list_size = tuple._1.size();
                        if(list_size<=k_last){
                            return true;
                        }else{
                            return false;
                        }
                    })
                    .collect();
            Broadcast<List<Tuple2<List<String>, Integer>>> bc_gene_set_size_k_last_support_pair_rdd = sc.broadcast(gene_set_size_k_last_support_pair_rdd);

            List<Tuple2<Float, Tuple2<List<String>,List<String>>>> gene_set_size_k_support_list = suppItemSet
                    .filter(tuple -> {
                        int list_size = tuple._1.size();
                        if(list_size==k_this){
                            return true;
                        }else{
                            return false;
                        }
                    })
                    .flatMapToPair(tuple ->{
                        List<Tuple2<Float, Tuple2<List<String>,List<String>>>> return_list = new ArrayList<>();
                        Integer s_support = tuple._2;
                        List<String> gene_set = tuple._1;
                        List<Tuple2<List<String>, Integer>> bc_gene_set_size_k_last_support_pair_rdd_value = bc_gene_set_size_k_last_support_pair_rdd.value();
                        List<List<String>> checking_gene_set_list = CombinationGenerator.getCombinations(gene_set, k_this);
                        for(List<String> checking_gene_set : checking_gene_set_list){
                            for(Tuple2<List<String>, Integer> tuple_gene_set_size_k_last_support : bc_gene_set_size_k_last_support_pair_rdd_value){
                                Set<String> tuple_gene_set_size_k_last_support_set = new HashSet<>(tuple_gene_set_size_k_last_support._1);
                                Set<String> checking_gene_set_set = new HashSet<>(checking_gene_set);
                                boolean flag = tuple_gene_set_size_k_last_support_set.equals(checking_gene_set_set);
                                if(flag){
                                    Integer r_support = tuple_gene_set_size_k_last_support._2;
                                    Float conf_result = s_support.floatValue()/r_support.floatValue();
                                    List<String> s_minus_r_list = new ArrayList<>();
                                    s_minus_r_list.addAll(gene_set);
                                    s_minus_r_list.removeAll(checking_gene_set);
                                    Tuple2<List<String>,List<String>> inner_temp = new Tuple2<>(checking_gene_set, s_minus_r_list);
                                    Tuple2<Float, Tuple2<List<String>,List<String>>> temp = new Tuple2<>(conf_result, inner_temp);
                                    return_list.add(temp);
                                }
                            }
                        }
                        return return_list.iterator();
                    })
                    .collect();

            rule_result.addAll(gene_set_size_k_support_list);
        }

        // TODO: add filter here
        JavaRDD<String> output = sc
                .parallelize(rule_result)
                .mapToPair(tuple->tuple)
                .sortByKey(false)
                .map(tuple -> {
                    Float confidence_result = tuple._1;
                    Tuple2<List<String>,List<String>> temp = tuple._2;
                    List<String> r_list = temp._1;
                    List<String> s_minus_r_list = temp._2;
                    String r_merge = "";
                    for(String s : r_list){
                        if(r_merge.equals("")){
                            r_merge = s;
                        }else{
                            r_merge = r_merge + " " + s;
                        }
                    }
                    String s_minus_r_merge = "";
                    for(String s : s_minus_r_list){
                        if(s_minus_r_merge.equals("")){
                            s_minus_r_merge = s;
                        }else{
                            s_minus_r_merge = s_minus_r_merge + " " + s;
                        }
                    }
                    String out_string = r_merge + "\t" + s_minus_r_merge + "\t" + confidence_result;
                    return out_string;
                });
        output.coalesce(1).saveAsTextFile(outputDataPath);
        sc.close();
    }
}