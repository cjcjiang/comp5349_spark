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
        final Float confidence_default = 0.6f;
        final Float confidence_user;
        final String inputDataPath;
        final String outputDataPath;
        final String inputDataPath_default = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/user/yjia4072/spark_test/";
        final String outputDataPath_default = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/user/yjia4072/spark_test/";

        if(args.length==3){
            inputDataPath = args[0];
            outputDataPath = args[1];
            confidence_user = Float.parseFloat(args[2]);
            System.out.println("The confidence threshold is set to: " + confidence_user);
        }else{
            inputDataPath = inputDataPath_default;
            outputDataPath = outputDataPath_default;
            confidence_user = confidence_default;
            System.out.println("Wrong command, all things are set to default.");
            System.out.println("The confidence threshold is set to: " + confidence_user);
        }

        SparkConf conf = new SparkConf();
        conf.setAppName("LAB457_GP6_AS3_TaskThree_Association_Rule_Generation");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // get the results from task two
        JavaRDD<String> freqItemSet = sc.textFile(inputDataPath + "task_two_result/part-00000");
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

        final Integer k = suppItemSet
                .map(tuple -> {
                    Integer list_size = tuple._1.size();
                    return list_size;
                })
                .max(new KMaxComparator());

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

        JavaRDD<String> output = sc
                .parallelize(rule_result)
                .filter(tuple -> {
                    Float confidence_result = tuple._1;
                    if(confidence_result>=confidence_user){
                        return true;
                    }else{
                        return false;
                    }
                })
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
        output.coalesce(1).saveAsTextFile(outputDataPath + "task_three_result");
        sc.close();
    }
}