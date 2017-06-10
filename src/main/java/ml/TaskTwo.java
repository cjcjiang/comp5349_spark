package ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TaskTwo {
    public static void main(String[] args) {
        final String[] cancer = {"breast-cancer", "prostate-cancer", "pancreatic-cancer", "leukemia", "lymphoma"};
        final Double support_value_default = 0.3;
        final Double support_value;
        final Integer k_default = 5;
        final Integer k_user;
        final String inputDataPath;
        final String outputDataPath;
        final String inputDataPath_default = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/share/genedata/test/";
        final String outputDataPath_default = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au:8020/user/yjia4072/spark_test/";
        Integer k_temp;

        if(args.length==4){
            inputDataPath = args[0];
            outputDataPath = args[1];
            support_value = Double.parseDouble(args[2]);
            k_temp = Integer.parseInt(args[3]);
            System.out.println("The minimum support is set to: " + support_value + "; the maximum itemset size is set to: " + k_temp);
        }else{
            inputDataPath = inputDataPath_default;
            outputDataPath = outputDataPath_default;
            support_value = support_value_default;
            k_temp = k_default;
            System.out.println("Wrong command, all things are set to default.");
            System.out.println("The minimum support is set to: " + support_value + "; the maximum itemset size is set to: " + k_temp);
        }

        SparkConf conf = new SparkConf();

        conf.setAppName("LAB457_GP6_AS3_TaskTwo");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> gene_express_value_data_raw = sc.textFile(inputDataPath+"GEO.txt"),
                patient_id_data_raw = sc.textFile(inputDataPath + "PatientMetaData.txt");

        String gene_header = gene_express_value_data_raw.first();
        String patient_header = patient_id_data_raw.first();

        // Filter out the header line
        // Select out lines whose expression_value is bigger than 1250000
        // Make patients' id as the key, gene id with strong expression value as the value
        JavaPairRDD<String, String> genes_strongly_expressed = gene_express_value_data_raw
                .filter(s ->!s.equalsIgnoreCase(gene_header))
                .filter(s -> {
                    String[] values = s.split(",");
                    float expression_value = Float.parseFloat(values[2]);
                    boolean flag = false;
                    if(expression_value>=1250000){
                        flag = true;
                    }
                    return flag;
                }).mapToPair(s -> {
                    String[] values = s.split(",");
                    String patient_id = values[0];
                    String gene_id = values[1];
                    Tuple2<String, String> temp = new Tuple2<>(patient_id,gene_id);
                    return temp;
                });

        // Filter out the header line
        // Select out the patients' id who have cancer
        // Make patients' id as the key, diseases as the value
        JavaPairRDD<String, String> cancer_patient_id = patient_id_data_raw.filter(s ->{
            String[] values = s.split(",");
            String[] diseases = values[4].split(" ");
            boolean flag = false;
            for(String disease : diseases){
                if(Arrays.asList(cancer).contains(disease)){
                    flag = true;
                }
            }
            return ((!s.equalsIgnoreCase(patient_header))&&flag);
        }).mapToPair(s -> {
            String[] values = s.split(",");
            String patient_id = values[0];
            String diseases = values[4];
            Tuple2<String, String> temp = new Tuple2<>(patient_id,diseases);
            return temp;
        });

        JavaPairRDD<String, String> pid_gid = genes_strongly_expressed
                .join(cancer_patient_id)
                .mapToPair(tuple -> {
                    String pid = tuple._1;
                    String gid = tuple._2._1;
                    return new Tuple2<>(pid, gid);
                })
                .cache();

        // Aggregating for counting the support num and k_max
        JavaPairRDD<String, List<String>> counting_purpose = pid_gid
                .aggregateByKey(
                        new ArrayList<>(),
                        (last_merge_value, in_value) -> {
                            ArrayList<String> temp = new ArrayList<>();
                            temp.addAll(last_merge_value);
                            temp.add(in_value);
                            return temp;
                        },
                        (merge_value_1, merge_value_2) ->{
                            ArrayList<String> temp = new ArrayList<>();
                            temp.addAll(merge_value_1);
                            temp.addAll(merge_value_2);
                            return temp;
                        }
                );
        // Count all the cancer patients in geo.txt
        Long checking_cancer_patient_num = counting_purpose
                .keys()
                .count();
        final Long support_num = new Double(checking_cancer_patient_num * support_value).longValue();
        System.out.println("The support_num is: " + support_num);
        // Get the largest number for item set size k
        final Integer k_max = counting_purpose
                .values()
                .map(list -> {
                    Integer list_size = list.size();
                    return list_size;
                })
                .max(new KMaxComparator());
        if(k_temp>k_max){
            k_user = k_max;
            System.out.println("The max k should be: " + k_max);
            System.out.println("The max k is changed to: " + k_user);
        }else{
            k_user = k_temp;
        }

        // Get k=1 filtered item-set
        // Key: gene set, at this time, the list only contains one gene
        // Value: pid list, the list contains all the patients who have this gene set
        JavaPairRDD<List<String>, List<String>> gene_set_size_one_pid_temp = pid_gid
                .mapToPair(tuple -> {
                    String pid = tuple._1;
                    String gid = tuple._2;
                    List<String> gid_list = new ArrayList<>();
                    gid_list.add(gid);
                    return new Tuple2<>(gid_list,pid);
                })
                .aggregateByKey(
                        new ArrayList<String>(),
                        (last_merge_value, in_value) -> {
                            List<String> temp = new ArrayList<>();
                            temp.addAll(last_merge_value);
                            temp.add(in_value);
                            return temp;
                        },
                        (merge_value_1, merge_value_2) ->{
                            List<String> temp = new ArrayList<>();
                            temp.addAll(merge_value_1);
                            temp.addAll(merge_value_2);
                            return temp;
                        }
                );
        JavaPairRDD<List<String>, List<String>> gene_set_size_one_pid = gene_set_size_one_pid_temp
                .filter(tuple -> {
                    Integer gene_set_support_num = tuple._2.size();
                    if(gene_set_support_num<support_num){
                        return false;
                    }else{
                        return true;
                    }
                })
                .sortByKey(new ListComparator())
                // Change the repartition number based on the number of executors
                // The number of executors used should be based on the size of the data
                .repartition(10)
                .cache();

        // Start the iteration
        JavaPairRDD<List<String>, List<String>> gene_set_pid_occ = gene_set_size_one_pid;
        boolean loop_continue_flag = true;
        int i = 2;
        while((i<=k_user)&&loop_continue_flag){
            System.out.println("Checking for candidate item-set with size: " + i);
            int k_last = i - 1;
            JavaPairRDD<List<String>, List<String>> gene_set_size_k_pid;
            if(i==2){
                List<Tuple2<List<String>, List<String>>> gene_set_size_one_pid_collect_list = gene_set_size_one_pid.collect();
                Broadcast<List<Tuple2<List<String>, List<String>>>> bc_gene_set_size_one_pid_collect_list = sc.broadcast(gene_set_size_one_pid_collect_list);
                gene_set_size_k_pid = gene_set_size_one_pid
                        .flatMapToPair(tuple -> {
                            Tuple2<List<String>, List<String>> single_gid_pid_tuple = tuple;
                            List<Tuple2<List<String>, List<String>>> part_gene_set_size_2_list = new ArrayList<>();
                            List<Tuple2<List<String>, List<String>>> bc_gene_set_size_one_pid_collect_list_value = bc_gene_set_size_one_pid_collect_list.value();
                            int ite_start_index = bc_gene_set_size_one_pid_collect_list_value.indexOf(single_gid_pid_tuple) + 1;
                            while(ite_start_index<bc_gene_set_size_one_pid_collect_list_value.size()){
                                List<String> inner_part_gene_set_size_2_list = new ArrayList<>();
                                Tuple2<List<String>, List<String>> inner_single_gene = bc_gene_set_size_one_pid_collect_list_value.get(ite_start_index);
                                List<String> outer_pid = new ArrayList<>();
                                List<String> inner_pid = inner_single_gene._2;
                                outer_pid.addAll(single_gid_pid_tuple._2);
                                outer_pid.retainAll(inner_pid);
                                if(outer_pid.size()!=0){
                                    inner_part_gene_set_size_2_list.add(single_gid_pid_tuple._1.get(0));
                                    inner_part_gene_set_size_2_list.add(inner_single_gene._1.get(0));
                                    part_gene_set_size_2_list.add(new Tuple2<>(inner_part_gene_set_size_2_list, outer_pid));
                                }
                                ite_start_index++;
                            }
                            return part_gene_set_size_2_list.iterator();
                        })
                        .filter(tuple -> {
                            Integer gene_set_support_num = tuple._2.size();
                            if(gene_set_support_num<support_num){
                                return false;
                            }else{
                                return true;
                            }
                        })
                        .sortByKey(new ListComparator());
            }else{
                JavaPairRDD<List<String>, List<String>> gene_set_size_k_last_pid = gene_set_pid_occ
                        .filter(tuple -> {
                            List<String> gene_set_list = tuple._1;
                            int gene_set_size = gene_set_list.size();
                            if(gene_set_size==k_last){
                                return true;
                            }else{
                                return false;
                            }
                        });
                List<Tuple2<List<String>, List<String>>> gene_set_size_k_last_collect_list = gene_set_size_k_last_pid.collect();
                Broadcast<List<Tuple2<List<String>, List<String>>>> bc_gene_set_size_k_last_collect_list =sc.broadcast(gene_set_size_k_last_collect_list);
                gene_set_size_k_pid = gene_set_size_k_last_pid
                        .flatMapToPair(tuple -> {
                            List<Tuple2<List<String>, List<String>>> part_gene_set_size_k_list = new ArrayList<>();
                            List<Tuple2<List<String>, List<String>>> bc_gene_set_size_k_last_collect_list_value = bc_gene_set_size_k_last_collect_list.value();
                            int start_index = bc_gene_set_size_k_last_collect_list_value.indexOf(tuple) + 1;
                            while(start_index<bc_gene_set_size_k_last_collect_list_value.size()){
                                Tuple2<List<String>, List<String>> gene_set_size_k_last_pid_tuple = tuple;
                                Tuple2<List<String>, List<String>> inner_gene_set_size_k_last = bc_gene_set_size_k_last_collect_list_value.get(start_index);
                                boolean flag = true;
                                int size_k_last = inner_gene_set_size_k_last._1.size();
                                for(int p =0;p<size_k_last-1;p++){
                                    String out = gene_set_size_k_last_pid_tuple._1.get(p);
                                    String inner = inner_gene_set_size_k_last._1.get(p);
                                    if(!out.equals(inner)){flag = false;}
                                }
                                if(flag){
                                    List<String> outer_gene_set = new ArrayList<>();
                                    List<String> outer_pid = new ArrayList<>();
                                    outer_gene_set.addAll(gene_set_size_k_last_pid_tuple._1);
                                    outer_pid.addAll(gene_set_size_k_last_pid_tuple._2);
                                    outer_gene_set.add(inner_gene_set_size_k_last._1.get(size_k_last-1));
                                    outer_pid.retainAll(inner_gene_set_size_k_last._2);
                                    if(outer_pid.size()!=0){
                                        part_gene_set_size_k_list.add(new Tuple2<>(outer_gene_set, outer_pid));
                                    }
                                }
                                start_index++;
                            }
                            return part_gene_set_size_k_list.iterator();
                        })
                        .filter(tuple -> {
                            Integer gene_set_support_num = tuple._2.size();
                            if(gene_set_support_num<support_num){
                                return false;
                            }else{
                                return true;
                            }
                        })
                        .sortByKey(new ListComparator());
            }

            // If all the size k gene set is filtered, stop the loop
            long gene_set_size_k_size = gene_set_size_k_pid.count();
            if(gene_set_size_k_size==0){
                loop_continue_flag = false;
                System.out.println("For item-set size " + i + ", none of the candidate has passed the support check, the loop will stop.");
            }
            gene_set_pid_occ = gene_set_pid_occ.union(gene_set_size_k_pid);
            i++;
        }

        // Change gene_set to the output format
        // Input List<String> Integer
        JavaRDD<String> output = gene_set_pid_occ
                .sortByKey(new ListComparator())
                .mapToPair(tuple -> {
                    List<String> gene_set_list = tuple._1;
                    Integer gene_set_num = tuple._2.size();
                    Tuple2<Integer, List<String>> temp = new Tuple2<>(gene_set_num, gene_set_list);
                    return temp;
                })
                .aggregateByKey(
                        "",
                        1,
                        (last_merge_value, in_value) -> {
                            List<String> temp = in_value;
                            String this_merge = "";
                            for(String s : temp){
                                if(this_merge.equals("")){
                                    this_merge = s;
                                }else{
                                    this_merge = this_merge + ";" + s;
                                }
                            }
                            if(last_merge_value.equals("")){
                            }else{
                                this_merge = last_merge_value + "\t" + this_merge;
                            }
                            return this_merge;
                        },
                        (merge_value_1, merge_value_2) -> {
                            String m1 = merge_value_1;
                            String m2 = merge_value_2;
                            String r_s = m1 + "\t" + m2;
                            return r_s;
                        }
                )
                .sortByKey(false)
                .map(tuple->{
                    Integer supp = tuple._1;
                    String gene_set_list = tuple._2;
                    String outer_string_temp = supp + "\t" + gene_set_list;
                    return outer_string_temp;
                });

        output.saveAsTextFile(outputDataPath + "task_two_result");
    }
}