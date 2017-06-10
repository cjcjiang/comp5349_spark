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

/*
* GEO.txt
* patientid, geneid, expression value
*
* PatientMetaData.txt
* id, age, gender, postcode, diseases, drug_response
*
* */

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
        // Make patients' id as the key, gene id and expression value flag as the value
        JavaPairRDD<String, Tuple2<String, Integer>> genes_strongly_expressed = gene_express_value_data_raw
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
                    Integer expression_value_flag = 1;
                    Tuple2<String, Integer> temp = new Tuple2<>(gene_id,expression_value_flag);
                    Tuple2<String, Tuple2<String, Integer>> result = new Tuple2<>(patient_id, temp);
                    return result;
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

        JavaPairRDD<String, Tuple2<String, Tuple2<String,Integer>>> cancer_patient_diseases_gene_value = cancer_patient_id.join(genes_strongly_expressed);

        // Prepare for the iteration
        // Input JavaPairRDD<String, Tuple2<String, Tuple2<String,Integer>>>
        // Output JavaPairRDD<String, List<String>>
        // String is patient id, List<String> contains all single genes in one patient
        JavaPairRDD<String, List<String>> patient_single_gene_list_pair_rdd = cancer_patient_diseases_gene_value
                .mapToPair(tuple -> {
                    String patient_id = tuple._1;
                    String gene_id = tuple._2._2._1;
                    List<String> patient_single_gene_list_temp = new ArrayList<>();
                    patient_single_gene_list_temp.add(gene_id);
                    Tuple2<String, List<String>> temp = new Tuple2<>(patient_id, patient_single_gene_list_temp);
                    return temp;
                })
                .reduceByKey((l1,l2) -> {
                    List<String> patient_single_gene_list_temp = new ArrayList<>();
                    patient_single_gene_list_temp.addAll(l1);
                    patient_single_gene_list_temp.addAll(l2);
                    return patient_single_gene_list_temp;
                });

        // Count the amount of the cancer patient that occurs in geo.txt
        Long cancer_patient_num = patient_single_gene_list_pair_rdd.count();
        final Long support_num = new Double(cancer_patient_num * support_value).longValue();
        System.out.println("The support_num is: " + support_num);

        // Prepare for the iteration
        // Cache the JavaRDD contains all patient divided single gene list in memory
        JavaRDD<List<String>> patient_divided_single_gene_list_rdd = patient_single_gene_list_pair_rdd.values().cache();

        // Prepare for the iteration
        // Get the largest number for item set size k
        final Integer k_max = patient_divided_single_gene_list_rdd
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

        // Have the k=1 item sets, also used as the PairRDD to store all the item sets
        // Input JavaRDD<List<String>>
        // First, flatMapToPair Output JavaPairRDD<String,Integer>, first String is gene id, second Integer is 1
        // Second, reduceByKey count the occurrence time of each gene_id
        // Third, filter out all gene_id that the occurrence time is less than the support_num
        // Finally, cache the size k=1 frequent item set
        JavaPairRDD<String,Integer> gene_set_size_1_pair_rdd = patient_divided_single_gene_list_rdd
                .flatMapToPair(list -> {
                    List<Tuple2<String, Integer>> gene_set_size_1_list_temp = new ArrayList<>();
                    for(String s : list){
                        Integer count_num = 1;
                        Tuple2<String, Integer> temp = new Tuple2<>(s, count_num);
                        gene_set_size_1_list_temp.add(temp);
                    }
                    return gene_set_size_1_list_temp.iterator();
                })
                .reduceByKey((n1, n2) -> n1 + n2)
                .filter(tuple -> {
                    Integer gene_support_num = tuple._2;
                    if(gene_support_num<support_num){
                        return false;
                    }else{
                        return true;
                    }
                })
                .sortByKey()
                .cache();

        List<String> single_gene_in_gene_set_size_1_list = gene_set_size_1_pair_rdd.keys().collect();
        Broadcast<List<String>> bc_single_gene_in_gene_set_size_1_list = sc.broadcast(single_gene_in_gene_set_size_1_list);
        // Prepare for iteration
        // Have the k=2 gene set
        // JavaPairRDD<String,Integer> gene_set_size_1_pair_rdd
        JavaRDD<List<String>> gene_set_size_2_rdd = gene_set_size_1_pair_rdd
                .flatMap(tuple -> {
                    String single_gene = tuple._1;
                    List<List<String>> part_gene_set_size_2_list = new ArrayList<>();
                    List<String> bc_single_gene_in_gene_set_size_1_list_value = bc_single_gene_in_gene_set_size_1_list.value();
                    int ite_start_index = bc_single_gene_in_gene_set_size_1_list_value.indexOf(single_gene) + 1;
                    while(ite_start_index<bc_single_gene_in_gene_set_size_1_list_value.size()){
                        List<String> inner_part_gene_set_size_2_list = new ArrayList<>();
                        String inner_single_gene = bc_single_gene_in_gene_set_size_1_list_value.get(ite_start_index);
                        inner_part_gene_set_size_2_list.add(single_gene);
                        inner_part_gene_set_size_2_list.add(inner_single_gene);
                        part_gene_set_size_2_list.add(inner_part_gene_set_size_2_list);
                        ite_start_index++;
                    }
                    return part_gene_set_size_2_list.iterator();
                })
                .cache();

        // Prepare for the iteration
        // Make the single patient gene list only contains the useful gene
        JavaRDD<List<String>> patient_divided_single_gene_list_int_pair_rdd = patient_divided_single_gene_list_rdd
                .map(patient_divided_single_gene_list -> {
                    List<String> new_patient_divided_single_gene_list = new ArrayList<>();
                    List<String> bc_single_gene_in_gene_set_size_1_list_value = bc_single_gene_in_gene_set_size_1_list.value();
                    for(String single_gene_checking : patient_divided_single_gene_list){
                        if(bc_single_gene_in_gene_set_size_1_list_value.contains(single_gene_checking)){
                            new_patient_divided_single_gene_list.add(single_gene_checking);
                        }
                    }
                    return new_patient_divided_single_gene_list;
                })
                .cache();

        // Prepare for the iteration
        // Have the initial gene set which only contains item set with size k=1
        JavaPairRDD<List<String>,Integer> gene_set = gene_set_size_1_pair_rdd
                .mapToPair(tuple -> {
                    String single_gene = tuple._1;
                    Integer support = tuple._2;
                    List<String> gene_set_size_1_list = new ArrayList<>();
                    gene_set_size_1_list.add(single_gene);
                    return new Tuple2<>(gene_set_size_1_list, support);
                });

        // Start the iteration
        // With JavaPairRDD<String,Integer> gene_set
        boolean loop_continue_flag = true;
        int i =2;
        while((i<=k_user)&&loop_continue_flag){
            System.out.println("Checking for candidate itemset with size: " + i);
            int k_last = i - 1;

            JavaPairRDD<List<String>, Integer> gene_set_size_k_rdd;

            if(i==2){
                gene_set_size_k_rdd = gene_set_size_2_rdd
                        .mapToPair(list -> new Tuple2<>(list, 1));
            }else{
                JavaRDD<List<String>> gene_set_size_k_last_rdd = gene_set
                        .filter(tuple -> {
                            List<String> gene_set_list = tuple._1;
                            int gene_set_size = gene_set_list.size();
                            if(gene_set_size==k_last){
                                return true;
                            }else{
                                return false;
                            }
                        })
                        .map(tuple -> tuple._1);

                List<List<String>> gene_set_size_k_last_list = gene_set_size_k_last_rdd.collect();
                Broadcast<List<List<String>>> bc_gene_set_size_k_last_list =sc.broadcast(gene_set_size_k_last_list);
                gene_set_size_k_rdd = gene_set_size_k_last_rdd
                        .flatMapToPair(list -> {
                            List<Tuple2<List<String>, Integer>> part_gene_set_size_k_list = new ArrayList<>();
                            List<List<String>> bc_gene_set_size_k_last_list_value = bc_gene_set_size_k_last_list.value();
                            int start_index = bc_gene_set_size_k_last_list_value.indexOf(list) + 1;
                            while(start_index<bc_gene_set_size_k_last_list_value.size()){
                                List<String> gene_set_size_k_last = list;
                                List<String> inner_gene_set_size_k_last = bc_gene_set_size_k_last_list_value.get(start_index);
                                boolean flag = true;
                                int size_k_last = inner_gene_set_size_k_last.size();
                                for(int p =0;p<size_k_last-1;p++){
                                    String out = gene_set_size_k_last.get(p);
                                    String inner = inner_gene_set_size_k_last.get(p);
                                    if(!out.equals(inner)){flag = false;}
                                }
                                if(flag){
                                    gene_set_size_k_last.add(inner_gene_set_size_k_last.get(size_k_last-1));
                                    part_gene_set_size_k_list.add(new Tuple2<>(gene_set_size_k_last, 1));
                                }
                                start_index++;
                            }
                            return part_gene_set_size_k_list.iterator();
                        });
            }

            // Make this n partition
            // For 5*10, small data size, about 45s
            JavaRDD<List<String>> patient_divided_single_gene_list_int_pair_rdd_n_p = patient_divided_single_gene_list_int_pair_rdd.repartition(5);
//            JavaRDD<List<String>> patient_divided_single_gene_list_int_pair_rdd_n_p = patient_divided_single_gene_list_int_pair_rdd.coalesce(1);
            JavaPairRDD<List<String>, Integer> gene_set_size_k_rdd_n_p = gene_set_size_k_rdd.repartition(10);
//            JavaPairRDD<List<String>, Integer> gene_set_size_k_rdd_n_p = gene_set_size_k_rdd.coalesce(1);
            JavaPairRDD<List<String>,Integer> gene_set_size_k = patient_divided_single_gene_list_int_pair_rdd_n_p
                    .cartesian(gene_set_size_k_rdd_n_p)
                    .filter(tuple -> {
                        List<String> patient_whole_gene_list = tuple._1;
                        List<String> gene_set_size_k_list_in_car = tuple._2._1;
                        if(patient_whole_gene_list.containsAll(gene_set_size_k_list_in_car)){
                            return true;
                        }else{
                            return false;
                        }
                    })
                    .mapToPair(tuple -> {
                        Tuple2<List<String>, Integer> temp = tuple._2;
                        return temp;
                    })
                    .reduceByKey((n1,n2) -> n1+n2)
                    .filter(tuple -> {
                        Integer gene_support_num = tuple._2;
                        if(gene_support_num<support_num){
                            return false;
                        }else{
                            return true;
                        }
                    });

            int gene_set_size_k_size = gene_set_size_k.collect().size();
            if(gene_set_size_k_size==0){
                loop_continue_flag = false;
                System.out.println("For itemset size " + i + ", none of the candidate has passed the support check, the loop will stop.");
            }
            gene_set = gene_set.union(gene_set_size_k);
            i++;
        }

        // Change gene_set to the output format
        // Input List<String> Integer
        JavaRDD<String> output = gene_set
                .coalesce(1)
                .sortByKey(new ListComparator())
                .mapToPair(tuple -> {
                    List<String> gene_set_list = tuple._1;
                    Integer gene_set_num = tuple._2;
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
        sc.close();
    }
}

