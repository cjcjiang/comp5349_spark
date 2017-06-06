package ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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
        final Integer k_default = 5;

        // TODO: user define support_value
        // TODO: user define max k size

        String inputDataPath = args[0], outputDataPath = args[1];
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

        // TODO: join zhi qian jian shao parrel fen bu
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
        Long support_num = new Double(cancer_patient_num * support_value_default).longValue();
        System.out.println("The support_num is: " + support_num);

        // Prepare for the iteration
        // Cache the JavaRDD contains all patient divided single gene list in memory
        JavaRDD<List<String>> patient_divided_single_gene_list_rdd = patient_single_gene_list_pair_rdd.values().cache();

        // Prepare for the iteration
        // Get the largest number for item set size k
        Integer k_max = patient_divided_single_gene_list_rdd
                .map(list -> {
                    Integer list_size = list.size();
                    return list_size;
                })
                .max(new KMaxComparator());
        System.out.println("The max k should be: " + k_max);

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
                .cache();
//        System.out.println("I have passed gene_set_size_1_pair_rdd");

        // Prepare for the iteration
        // Have the initial gene set which only contains item set with size k=1
        JavaPairRDD<String,Integer> gene_set = gene_set_size_1_pair_rdd;
//        System.out.println("I have passed gene_set");

        // Start the iteration
        // With JavaPairRDD<String,Integer> gene_set
        for(int i=2;i<=k_default;i++){
            System.out.println("I am in loop: " + i);
            int k_last = i - 1;

            // To have the k size item set
            // First, filter out only the k-1 size gene set and store it in a list
            List<String> gene_set_size_k_last_list = gene_set
                    .filter(tuple -> {
                        String[] gene_set_array = tuple._1.split(";");
                        int gene_set_array_size = gene_set_array.length;
                        if(gene_set_array_size==k_last){
                            return true;
                        }else{
                            return false;
                        }
                    })
                    .map(tuple -> tuple._1)
                    .collect();
//            System.out.println("I have passed gene_set_size_k_last_list");
            // Have the list store all of the k=1 single gene
            List<String> gene_set_size_1_list = gene_set_size_1_pair_rdd
                    .map(tuple -> tuple._1)
                    .collect();
//            System.out.println("I have passed gene_set_size_1_list");
            // Have a list to store all the new k size gene set
            List<String> gene_set_size_k_list = new ArrayList<>();
            for(String gene_set_size_k_last : gene_set_size_k_last_list){
//                System.out.println("I am in item set size k generate loop: " + i);
                String[] single_gene_array_in_gene_set_size_k_last = gene_set_size_k_last.split(";");
                List<String> single_gene_list_in_gene_set_size_k_last = Arrays.asList(single_gene_array_in_gene_set_size_k_last);
                for(String single_gene : gene_set_size_1_list){
                    if(single_gene_list_in_gene_set_size_k_last.stream().noneMatch(string -> string.equals(single_gene))){
                        String gene_set_size_k_string = gene_set_size_k_last + ";" + single_gene;
                        gene_set_size_k_list.add(gene_set_size_k_string);
                    }
                }
            }
//            System.out.println("I have passed gene_set_size_k_list");
            // Broadcast gene_set_size_k_string_rdd
//            JavaRDD<String> gene_set_size_k_string_rdd = sc.parallelize(gene_set_size_k_list).coalesce(1);
//            Broadcast<JavaRDD<String>> bc_gene_set_size_k_string_rdd = sc.broadcast(gene_set_size_k_string_rdd);
//            System.out.println("I have passed the broadcast");

            // Broadcast gene_set_size_k_list
            // TODO: low efficiency
            Broadcast<List<String>> bc_gene_set_size_k_list = sc.broadcast(gene_set_size_k_list);
//            System.out.println("I have passed the broadcast");

            // Count the support num and filter
//            JavaPairRDD<String,Integer> gene_set_size_k = patient_divided_single_gene_list_rdd
//                    .flatMapToPair(patient_divided_single_gene_list -> {
//                        System.out.println("I am in patient_divided_single_gene_list_rdd flatMapToPair");
//                        JavaRDD<String> bc_gene_set_size_k_string_rdd_value = bc_gene_set_size_k_string_rdd.value();
//                        System.out.println("I pass the broadcast value get");
//                        List<Tuple2<String, Integer>> part_gene_set_size_k_list = bc_gene_set_size_k_string_rdd_value
//                                .flatMapToPair(gene_set_size_k_string -> {
//                                    List<Tuple2<String, Integer>> inner_gene_set_size_k_list = new ArrayList<>();
//                                    String[] single_gene_array_in_gene_set_size_k = gene_set_size_k_string.split(";");
//                                    List<String> single_gene_list_in_gene_set_size_k = Arrays.asList(single_gene_array_in_gene_set_size_k);
//                                    // If this patient contains all the single genes in this k size gene set, Integer will be 1, else will be 0
//                                    if(patient_divided_single_gene_list.containsAll(single_gene_list_in_gene_set_size_k)){
//                                        inner_gene_set_size_k_list.add(new Tuple2<>(gene_set_size_k_string,1));
//                                    }
//                                    return inner_gene_set_size_k_list.iterator();
//                                })
//                                .collect();
//                        return  part_gene_set_size_k_list.iterator();
//                    })
//                    .reduceByKey((n1,n2) -> n1+n2)
//                    .filter(tuple -> {
//                        Integer gene_support_num = tuple._2;
//                        if(gene_support_num<support_num){
//                            return false;
//                        }else{
//                            return true;
//                        }
//                    });

            // Not serilizable
//            JavaPairRDD<String,Integer> gene_set_size_k = patient_divided_single_gene_list_rdd
//                    .flatMapToPair(new PairFlatMapFunction<List<String>, String, Integer>(){
//                        public Iterator<Tuple2<String, Integer>> call (List<String> patient_divided_single_gene_list) throws Exception{
//                            List<String> bc_gene_set_size_k_list_value = bc_gene_set_size_k_list.value();
//                            JavaRDD<String> bc_gene_set_size_k_string_rdd = sc.parallelize(bc_gene_set_size_k_list_value);
//                            List<Tuple2<String, Integer>> part_gene_set_size_k_list = bc_gene_set_size_k_string_rdd
//                                    .flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
//                                        public Iterator<Tuple2<String, Integer>> call (String gene_set_size_k_string) throws Exception{
//                                            List<Tuple2<String, Integer>> inner_gene_set_size_k_list = new ArrayList<>();
//                                            String[] single_gene_array_in_gene_set_size_k = gene_set_size_k_string.split(";");
//                                            List<String> single_gene_list_in_gene_set_size_k = Arrays.asList(single_gene_array_in_gene_set_size_k);
//                                            if(patient_divided_single_gene_list.containsAll(single_gene_list_in_gene_set_size_k)){
//                                                inner_gene_set_size_k_list.add(new Tuple2<>(gene_set_size_k_string,1));
//                                            }
//                                            return inner_gene_set_size_k_list.iterator();
//                                        }
//                                    }).collect();
//                            return part_gene_set_size_k_list.iterator();
//                        }
//                    })
//                    .reduceByKey((n1,n2) -> n1+n2)
//                    .filter(tuple -> {
//                        Integer gene_support_num = tuple._2;
//                        if(gene_support_num<support_num){
//                            return false;
//                        }else{
//                            return true;
//                        }
//                    });

            // Not serilizable
//            JavaPairRDD<String,Integer> gene_set_size_k = patient_divided_single_gene_list_rdd
//                    .flatMapToPair(patient_divided_single_gene_list -> {
//                        List<String> bc_gene_set_size_k_list_value = bc_gene_set_size_k_list.value();
//                        JavaRDD<String> bc_gene_set_size_k_string_rdd_value = sc.parallelize(bc_gene_set_size_k_list_value);
//                        List<Tuple2<String, Integer>> part_gene_set_size_k_list = bc_gene_set_size_k_string_rdd_value
//                                .flatMapToPair(gene_set_size_k_string -> {
//                                    List<Tuple2<String, Integer>> inner_gene_set_size_k_list = new ArrayList<>();
//                                    String[] single_gene_array_in_gene_set_size_k = gene_set_size_k_string.split(";");
//                                    List<String> single_gene_list_in_gene_set_size_k = Arrays.asList(single_gene_array_in_gene_set_size_k);
//                                    // If this patient contains all the single genes in this k size gene set, Integer will be 1, else will be 0
//                                    if(patient_divided_single_gene_list.containsAll(single_gene_list_in_gene_set_size_k)){
//                                        inner_gene_set_size_k_list.add(new Tuple2<>(gene_set_size_k_string,1));
//                                    }
//                                    return inner_gene_set_size_k_list.iterator();
//                                })
//                                .collect();
//                        return  part_gene_set_size_k_list.iterator();
//                    })
//                    .reduceByKey((n1,n2) -> n1+n2)
//                    .filter(tuple -> {
//                        Integer gene_support_num = tuple._2;
//                        if(gene_support_num<support_num){
//                            return false;
//                        }else{
//                            return true;
//                        }
//                    });

            // Iterate the broadcast list, low efficiency
            JavaPairRDD<String,Integer> gene_set_size_k = patient_divided_single_gene_list_rdd
                    .flatMapToPair(patient_divided_single_gene_list -> {
                        List<Tuple2<String, Integer>> part_gene_set_size_k_list = new ArrayList<>();
                        List<String> bc_gene_set_size_k_list_value = bc_gene_set_size_k_list.value();
                        for(String gene_set_size_k_string : bc_gene_set_size_k_list_value){
                            String[] single_gene_array_in_gene_set_size_k = gene_set_size_k_string.split(";");
                            List<String> single_gene_list_in_gene_set_size_k = Arrays.asList(single_gene_array_in_gene_set_size_k);
                            // If this patient contains all the single genes in this k size gene set, Integer will be 1, else will be 0
                            if(patient_divided_single_gene_list.containsAll(single_gene_list_in_gene_set_size_k)){
                                Tuple2<String, Integer> temp = new Tuple2<>(gene_set_size_k_string,1);
                                part_gene_set_size_k_list.add(temp);
                            }
                        }
                        return  part_gene_set_size_k_list.iterator();
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

            // Try to use mapPartition
//            JavaPairRDD<String,Integer> gene_set_size_k = patient_divided_single_gene_list_rdd
//                    .mapPartitionsToPair(patient_divided_single_gene_iterator -> {
//                        List<Tuple2<String, Integer>> part_gene_set_size_k_list = new ArrayList<>();
//                        List<String> bc_gene_set_size_k_list_value = bc_gene_set_size_k_list.value();
//                        while(patient_divided_single_gene_iterator.hasNext()){
//                            List<String> patient_divided_single_gene_list = patient_divided_single_gene_iterator.next();
//                            for(String gene_set_size_k_string : bc_gene_set_size_k_list_value){
//                                String[] single_gene_array_in_gene_set_size_k = gene_set_size_k_string.split(";");
//                                List<String> single_gene_list_in_gene_set_size_k = Arrays.asList(single_gene_array_in_gene_set_size_k);
//                                // If this patient contains all the single genes in this k size gene set, Integer will be 1, else will be 0
//                                if(patient_divided_single_gene_list.containsAll(single_gene_list_in_gene_set_size_k)){
//                                    Tuple2<String, Integer> temp = new Tuple2<>(gene_set_size_k_string,1);
//                                    part_gene_set_size_k_list.add(temp);
//                                }
//                            }
//                        }
//                        return  part_gene_set_size_k_list.iterator();
//                    },true)
//                    .reduceByKey((n1,n2) -> n1+n2)
//                    .filter(tuple -> {
//                        Integer gene_support_num = tuple._2;
//                        if(gene_support_num<support_num){
//                            return false;
//                        }else{
//                            return true;
//                        }
//                    });

            // TODO: Reverse the join order (for k=2, the size of the possible gene set can be very large)
            // Even for small, k_max can be 384

//            System.out.println("I have passed the gene_set_size_k");

            // Have a list to store gene set size k
            List<Tuple2<String,Integer>> gene_set_this_loop_list = gene_set_size_k.collect();
//            System.out.println("I have passed gene_set_this_loop_list");

            // Have the list of gene_set without gene set size k
            List<Tuple2<String,Integer>> gene_set_previous_loop_list = gene_set.collect();
//            System.out.println("I have passed gene_set_previous_loop_list");

            // Merge gene_set_full_list and gene_set_size_k_string_int_tuple_list
            List<Tuple2<String, Integer>> loop_final_list = new ArrayList<>();
            loop_final_list.addAll(gene_set_previous_loop_list);
            loop_final_list.addAll(gene_set_this_loop_list);
//            System.out.println("I have passed loop_final_list");

            // Convert gene_set_full_list to JavaPairRDD and cache this in memory
            gene_set = sc
                    .parallelize(loop_final_list)
                    .mapToPair(tuple -> tuple)
                    .cache();

//            System.out.println("I have passed gene_set cache");
        }

        // Change gene_set to the output format
        // Input JavaPairRDD<String, Integer>
        // Output JavaPairRDD<Integer, String>
        JavaPairRDD<Integer, String> output = gene_set
                .mapToPair(tuple -> {
                    String gene_set_string = tuple._1;
                    Integer gene_set_num = tuple._2;
                    Tuple2<Integer, String> temp = new Tuple2<>(gene_set_num, gene_set_string);
                    return temp;
                })
                .reduceByKey((s1,s2)->s1+"\t"+s2);

        // For JavaRDDLike, functions like map, mapToPair, flatMap, flatMapToPair
        // Input, everything in <>, can be object for JavaRDD, or Tuple2 for JavaPairRDD
        // map Output: one object; mapToPair Output: Tuple2<key, value>
        // flatMap Output: iterator<Object>; flatMapToPair Output: iterator<Tuple2<key,value>>

        output.map(s->s.productIterator().toSeq().mkString("\t")).saveAsTextFile(outputDataPath + "task_two_result");
//        patient_single_gene_list_pair_rdd.saveAsTextFile(outputDataPath + "patient_single_gene_list_pair_rdd");
        sc.close();

    }
}

