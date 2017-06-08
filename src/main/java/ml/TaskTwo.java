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
        final Double support_value;
        final Integer k_default = 5;
        final Integer k_user;
        final String inputDataPath;
        final String outputDataPath;

        // TODO: user define max k size

        // TODO: error handling here
        if(args.length==4){
            inputDataPath = args[0];
            outputDataPath = args[1];
            support_value = Double.parseDouble(args[2]);
            k_user = Integer.parseInt(args[3]);
            System.out.println("The minimum support is set to: " + support_value + "; the maximum itemset size is set to: " + k_user);
        }else{
            inputDataPath = args[0];
            outputDataPath = args[1];
            support_value = support_value_default;
            k_user = k_default;
            System.out.println("Wrong command, all things are set to default.");
            System.out.println("The minimum support is set to: " + support_value + "; the maximum itemset size is set to: " + k_user);
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

        System.out.println("The number of all of the patients is: " + patient_id_data_raw.count());

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

        System.out.println("The number of all of cancer patients is: " + cancer_patient_id.count());

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
        final Long support_num = new Double(cancer_patient_num * support_value).longValue();
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
        Long distinct_gene_num = gene_set_size_1_pair_rdd.count();

        // Have the eff test
        JavaPairRDD<String,Integer> gene_set_size_1_no_filter = patient_divided_single_gene_list_rdd
                .flatMapToPair(list -> {
                    List<Tuple2<String, Integer>> gene_set_size_1_list_temp = new ArrayList<>();
                    for(String s : list){
                        Integer count_num = 1;
                        Tuple2<String, Integer> temp = new Tuple2<>(s, count_num);
                        gene_set_size_1_list_temp.add(temp);
                    }
                    return gene_set_size_1_list_temp.iterator();
                })
                .reduceByKey((n1, n2) -> n1 + n2);
        Long distinct_gene_num_no_filter = gene_set_size_1_no_filter.count();
        System.out.println("Before filter gene type num is: " + distinct_gene_num_no_filter + "; after filter it is: " + distinct_gene_num);

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
                });
        // TODO: if i need bc here
        List<List<String>> gene_set_size_2_list = gene_set_size_2_rdd.collect();

        // Prepare for the iteration
        // Have the initial gene set which only contains item set with size k=1
        JavaPairRDD<List<String>,Integer> gene_set = gene_set_size_1_pair_rdd
                .mapToPair(tuple -> {
                    String single_gene = tuple._1;
                    Integer support = tuple._2;
                    List<String> gene_set_size_1_list = new ArrayList<>();
                    gene_set_size_1_list.add(single_gene);
                    return new Tuple2<>(gene_set_size_1_list, support);
                })
                .cache();
//        System.out.println("I have passed gene_set");

        // Broadcast patient_divided_single_gene_list_rdd
//        List<List<String>> patient_divided_single_gene_list = patient_divided_single_gene_list_rdd.collect();
//        Broadcast<List<List<String>>> bc_list_patient_divided_single_gene_list = sc.broadcast(patient_divided_single_gene_list);

        // Start the iteration
        // With JavaPairRDD<String,Integer> gene_set
        for(int i=2;i<=k_user;i++){
            System.out.println("I am in loop: " + i);
            int k_last = i - 1;

            List<List<String>> gene_set_size_k_list;

            if(i==2){
                gene_set_size_k_list = gene_set_size_2_list;
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

                JavaRDD<List<String>> gene_set_size_k_rdd = gene_set_size_k_last_rdd
                        .flatMap(list -> {
                            List<List<String>> part_gene_set_size_k_list = new ArrayList<>();
                            int start_index = gene_set_size_k_last_list.indexOf(list) + 1;
                            while(start_index<gene_set_size_k_last_list.size()){
                                List<String> gene_set_size_k_last = list;
                                List<String> inner_gene_set_size_k_last = gene_set_size_k_last_list.get(start_index);
                                boolean flag = true;
                                int size_k_last = inner_gene_set_size_k_last.size();
                                for(int p =0;p<size_k_last-1;p++){
                                    String out = gene_set_size_k_last.get(p);
                                    String inner = inner_gene_set_size_k_last.get(p);
                                    if(!out.equals(inner)){flag = false;}
                                }
                                if(flag){
                                    gene_set_size_k_last.add(inner_gene_set_size_k_last.get(size_k_last-1));
                                    part_gene_set_size_k_list.add(gene_set_size_k_last);
                                }
                                start_index++;
                            }
                            return part_gene_set_size_k_list.iterator();
                        });

                gene_set_size_k_list = gene_set_size_k_rdd.collect();
            }

            System.out.println("gene_set_size_k_list size is: " + gene_set_size_k_list.size());

            // Car try
            JavaRDD<List<String>> gene_set_size_k_rdd = sc.parallelize(gene_set_size_k_list);
            JavaPairRDD<List<String>, List<String>> car_result = patient_divided_single_gene_list_rdd.cartesian(gene_set_size_k_rdd);
            JavaPairRDD<List<String>,Integer> gene_set_size_k = car_result
                    .mapToPair(tuple -> {
                        List<String> patient_whole_gene_list = tuple._1;
                        List<String> gene_set_size_k_list_in_car = tuple._2;
                        Tuple2<List<String>, Integer> temp;
                        if(patient_whole_gene_list.containsAll(gene_set_size_k_list_in_car)){
                            temp = new Tuple2<>(gene_set_size_k_list_in_car, 1);
                        }else{
                            temp = new Tuple2<>(gene_set_size_k_list_in_car, 0);
                        }
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

            // To have the k size item set
            // First, filter out only the k-1 size gene set and store it in a list

            // Cloud computing generate gene_set_size_k
            // Have and broadcast the list store all of the k last gene set

            // Broadcast gene_set_size_k_list
//            List<String> gene_set_size_k_list = gene_set_size_k_list_rdd.collect();
//            Broadcast<List<String>> bc_gene_set_size_k_list = sc.broadcast(gene_set_size_k_list);
//            System.out.println("The size of gene_set_size_k_list is: " + gene_set_size_k_list.size());

            // Iterate the broadcast list, low efficiency
//            JavaPairRDD<List<String>,Integer> gene_set_size_k = patient_divided_single_gene_list_rdd
//                    .flatMapToPair(patient_divided_single_gene_list -> {
//                        List<Tuple2<List<String>, Integer>> part_gene_set_size_k_list = new ArrayList<>();
//                        for(List<String> gene_set_in_gene_set_size_k_list : gene_set_size_k_list){
//                            // If this patient contains all the single genes in this k size gene set, Integer will be 1, else will be 0
//                            boolean flag = gene_set_in_gene_set_size_k_list.stream().allMatch(single_gene -> patient_divided_single_gene_list.contains(single_gene));
//                            if(flag){
//                                Tuple2<List<String>, Integer> temp = new Tuple2<>(gene_set_in_gene_set_size_k_list,1);
//                                part_gene_set_size_k_list.add(temp);
//                            }
//                        }
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

            // Reverse join
//            JavaRDD<List<String>> gene_set_size_k_rdd = sc.parallelize(gene_set_size_k_list);
//            JavaPairRDD<List<String>,Integer> gene_set_size_k = gene_set_size_k_rdd
//                    .flatMapToPair(list -> {
//                        List<Tuple2<List<String>, Integer>> part_gene_set_size_k = new ArrayList<>();
//                        List<List<String>> bc_list_patient_divided_single_gene_list_value = bc_list_patient_divided_single_gene_list.value();
//                        for(List<String> gene_set_in_patient_divided_single_gene_list : bc_list_patient_divided_single_gene_list_value){
//                            if(gene_set_in_patient_divided_single_gene_list.containsAll(list)){
//                                Tuple2<List<String>, Integer> temp = new Tuple2<>(list, 1);
//                                part_gene_set_size_k.add(temp);
//                            }
//                        }
//                        return part_gene_set_size_k.iterator();
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

            // Have a list to store gene set size k
            List<Tuple2<List<String>,Integer>> gene_set_this_loop_list = gene_set_size_k.collect();
            System.out.println("The size of gene_set_this_loop_list is: " + gene_set_this_loop_list.size());

            // Have the list of gene_set without gene set size k
            List<Tuple2<List<String>,Integer>> gene_set_previous_loop_list = gene_set.collect();
            System.out.println("The size of gene_set_previous_loop_list is: " + gene_set_previous_loop_list.size());

            // Merge gene_set_full_list and gene_set_size_k_string_int_tuple_list
            List<Tuple2<List<String>, Integer>> loop_final_list = new ArrayList<>();
            loop_final_list.addAll(gene_set_previous_loop_list);
            loop_final_list.addAll(gene_set_this_loop_list);
            System.out.println("The size of loop_final_list is: " + loop_final_list.size());

            // Convert gene_set_full_list to JavaPairRDD and cache this in memory
            gene_set = sc
                    .parallelize(loop_final_list)
                    .mapToPair(tuple -> tuple)
                    .cache();

//            gene_set = gene_set.union(gene_set_size_k).cache();

//            gene_set_size_k.saveAsTextFile(outputDataPath + "task_two_result_" + i);
        }

        // Change gene_set to the output format
        // Input JavaPairRDD<String, Integer>
        // Output JavaPairRDD<Integer, String>
        JavaPairRDD<Integer, String> output = gene_set
                .mapToPair(tuple -> {
                    List<List<String>> temp_list = new ArrayList<>();
                    List<String> gene_set_list = tuple._1;
                    temp_list.add(gene_set_list);
                    Integer gene_set_num = tuple._2;
                    Tuple2<Integer, List<List<String>>> temp = new Tuple2<>(gene_set_num, temp_list);
                    return temp;
                })
                .reduceByKey((l1,l2)->{
                    List<List<String>> temp = new ArrayList<>();
                    temp.addAll(l1);
                    temp.addAll(l2);
                    return temp;
                })
                .mapToPair(tuple->{
                    Integer supp = tuple._1;
                    List<List<String>> temp = tuple._2;
                    List<String> temp_1 = new ArrayList<>();
                    for(List<String> inner_temp_list : temp){
                        String out_string_temp = "";
                        for(String temp_string:inner_temp_list){
                            out_string_temp = temp_string + ";" + out_string_temp;
                        }
                        temp_1.add(out_string_temp);
                    }
                    String out_string = "";
                    for(String inner_temp_string: temp_1){
                        out_string = out_string + "\t" + inner_temp_string;
                    }
                    return new Tuple2<>(supp, out_string);
                });

        // For JavaRDDLike, functions like map, mapToPair, flatMap, flatMapToPair
        // Input, everything in <>, can be object for JavaRDD, or Tuple2 for JavaPairRDD
        // map Output: one object; mapToPair Output: Tuple2<key, value>
        // flatMap Output: iterator<Object>; flatMapToPair Output: iterator<Tuple2<key,value>>

//        output.map(s->s.productIterator().toSeq().mkString("\t")).saveAsTextFile(outputDataPath + "task_two_result");
        output.map(s->s.productIterator().toSeq().mkString("\t")).saveAsTextFile(outputDataPath + "task_two_result");
//        patient_single_gene_list_pair_rdd.saveAsTextFile(outputDataPath + "patient_single_gene_list_pair_rdd");
        sc.close();

    }
}

