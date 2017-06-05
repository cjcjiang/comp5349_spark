package ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
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
        final Integer k_default = 5;

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
        // Make patients' id as the key, gene id and expression value as the value
        JavaPairRDD<String, Tuple2<Integer, Integer>> genes_strongly_expressed = gene_express_value_data_raw
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
                    Integer gene_id = Integer.parseInt(values[1]);
                    // Float expression_value = Float.parseFloat(values[2]);
                    Integer expression_value_flag = 1;
                    Tuple2<Integer, Integer> temp = new Tuple2<>(gene_id,expression_value_flag);
                    Tuple2<String, Tuple2<Integer, Integer>> result = new Tuple2<>(patient_id, temp);
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

        JavaPairRDD<String, Tuple2<String, Tuple2<Integer,Integer>>> cancer_patient_diseases_gene_value = cancer_patient_id.join(genes_strongly_expressed);

        // Prepare the PairRDD that will be counted to have the occurrence number of the patients
        JavaPairRDD<String,Integer> patient_count_prepare = cancer_patient_diseases_gene_value
                .mapToPair(tuple ->{
                    String patient_id = tuple._1;
                    Integer num = 1;
                    Tuple2<String, Integer> temp = new Tuple2<>(patient_id, num);
                    return temp;
                }).reduceByKey((n1,n2) -> n1+n2);

        // Count the amount of the cancer patient that occurs in geo.txt
        Long cancer_patient_num = patient_count_prepare.count();
        Long support_num = new Double(cancer_patient_num * support_value_default).longValue();
        System.out.println("The support_num is: " + support_num);

        // To have the k=1 item sets, also used as the PairRDD to store all the item sets
        // Input JavaPairRDD<String, Tuple2<String, Tuple2<Integer,Float>>>
        // mapToPair Output JavaPairRDD<String,Integer>, first String is gene id, second Integer is 1
        // reduceByKey count the occurrence time of each gene_id
        // Finally, filter out all gene_id that the occurrence time is less than the support_num
        JavaPairRDD<String,Integer> gene_set_size_1_pair_rdd = cancer_patient_diseases_gene_value
                .values()
                .mapToPair(tuple -> {
                    String gene_id = "" + tuple._2._1;
                    Integer gene_num = tuple._2._2;
                    Tuple2<String, Integer> temp = new Tuple2<>(gene_id, gene_num);
                    return temp;
                })
                .reduceByKey((n1, n2) -> (n1 + n2))
                .filter(tuple -> {
                    Integer gene_support_num = tuple._2;
                    if(gene_support_num<support_num){
                        return false;
                    }else{
                        return true;
                    }
                });

        // Prepare for the iteration
        // Convert the gene_set_size_1_pair_rdd to list
        List<String> gene_set_size_1_list = gene_set_size_1_pair_rdd.keys().collect();

        // Prepare for the iteration
        // Have the initial gene set which only contains item set with size k=1
        JavaPairRDD<String,Integer> gene_set = gene_set_size_1_pair_rdd;

        // Prepare for the iteration
        // Get the largest number for item set size k
        Integer k_max = cancer_patient_diseases_gene_value
                .mapToPair(tuple -> {
                    String patient_id = tuple._1;
                    Integer num = tuple._2._2._2;
                    Tuple2<String, Integer> temp = new Tuple2<>(patient_id, num);
                    return temp;
                })
                .reduceByKey((n1, n2) -> n1 + n2)
                .max(new KMaxComparator())
                ._2;
         System.out.println("The max k should be: " + k_max);

        // Prepare for the iteration
        // Get the whole patients' gene list
        // Input JavaPairRDD<String, Tuple2<String, Tuple2<Integer,Integer>>>
        // Output JavaRDD<String>, this string contains all of the genes from one patient
        JavaPairRDD<String, String> patient_gene_whole_string_pair_rdd = cancer_patient_diseases_gene_value
                .mapToPair(tuple -> {
                    String patient_id = tuple._1;
                    String gene_id = "" + tuple._2._2._1;
                    Tuple2<String, String> temp = new Tuple2<>(patient_id, gene_id);
                    return temp;
                })
                .reduceByKey((s1,s2) -> {
                    String gene_id_1 = s1;
                    String gene_id_2 = s2;
                    String patient_gene_whole_string = gene_id_1 + ";" + gene_id_2;
                    return patient_gene_whole_string;
                });
        JavaRDD<String> gene_whole_string_rdd = patient_gene_whole_string_pair_rdd.values();
        List<String> gene_whole_string_list = gene_whole_string_rdd.collect();

        // Start the iteration
        // gene_set, JavaPairRDD<String,Integer>
        for(int i=2;i<=k_default;i++){
            System.out.println("I am in first loop: " + i);
            int k_last = i -1;
            // Have the list of gene_set
            List<Tuple2<String,Integer>> gene_set_full_list = gene_set.collect();
            // To calculate the k size item set, firstly, filter out only the k-1 size item set
            JavaPairRDD<String,Integer> gene_set_size_k_last = gene_set
                    .filter(tuple -> {
                        String[] gene_set_array = tuple._1.split(";");
                        int gene_set_array_size = gene_set_array.length;
                        if(gene_set_array_size==k_last){
                            return true;
                        }else{
                            return false;
                        }
                    });
            List<Tuple2<String,Integer>> gene_set_list_size_k_last = gene_set_size_k_last.collect();
            // Have a list to store the k size item set
            List<Tuple2<String,Integer>> gene_set_size_k_string_int_tuple_list = new ArrayList<>();
            // Generate the new k size gene set list
            List<String> gene_set_size_k_list = new ArrayList<>();
            for(Tuple2<String,Integer> tuple : gene_set_list_size_k_last){
                System.out.println("I am in item set size k generate loop: " + i);
                String[] gene_set_size_k_last_array = tuple._1.split(";");
                List<String> gene_set_size_k_last_list = Arrays.asList(gene_set_size_k_last_array);
                for(String single_gene : gene_set_size_1_list){
                    if(gene_set_size_k_last_list.stream().noneMatch(s -> s.equals(single_gene))){
                        String gene_set_size_k_string = tuple._1 + ";" + single_gene;
                        gene_set_size_k_list.add(gene_set_size_k_string);
                    }
                }
            }
            // Loop through all of the cancer patients' gene large set
            for(String gene_whole_string : gene_whole_string_list){
                System.out.println("I am in  cancer patients' gene large set loop: " + i);
                // For each patient's large gene set, check if that contains the k size item set
                String[] single_gene_array = gene_whole_string.split(";");
                List<String> single_gene_list = Arrays.asList(single_gene_array);
                for(String gene_set_size_k : gene_set_size_k_list){
                    String[] gene_set_size_k_string_array = gene_set_size_k.split(";");
                    List<String> gene_set_size_k_string_list = Arrays.asList(gene_set_size_k_string_array);
                    // Gene set occurrence flag, true for occurrence
                    boolean flag = gene_set_size_k_string_list.stream().allMatch(s_set -> single_gene_list.stream().anyMatch(s_single -> s_single.equals(s_set)));
                    // System.out.println("This is the flag check: " + flag);
                    if(flag){
                        Tuple2<String,Integer> temp = new Tuple2<>(gene_set_size_k,1);
                        gene_set_size_k_string_int_tuple_list.add(temp);
                    }
                }
            }
            // Merge gene_set_full_list and gene_set_size_k_string_int_tuple_list
            List<Tuple2<String, Integer>> loop_final_list = new ArrayList<>();
            loop_final_list.addAll(gene_set_full_list);
            loop_final_list.addAll(gene_set_size_k_string_int_tuple_list);

            System.out.println("I have passed addALL");
            // Convert gene_set_full_list to JavaPairRDD
            JavaRDD<Tuple2<String,Integer>> gene_set_no_reduce = sc.parallelize(loop_final_list);
            System.out.println("I have passed sc.parallelize");
            // Get the final JavaPairRDD gene_set for this loop
            gene_set = gene_set_no_reduce
                    .mapToPair(tuple -> tuple)
                    .reduceByKey((int1, int2) -> int1 + int2)
                    .filter(tuple -> {
                        Integer gene_support_num = tuple._2;
                        if(gene_support_num<support_num){
                            return false;
                        }else{
                            return true;
                        }
                    });
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

        output.map(s->s.productIterator().toSeq().mkString("\t")).saveAsTextFile(outputDataPath + "patient_gene_value");
        sc.close();

    }
}

