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

public class TaskTwoRE {
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

        // Count all the cancer patients in geo.txt
        Long checking_cancer_patient_num = pid_gid
                .reduceByKey((s1, s2) -> s1)
                .keys()
                .count();
        final Long support_num = new Double(checking_cancer_patient_num * support_value).longValue();
        System.out.println("The support_num is: " + support_num);

        // Get k=1 filtered item-set
        // Key: gene set, at this time, the list only contains one gene
        // Value: pid list, the list contains all the patients who have this gene set
        JavaPairRDD<List<String>, ArrayList<String>> gene_set_size_one_pid = pid_gid
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
                )
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
}