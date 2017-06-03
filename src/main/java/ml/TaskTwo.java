package ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

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
        //
        JavaPairRDD<String, Tuple2<Integer, Float>> genes_strongly_expressed = gene_express_value_data_raw
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
                    Float expression_value = Float.parseFloat(values[2]);
                    Tuple2<Integer, Float> temp = new Tuple2<>(gene_id,expression_value);
                    Tuple2<String, Tuple2<Integer, Float>> result = new Tuple2<>(patient_id, temp);
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

        JavaPairRDD<String, Tuple2<String, Tuple2<Integer,Float>>> cancer_patient_gene_value = cancer_patient_id.join(genes_strongly_expressed);

        JavaPairRDD<String, String> patient_gene_value = cancer_patient_gene_value.mapToPair(t -> {
            String patient_id = t._1;
            Tuple2<Integer,Float> gene_info = t._2._2;
            String out = "(" + gene_info._1 + ";" + gene_info._2 + ")";
            Tuple2<String, String> temp = new Tuple2<>(patient_id, out);
            return temp;
        }).reduceByKey((t1,t2) -> {
            String out = t1 + t2;
            return out;
        });

        patient_gene_value.saveAsTextFile(outputDataPath + "patient_gene_value");
        sc.close();

    }
}

