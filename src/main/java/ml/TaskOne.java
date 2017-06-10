package ml;

import java.util.Arrays;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class TaskOne {
    public static void main(String[] args) {
        String inputDataPath = args[0];
        String outputDataPath = args[1];
        SparkConf conf = new SparkConf();
        // we can set lots of information here
        conf.setAppName("LAB457_GP6_AS3_TaskOne_Cancer_Gene_Analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // create some RDDs
        JavaRDD<String> geoData = sc.textFile(inputDataPath + "GEO.txt"),
                patientMetaData = sc.textFile(inputDataPath + "PatientMetaData.txt");
        final String geoHeader = geoData.first();
        final String patientHeader = patientMetaData.first();
        // do a filter
        JavaRDD<String> filteredGeoData =
            geoData.filter(s-> !s.equalsIgnoreCase(geoHeader));

        JavaRDD<String> finalGeoData =
                filteredGeoData.filter(s->{
                    String[] geoArray = s.split(",");
                    Float expressionValue = Float.parseFloat(geoArray[2]);
                    return (expressionValue>1250000);
                });

        // extract patient's gene data from raw data
        JavaPairRDD<String, Float> geoExtraction =
            finalGeoData.mapToPair(s->{
                String[] geoArray = s.split(",");
                return new Tuple2<>(geoArray[0],Float.parseFloat(geoArray[2]));
            });

        // extract the patient data from raw data
        JavaPairRDD<String, String> patientExtraction =
            patientMetaData
                .filter(s->!s.equalsIgnoreCase(patientHeader))
                .mapToPair(s->{
                String[] patientArray = s.split(",");
                return new Tuple2<>(patientArray[0],patientArray[4]);
            });

        // join two RDD pairs to get the disease
        // joinResults format <patient_id, Tuple2<expression_value, disease>
        JavaPairRDD<String, Tuple2<Float, String>> joinResults =
            geoExtraction
                .join(patientExtraction);

        // get the value tuple2 and sort them by key
        JavaPairRDD<Float, String> disease = joinResults.values()
            .mapToPair(s->s);

        // split the disease data and get the cancer types
        JavaRDD<String> diseaseType = disease.values().map(s->s);
        JavaRDD<String> cancer =
            diseaseType
                .flatMap(s-> Arrays.asList(s.split(" ")).iterator());

        // filter out cancer types and count the number
        JavaPairRDD<String, Integer> finalResult =
            cancer
                .filter(s-> s.contains("cancer")|| s.equalsIgnoreCase("leukemia")||s.equalsIgnoreCase("lymphoma"))
                .mapToPair(s-> new Tuple2<>(s,Integer.valueOf(1)))
                .reduceByKey((s1,s2)->(s1+s2))
                .mapToPair(s-> new Tuple2<Integer, String>(s._2,s._1)) // swap the key-value pairs for sorting
                .sortByKey(false)
                .mapToPair(s-> new Tuple2<String, Integer>(s._2, s._1)); // swap back the key-value pairs

        finalResult.map(s->s.productIterator().toSeq().mkString("\t")).saveAsTextFile(outputDataPath + "task_one_result");
        sc.close();
    }
}