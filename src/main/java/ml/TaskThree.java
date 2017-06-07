package ml;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class TaskThree {
    public static void main(String[] args) {
        Float confidence = 0.6f;
        String inputDataPath = args[0];
        String outputDataPath = args[1];
        // specify the confidence parameter
        confidence = Float.parseFloat(args[2]);
        SparkConf conf = new SparkConf();
        // we can set lots of information here
        conf.setAppName("Association Rule Generation");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // get the results from task two
        JavaRDD<String> freqItemSet = sc.textFile(inputDataPath);
        // format <support, itemSet>
        JavaPairRDD<Integer,String> suppItemSet =
            freqItemSet
                // first split the support number and the itemSet
                .mapToPair(s->{
                    String[] array = s.split("/t");
                    int support = Integer.parseInt(array[0]);
                    String itemSet = "";
                    for(int i=1;i< array.length;i++){
                        itemSet = itemSet + "/t" + array[i];
                    }
                    return new Tuple2<>(support, itemSet);
                })
                // split the itemSet
                .flatMapToPair(s->{

                })
    }
}