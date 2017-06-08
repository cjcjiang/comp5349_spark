package ml;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TaskThree {
    public static void main(String[] args) {
        Float confidence = 0.6f; // default value of confidence
        String inputDataPath = args[0];
        String outputDataPath = args[1];
        String valueOfK = args[2];
        final int k = Integer.parseInt(valueOfK);
        // specify the confidence parameter
        confidence = Float.parseFloat(args[2]);
        SparkConf conf = new SparkConf();
        // we can set lots of information here
        conf.setAppName("Association Rule Generation");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // get the results from task two
        JavaRDD<String> freqItemSet = sc.textFile(inputDataPath);
        // format <itemSet, support>
        JavaPairRDD<Integer, String> suppItemSet =
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
                // this function returns the gene set which is of size 2 or larger
                .flatMapToPair(s->{
                    List<Tuple2<Integer, String>> size_filtered_geneSet = new ArrayList<>();
                    String[] array = s._2.split("/t");
                    for (String item:array){
                        if (item.contains(";")){
                            Tuple2<Integer, String> temp = new Tuple2<>(s._1, item);
                            size_filtered_geneSet.add(temp);
                        }
                    }
                return size_filtered_geneSet.iterator();
                });

        // return format <{1,2}|150, 10>
        JavaPairRDD<String, Integer> countItemSubSet =
            suppItemSet
                .flatMapToPair(s->{
                    List<Tuple2<String, Integer>> finalSubSetResult = new ArrayList<>();
                    String supp = s._1.toString();
                    CombinationGenerator g = new CombinationGenerator();
                    List<Object> temp = Arrays.asList(s._2.split(";"));
                    List<List<Object>> sub = g.getCombinations(temp,k);
                    for(List list: sub){
                        finalSubSetResult.add(new Tuple2<>(list.toString()+"|"+supp, 1));
                    }
                    return finalSubSetResult.iterator();
                })
                .reduceByKey((s1,s2)->s1+s2);

        System.out.println(countItemSubSet.top(10));

        // format <150, {1,2}|10>
        JavaPairRDD<Integer, String> formattedResult =
            countItemSubSet
                .mapToPair(s->{
                    String[] listAndSupp = s._1.split("|");
                    String subSet = listAndSupp[0];
                    Integer supp = Integer.parseInt(listAndSupp[1]);
                    String subSupp = s._2.toString();
                    return new Tuple2<>(supp,subSupp);
                });

        // joinResult format: (150, Tuple2<{1,2,3},{1,2}|10>)
        JavaPairRDD<Integer, Tuple2<String,String>> joinResult = suppItemSet.join(formattedResult);
        System.out.println(joinResult.top(10));

        JavaPairRDD<String, Float> finalResult =
            joinResult
                .mapToPair(s->{
                    String largeSet = s._2._1;
                    String[] temp = s._2._2.split("|");
                    String subSet = temp[0];
                    int subSupp = Integer.parseInt(temp[1]);
                    int supp = s._1;
                    float confidence_value = (float)supp/subSupp;
                    return new Tuple2<String, Float>(subSet+"/t"+largeSet+"-"+subSet, confidence_value);
                });

        finalResult.map(s->s.productIterator().toSeq().mkString("\t")).saveAsTextFile(outputDataPath);












    }
}