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
        // format <support, itemSet>
        JavaPairRDD<Integer, String> suppItemSet =
            freqItemSet
                // first split the support number and the itemSet
                .mapToPair(s->{
                    String[] array = s.split("\t");
                    Integer support = Integer.parseInt(array[0]);
                    String itemSet = "";
                    for(int i=1;i< array.length;i++){
                        itemSet = itemSet + "\t" + array[i];
                    }
                    return new Tuple2<>(support, itemSet);
                });
                // split the itemSet
                // this function returns the gene set which is of size 2 or larger
//                .flatMapToPair(s->{
//                    List<Tuple2<Integer, String>> size_filtered_geneSet = new ArrayList<>();
//                    String[] array = s._2.split("/t");
//                    for (String item:array){
//                        String [] itemArray = item.split(";");
//                        if (itemArray.length > 1) {
//                            Tuple2<Integer, String> temp = new Tuple2<>(s._1, item);
//                            size_filtered_geneSet.add(temp);
//                        }
//                    }
//                return size_filtered_geneSet.iterator();
//                });

        // return format <{1,2}->{1}, 15>
        JavaPairRDD<String, Integer> finalItemSet =
            suppItemSet
                .flatMapToPair(s->{
                    List<Tuple2<String, Integer>> finalSubSetResult = new ArrayList<>();
                    Integer supp = s._1;
                    CombinationGenerator g = new CombinationGenerator();
                    String[] itemSet = s._2.split("\t");
                    for(String item: itemSet) {
                        if(item.split(";").length>1) {
                            List<String> temp = Arrays.asList(s._2.split(";"));
                            List<List<String>> sub = g.getCombinations(temp, k);
                            for(List list: sub){
                                finalSubSetResult.add(new Tuple2<>(temp.toString()+"->"+list.toString(), supp));
                            }
                        }
                        else{
                            finalSubSetResult.add(new Tuple2<>(item, supp));
                        }
                    }
                    return finalSubSetResult.iterator();
                });

        System.out.println(finalItemSet.top(10));

        // format <150, {1,2}|10>
        List<Tuple2<String, Integer>> formattedResult =
            finalItemSet
                .collect();

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
                    return new Tuple2<String, Float>(subSet+"/t"+largeSet+"-"+subSet, confidence_value).swap();
                })
                .sortByKey(false)
                .mapToPair(s->s.swap());

        finalResult.map(s->s.productIterator().toSeq().mkString("\t")).saveAsTextFile(outputDataPath);












    }
}