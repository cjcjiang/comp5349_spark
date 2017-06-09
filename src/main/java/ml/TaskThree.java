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

        // return format <item, support> eg. <45;12, 17>
        JavaPairRDD<String, Integer> finalItemSet =
            suppItemSet
                .flatMapToPair(s->{
                    List<Tuple2<String, Integer>> finalSubSetResult = new ArrayList<>();
                    Integer supp = s._1;
                    String[] itemSet = s._2.split("\t");
                    for(String item: itemSet) {
                                finalSubSetResult.add(new Tuple2<>(item, supp));
                            }
                    return finalSubSetResult.iterator();
                }).cache();

        System.out.println(finalItemSet.top(10));

        // put the finalItemSet into a local List
        List<Tuple2<String, Integer>> formattedResult =
            finalItemSet
                .collect();


        // format <R \t S, support>
        JavaPairRDD<String, Integer> filteredList=
            finalItemSet
                .filter(s->s._1.contains(";"))
                .flatMapToPair(s->{
                    List<Tuple2<String, Integer>> result = new ArrayList<>();
                    List<List<String>> temp = new ArrayList<>();
                    List<String> itemSet = Arrays.asList(s._1.split(";"));
                    CombinationGenerator g = new CombinationGenerator();
                    temp = g.getCombinations(itemSet,k);
                    for(List<String> item: temp) {
                        if (item.size() == 1) {
                            String tempItem = item.get(0);
                            result.add(new Tuple2<>(tempItem + "\t"+ s._1, s._2));
                        }
                        else{
                            String tp = "";
                            for(String i:item){
                                if(tp.equals("")){
                                    tp = i;
                                }
                                else {
                                    tp = tp + ";" + i;
                                }
                                result.add(new Tuple2<>(tp + "\t" + s._1, s._2));
                            }
                        }
                    }
                    return result.iterator();
                });


        JavaPairRDD<String, Float> finalResult =
            filteredList
                .mapToPair(s->{
//                    String largeSet = s._2._1;
//                    String[] temp = s._2._2.split("|");
//                    String subSet = temp[0];
//                    int subSupp = Integer.parseInt(temp[1]);
//                    int supp = s._1;
//                    float confidence_value = (float)supp/subSupp;
//                    return new Tuple2<String, Float>(subSet+"/t"+largeSet+"-"+subSet, confidence_value).swap();

                    String[] sets = s._1.split("/t");
                    String largeSet = sets[1];
                    String subSet = sets[0];
                    Integer supp = s._2;
                    Integer subSupp;
                    Float confidence_value = 0f;
                    String largeSetMinusSubSet = largeSet.replace(subSet, "");
                    for(Tuple2<String, Integer> item: formattedResult){
                        if(subSet.equals(item._1)){
                            subSupp = item._2;
                            confidence_value = (float)supp/subSupp;
                        }
                    }
                    return new Tuple2<String, Float>(subSet+"\t"+largeSetMinusSubSet, confidence_value).swap();
                })
                .sortByKey(false)
                .mapToPair(s->s.swap());

        finalResult.map(s->s.productIterator().toSeq().mkString("\t")).saveAsTextFile(outputDataPath);












    }
}