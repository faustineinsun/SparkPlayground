package feiyu.com.javawordcount;
/**
 * This example is from this website(http://spark.apache.org/docs/0.9.0/java-programming-guide.html)
 * Modified by feiyu
 */

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class JavaWordCount {

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaWordCount <master> <file>");
      System.exit(1);
    }

    JavaSparkContext ctx = new JavaSparkContext(
      args[0], 
      "JavaWordCount",
      System.getenv("SPARK_HOME"), 
      JavaSparkContext.jarOfClass(JavaWordCount.class));
    
    JavaRDD<String> lines = ctx.textFile(args[1], 1);//("hdfs://...");

    JavaRDD<String> words = lines.flatMap(
      new FlatMapFunction<String, String>() {
        private static final long serialVersionUID = -5911888979431588122L;

        @Override
        public Iterable<String> call(String s) {
          return Arrays.asList(s.split(" "));
        }
      }
        );

    JavaPairRDD<String, Integer> ones = words.map(
      new PairFunction<String, String, Integer>() {
        private static final long serialVersionUID = 3891971657832045764L;

        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }
        );

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        private static final long serialVersionUID = 735441761774188946L;

        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      }
        );
    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1 + ": " + tuple._2);
    }
    
    System.exit(0);
  }
}
