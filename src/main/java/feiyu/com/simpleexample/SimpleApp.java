package feiyu.com.simpleexample;

/**
 * This simple example of building a Spark standalone App in Java is from the website https://spark.apache.org/docs/0.9.0/quick-start.html  
 * Modified by feiyu
 */
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
  public static void main(String[] args) {
    String logFile = "src/main/resources/IsaacNewtonWikipedials.txt"; // Should be some file on your system
    JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
      "/Users/feiyu/workspace/spark-0.9.0-incubating-bin-hadoop2", new String[]{"target/SparkExamples-Java-1.0.jar"});
    /**
     * JavaSparkContext is used for initializing Spark 
     * Parameters of JavaSparkContext: 
     * 1) String master -> Option 1: use "local"
     *                  -> Options 2: Go to Spark Home and run $ ./sbin/start-master.sh, 
     *                     the master address will be shown at the 1st line of http://localhost:8080/, staring with spark:// and ending with post #
     *                     ex: spark://***.com:7077
     * 2) String appName
     * 3) String sparkHome
     * 4) String[] jars
     */
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      /**
       * From http://stackoverflow.com/questions/285793/what-is-a-serialversionuid-and-why-should-i-use-it
       * It is strongly recommended that all serializable classes explicitly declare serialVersionUID values, 
       * since the default serialVersionUID computation is highly sensitive to class details that may vary depending on 
       * compiler implementations, and can thus result in unexpected InvalidClassExceptions during deserialization.
       */
      private static final long serialVersionUID = 2931744970647541905L;

      public Boolean call(String s) { return s.contains("Newton"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      /**
       * 
       */
      private static final long serialVersionUID = -2869498203320427536L;

      public Boolean call(String s) { return s.contains("apple"); }
    }).count();

    System.out.println("Lines with \"Newton\": " + numAs + ", lines with \"apple\": " + numBs);
  }
}