package basicexamples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	
		      
		      
	public static void main(String[] args) {
	    
		if (args.length < 2) {
	      System.err.println("Please provide the input/output file full path as argument");
	      System.exit(0);
	    }

	    SparkConf conf = new SparkConf().setAppName("basicexamples.java.WordCount");//.setMaster("local");
	    JavaSparkContext context = new JavaSparkContext(conf);
	    
	    JavaRDD<String> lines = context.textFile(args[0]);
	    
	    //JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
	    
	    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));
	    
	    JavaPairRDD<String, Integer> wordPairs = words.mapToPair( word -> new Tuple2<String, Integer>(word, 1));
	    
	    JavaPairRDD<String, Integer> wordCounts = wordPairs.reduceByKey( (v1, v2) -> (v1 + v2));
	    
	    //JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);

	    //counter.saveAsTextFile(args[1]);
	    
	    System.out.println (words.count());
	    System.out.println (wordCounts.collect());
	    
	    wordCounts.saveAsTextFile(args[1]);
	    
	    context.close();
	    
	}

}

