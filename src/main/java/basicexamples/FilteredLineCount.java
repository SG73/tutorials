package basicexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilteredLineCount {

public static void main(String[] args) {
		
	
		if (args.length < 1) {
			System.err.println("Please provide the input file full path as argument");
			System.exit(0);
	    }

	    SparkConf conf = new SparkConf().setAppName("basicexamples.java.LineCount");//.setMaster("local");
	    JavaSparkContext context = new JavaSparkContext(conf);
	    
	    JavaRDD<String> lines = context.textFile(args[0]);
	    
	    JavaRDD<String> linesWithSparkV1 
	    = lines.filter (line -> { 
	    							if ("Spark".indexOf(line) == -1)  
	    								return true;
	    							else 
	    								return false;
	    						}
	    				);
	    
	   
	    JavaRDD<String> linesWithSparkV2 = lines.filter (line -> ("Spark".indexOf(line) == -1));  
	    								
	    System.out.println ("All lines count = " + lines.count());
	    System.out.println ("Lines with Spark count = " + linesWithSparkV1.count());
	    System.out.println ("Lines with Spark count = " + linesWithSparkV2.count());
	
	    context.close();
	    
	}
}
