package basicexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LineCount {

	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.err.println("Please provide the input file full path as argument");
			System.exit(0);
	    }

	    SparkConf conf = new SparkConf().setAppName("basicexamples.java.LineCount");//.setMaster("local");
	    JavaSparkContext context = new JavaSparkContext(conf);
	    
	    JavaRDD<String> lines = context.textFile(args[0]);
	    System.out.println (lines.count());
	
	    context.close();
	    
	}
	    
}
