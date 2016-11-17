package basicexamples;

import java.io.StringReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

public class InnerJoin {

	public static void main(String[] args) {
		
	    SparkConf conf = new SparkConf().setAppName("basicexamples.java.LineCount");//.setMaster("local");
	    JavaSparkContext context = new JavaSparkContext(conf);
	    
	    JavaRDD<String> set1 = context.textFile("file:///Users/sutty/eclipse_workspaces/sandbox/basicexamples/src/main/resources/ads.csv");
	    JavaPairRDD<String,String> set1KeyVals = set1.mapToPair(new ParseLineSet1());
	    
	    System.out.println(set1KeyVals.collect());
	    
	    context.close();
	    
	}
	
	@SuppressWarnings("serial")
	public static class ParseLineSet1 implements PairFunction<String, String, String> {

		public Tuple2<String, String> call(String line) throws Exception {

			if (line == null || line.trim().length() == 0) return null;
			
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] elements = reader.readNext();

			Tuple2<String, String> t = null;
			if (elements.length == 2) {
				t = new Tuple2<String, String>(elements[0],elements[1]);
			}
			reader.close();

			return t;
			
		}
	}
}
