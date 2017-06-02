package Lab1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.lang.Math.*;
import java.util.StringTokenizer;


public class MRreducer2 extends Reducer <Text, Text, Text, DoubleWritable> {

   	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		int count = 0; 
		double average_sum = 0.0;
		int failed_counts = 0;
		double sigma_failed_login_attempts = 0.0;
		ArrayList<Integer> failed_countsArray = new ArrayList<Integer>(); 	


		// TODO: parse out (key, values) (based on hint of cleverness mapper)

		// TODO: calculate mean_failed_login_attempts and write to context

		// TODO: calculate sigma_failed_login_attempts and write to context

		// TODO: calculate num_sigmas_for:<user> and write to context
	

		String compositeString;
		String[] compositeStringArray;
		Text acct = null;

		ArrayList<String> accts = new ArrayList<String>();
	
		// this calculates the sum of failed logins

		for (Text value : values) {
			compositeString = value.toString();
			compositeStringArray = compositeString.split("_");
			acct = new Text(compositeStringArray[0]);
			failed_counts = Integer.parseInt(compositeStringArray[1]);
			failed_countsArray.add(failed_counts);
			accts.add(acct.toString());
			average_sum += failed_counts;
			count += 1;	
		}

		double difference = 0.0;
		double difference2 = 0.0;
		double difference_square =0.0;
		double difference_square_sum = 0.0;
		double sigmas =0.0;
		double mean_failed_login_attempts = 0.0;
	
		// calculate and print mean 	

		mean_failed_login_attempts = average_sum / count;
		context.write(new Text("mean_failed_login_attempts"), new DoubleWritable(mean_failed_login_attempts));
	
		// this calculates the standard deviation of each user
	
		for (Integer failed_count: failed_countsArray){
			difference = (failed_count - mean_failed_login_attempts);
			difference_square = Math.pow(difference, 2);	
			difference_square_sum += difference_square;
		}
	
		// calcuate and print sigma for each user
	
		sigma_failed_login_attempts = Math.sqrt(difference_square_sum / count);
		context.write(new Text("sigma_failed_login_attempts"), new DoubleWritable(sigma_failed_login_attempts));
	
		// this calculates and print the no.of sigmas for each user
	
		for (int i = 0; i < accts.size(); i++) {
			acct = new Text(accts.get(i));
			difference2 = (failed_countsArray.get(i) - mean_failed_login_attempts);
			sigmas = difference2 / sigma_failed_login_attempts;
			Text keyText = new Text("num_sigmas_for:" + acct);
			context.write(keyText, new DoubleWritable(sigmas));
		}

	}
}
