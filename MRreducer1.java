package Lab1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.Iterator;

public class MRreducer1  extends Reducer <Text,IntWritable,Text,IntWritable> {

	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		   throws IOException, InterruptedException {
		
	// TODO: calculate total failed logins per user and write to context
		
		int sum_failed_login_attempts = 0;
		for (IntWritable value : values) {
			sum_failed_login_attempts += value.get();
		}
		context.write(key, new IntWritable(sum_failed_login_attempts));
	}
}
