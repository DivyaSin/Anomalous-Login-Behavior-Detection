package Lab1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.StringTokenizer;

public class MRmapper2 extends Mapper <LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

	// TODO: write (key, value) pair to context (hint: need to be clever here)		
		
		String[] s = value.toString().split("\\s+");
		context.write(new Text("failedattempts"), new Text(s[0]+ "_" +s[1]));
	}
}
