package Lab1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.StringTokenizer;


public class MRmapper1  extends Mapper <LongWritable,Text,Text,IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	// TODO: filter failed USER_LOGIN records, discard the rest

	// TODO: discard records with acct name that do NOT have ""

	// TODO: write (acctname, 1) to context

	
	String[] s = value.toString().split("\\s");
	if (s[0].equals("type=USER_LOGIN")){
			if(s[13].equals("res=failed\'")){
				String[] tokens = s[8].split("=");
				if (tokens[1].startsWith("\"") && tokens[1].endsWith("\"")){
					context.write(new Text(tokens[1]), one);
				}
			}
		}
	}
		
}
