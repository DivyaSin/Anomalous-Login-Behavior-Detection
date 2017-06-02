package Lab1; 

import org.apache.hadoop.conf.Configured;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MRdriver extends Configured implements Tool {
  	public int run(String[] args) throws Exception {

      // TODO: configure first MR job 

      // TODO: setup input and output paths for first MR job

      // TODO: run first MR job syncronously with verbose output set to true

      // TODO: configure the second MR job 

      // TODO: setup input and output paths for second MR job

      // TODO: run second MR job syncronously with verbose output set to true
      
      // TODO: detect anomaly based on sigma_threshold provided by user

      // TODO: for each user with score higher than threshold, print to screen:

      // detected anomaly for user: <username>  with score: <numSigmas>

		// configure job1 (MRmapper1 and MRreducer1))
		
    		Job job1 = new Job(getConf(), "MR1 receipts");

    		FileInputFormat.addInputPath(job1, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    		job1.setJarByClass(MRdriver.class);
    		job1.setMapperClass(MRmapper1.class);
    		job1.setReducerClass(MRreducer1.class);
    	
		job1.setInputFormatClass(TextInputFormat.class);
    		job1.setOutputKeyClass(Text.class);
    		job1.setOutputValueClass(IntWritable.class);
   		
		job1.waitForCompletion(true);
		
		// configure job2 (MRmapper2 and MRreducer2))		

    		Job job2 = new Job(getConf(), "MR2 receipts");

    		FileInputFormat.addInputPath(job2, new Path(args[1]));
    		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    		job2.setJarByClass(MRdriver.class);
    		job2.setMapperClass(MRmapper2.class);
    		job2.setReducerClass(MRreducer2.class);
    		
		job2.setOutputKeyClass(Text.class);
    		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapOutputKeyClass(Text.class);
 		job2.setMapOutputValueClass(Text.class);
		
		//job2.setInputFormatClass(TextInputFormat.class);
		//job2.setOutputFormatClass(TextOutputFormat.class);

		job2.waitForCompletion(true);

		
		// check the score higher than sigma_threshold and print
		

		try {

			Path pt=new Path(args[2]+"/part-r-00000");//Location of file in HDFS
        		FileSystem fs = FileSystem.get(new Configuration());
        		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        		String line;
			int sigma_int_threshold = Integer.parseInt(args[3]);
            		line=br.readLine();

            		while (line != null){
		
				if (line.startsWith("num")){
				String[] s = line.split("\\s+");
				double user_sigma = Double.parseDouble(s[1]);
				//System.out.println(user_sigma);

					if (user_sigma > sigma_int_threshold){
						//System.out.println(line);
						System.out.println("detected anomaly for user: " + s[0].replaceAll("num_sigmas_for:", "")  + " with score: " + s[1]);
					}
				}

                		line=br.readLine();
			}
		}	
		catch(Exception e){}
		return 0;
  	}

  	public static void main(String[] args) throws Exception { 
       		if(args.length != 4) {
        		System.err.println("usage: MRdriver <input-path> <output1-path> <output2-path> <sigma_int_threshold>");
         		System.exit(1);
       		}
       		//check sigma_int_threshold is an int
     		try {
        		Integer.parseInt(args[3]);

		}
      		catch (NumberFormatException e) {
        		System.err.println(e.getMessage());
			System.out.println("sigma_int_threshold should be integer");
        		System.exit(1);
     		}
      		Configuration conf = new Configuration();
      		System.exit(ToolRunner.run(conf, new MRdriver(), args));
  	 } 
}
