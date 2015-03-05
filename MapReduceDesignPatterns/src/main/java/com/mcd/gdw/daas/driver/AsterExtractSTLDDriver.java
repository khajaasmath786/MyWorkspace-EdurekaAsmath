
package com.mcd.gdw.daas.driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import  com.mcd.gdw.daas.mapreduce.*;

import com.mcd.gdw.daas.util.HDFSUtil;

public class AsterExtractSTLDDriver extends Configured implements Tool {

	

	public static void main(String[] args) throws Exception {Configuration conf1 = new Configuration();
	
		int retval = ToolRunner.run(conf1,new AsterExtractSTLDDriver(), args);

		System.out.println(" return value : " + retval);
		// InputAsterExtractSTLDMapper Output

	}
	
	@Override
	public int run(String[] argsAll) throws Exception {
		
		try{
			GenericOptionsParser gop = new GenericOptionsParser(argsAll);
			String[] args = gop.getRemainingArgs();
			
			Configuration conf = this.getConf();
			Job job = new Job(conf,"Aster Extract");
			FileSystem fileSystem = FileSystem.get(conf);
			
			job.setJarByClass(AsterExtractSTLDDriver.class);
			
			job.setMapperClass(AsterExtractSTLDMapper.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(0);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, new Path(args[1]), true);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
		
		
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		
		
		return 0;
	}


}
