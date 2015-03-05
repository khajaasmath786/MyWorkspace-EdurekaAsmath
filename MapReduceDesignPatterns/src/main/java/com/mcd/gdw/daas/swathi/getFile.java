package com.mcd.gdw.daas.swathi;
import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class getFile extends Configured implements Tool{

	public static class FFileMapper extends
	Mapper<LongWritable, Text, Text, Text> {
		
		String filename = ""; 
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String input_val=context.getConfiguration().get("input.storeid");
			boolean writetofile = false;
			String nam ="";
			if (value.toString().contains("storeId=\""+input_val+"\"")) 
					{
				Path filePath = ((FileSplit) context.getInputSplit()).getPath();
				nam = ((FileSplit) context.getInputSplit()).getPath().toString();
				if (!filename.contains(nam)){
				filename = filename +","+nam;
				writetofile=true;
					}	}
			if(writetofile)
			{
			context.write(new Text(nam+"::::::::::::"),value);
			}
		}
	
	}
	
	
	private void printUsage() {
		System.out.println("Usage : StldXmlParser_findfile <input_dir> <output>");
	}

	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			printUsage();
			return 2;
		}
		Configuration conf = this.getConf();
		Job job = new Job(conf,"Menu Item Parser");
		//Job job = new Job(conf,"TLDXmlParser");

		String[] remaining_args = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remaining_args.length >=2 ) {
			conf.set("input.storeid", remaining_args[2]);
		
		}
		
		conf.setClass("mapred.input.compression.codec", GzipCodec.class, CompressionCodec.class);
		
		
		job.setJobName("getFile");
		job.setJarByClass(getFile.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FFileMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outPath = new Path(args[1]);
		outPath.getFileSystem(conf).delete(outPath, true);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new getFile(), args);		

		System.exit(ret);
	}
	





}



 

 

