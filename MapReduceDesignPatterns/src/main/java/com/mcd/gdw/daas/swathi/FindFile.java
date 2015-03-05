package com.mcd.gdw.daas.swathi;
import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class FindFile extends Configured implements Tool{

    public static class FindFileMapper extends
    Mapper<Text, Text, Text, Text> {
        //String k="";
        //String path1="";
        Map<String, String> map1 = null;
        
        
        public void setup(Context context) throws IOException,
        InterruptedException {
            Configuration conf = context.getConfiguration();
            map1 = new HashMap<String, String>();
            String k="";
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path("/user/mc41783/STLD/20150209"));
        System.out.println(status.length);
        for (int i=0;i<status.length;i++){
            String path1=status[i].getPath().toString();
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
        String line;
        line=br.readLine();
        while (line != null){
        //System.out.println(line);
          k=line.substring(39,44);
        System.out.println(k);
        map1.put(k, path1);
        line=br.readLine();
        context.write(new Text(k), new Text(path1));

        }
        
        }
        }
        
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            
            
            
        }
    
    }
    
    
    private void printUsage() {
        System.out.println("Usage : StldXmlParser <input_dir> <output>");
    }

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            printUsage();
            return 2;
        }
        Configuration conf = this.getConf();
		Job job = new Job(conf,"FindFile");
        //job.setJobName("FindFile");
        job.setJarByClass(FindFile.class);
        //job.setMapOutputValueClass(Text.class);
        //job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(FindFileMapper.class);
        //job.setReducerClass(TLDSaleMetricReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outPath = new Path(args[1]);
        outPath.getFileSystem(conf).delete(outPath, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
        
        
        
          
          //job.addCacheFile(new URI("/home/swathi/dfiles/Currency.psv"));
         // job.addCacheFile(new URI("GiftItem.psv"));
         // job.addCacheFile(new URI("Currency.psv"));
          
          
         

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new FindFile(), args);        

        System.exit(ret);
    }
    





}

