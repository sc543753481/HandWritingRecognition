package com.briup.knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CalculationSimilarity extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CalculationSimilarity(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO 自动生成的方法存根
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"calculationSimilarity");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(CSMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		//为job指定输入路径
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job,new Path("/knn_data/train_bin1"));
		//为job指定输出路径
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/result1"));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class CSMapper extends Mapper<LongWritable, Text,Text,DoubleWritable>{
		static char[] worthB=new char[400];
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("/knn_data/unkown"));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line = reader.readLine();
			worthB = line.toCharArray();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			
			String line=value.toString();
			String[] str = line.split(",");
			char[] worthA=str[1].toCharArray();
			int a=0;
			int b=0;
			double sum=0.0;
			double similar=0.0;
			for(int i=0;i<worthA.length;i++) {
				a= worthA[i]-'0';
				b= worthB[i]-'0';
				sum += Math.pow((a-b), 2);
			}
			similar=1/(1+Math.sqrt(sum));
			
			context.write(new Text(str[0]), new DoubleWritable(similar));
		}
	}
	
	
}
