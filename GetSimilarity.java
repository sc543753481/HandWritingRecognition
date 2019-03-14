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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GetSimilarity extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetSimilarity(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO 自动生成的方法存根
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"getSimilarity");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(GSMapper.class);
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
	
	public static class GSMapper extends Mapper<Text, Text,Text,DoubleWritable>{
		static char[] unkown=new char[400];
		
		@Override
		protected void setup(Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			//把待识别向量读出来
			Configuration conf = context.getConfiguration();
			FileSystem fs=FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("/knn_data/unkown"));
			BufferedReader reader=new BufferedReader(new InputStreamReader(in));
			
			unkown = reader.readLine().toCharArray();
		}
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			//计算相似度
			double sum=0.0;
			char[] train_array = value.toString().split(",")[1].toCharArray();
			for(int i=0;i<400;i++) {
				int x=Integer.parseInt(Character.toString(unkown[i]));
				int t=Integer.parseInt(Character.toString(train_array[i]));
				
				sum+=(x-t)*(x-t);
			}
			double degree=1/(1+Math.sqrt(sum));
			
			context.write(key, new DoubleWritable(degree));
		}
	}
}
