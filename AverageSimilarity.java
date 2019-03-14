package com.briup.knn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageSimilarity extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AverageSimilarity(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "averageSimilarity");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(ASMapper.class);
		job.setMapOutputKeyClass(TagDegree.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(ASReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.addInputPath(job, new Path("/knn_data/result_sorted/part-r-00000"));
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		job.setGroupingComparatorClass(TagDegreeGroupComparator.class);
		
		job.waitForCompletion(true);
		return 0;
	}

	public static class ASMapper extends Mapper<LongWritable, Text, TagDegree, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TagDegree, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] infos = line.split("\t");
			String tag = infos[0].split("_")[0];
			String degree = infos[1];
			
			TagDegree tg = new TagDegree(tag, Double.parseDouble(degree));
			
			context.write(tg, new Text("1"));
		}
	}
	
	public static class ASReducer extends Reducer<TagDegree, Text, Text, Text>{
		@Override
		protected void reduce(TagDegree key, Iterable<Text> values, Reducer<TagDegree, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			
			HashMap<String, Double> mapAve=new HashMap<>();
			HashMap<String, Integer> mapSum=new HashMap<>();
			
			for(int i=0;i<20;i++) {
				Text next = values.iterator().next();
				String value = key.toString();
				String tag=value.split("\t")[0];
				Double degree=Double.parseDouble(value.split("\t")[1]);
				
				if (mapSum.containsKey(tag)) {
					mapSum.put(tag, mapSum.get(tag)+1);
					mapAve.put(tag, mapAve.get(tag)+degree);
				}
				else {
					mapSum.put(tag,1);
					mapAve.put(tag,degree);
				}
				
			}
			
			for (Entry<String, Integer> entry : mapSum.entrySet()) {
				 
			    double ave=mapAve.get(entry.getKey())/entry.getValue();
			    String value=Double.toString(ave)+"\t"+Integer.toString(entry.getValue());
			    context.write(new Text(entry.getKey()), new Text(value));
			}
		}
	}
	
}
