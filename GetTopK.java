package com.briup.knn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class GetTopK extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetTopK(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "getTopK");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(GTKMapper.class);
		job.setMapOutputKeyClass(TagDegree.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(GTKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.addInputPath(job, new Path("/knn_data/result_sorted/part-r-00000"));
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		job.setGroupingComparatorClass(TagDegreeGroupComparator.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class GTKMapper extends Mapper<LongWritable, Text,TagDegree,Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TagDegree, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			String[] info = value.toString().split("\t");
			String tag=info[0].substring(0, 1);
			
			Double degree=Double.parseDouble(info[1]);
			
			TagDegree td=new TagDegree(tag,degree);
			context.write(td, new Text("1"));
		}
	}
	
	public static class GTKReducer extends Reducer<TagDegree, Text, Text, Text>{
		@Override
		protected void reduce(TagDegree key, Iterable<Text> values, Reducer<TagDegree, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			//1 取前20个数据
			//2 计算平均相似度
			int i=0;
			Map<String,AvgNum> map=new HashMap<>();
			
			Iterator<Text> ite = values.iterator();
			
			while(i<20) {
				Text next=ite.next();
				String tag=key.getTag().toString();
				double degree=key.getDegree().get();
				
				if (!map.containsKey(tag)) {
					AvgNum an=new AvgNum(1,degree);
					map.put(tag, an);
				}
				else {
					AvgNum old_an=map.get(tag);
					double old_avg= old_an.getAvg();
					int old_num = old_an.getNum();
					int new_num=old_num+1;
					double new_avg=(old_avg*old_num+degree)/new_num;
					
					AvgNum new_an=new AvgNum(new_num,new_avg);
					map.put(tag, new_an);
				}
				
				
				i++;
			}
			
			for(Map.Entry<String, AvgNum> en:map.entrySet()) {
				context.write(new Text(en.getKey()), new Text(en.getValue().getAvg()+"\t"+en.getValue().getNum()));
			}
		}
	}
}
