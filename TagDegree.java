package com.briup.knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 对于相似度排序时进行使用的
 * tag   degree   group三个属性
 * 前两个用来存放数据，group属性控制分组
 * 故group属性值，给出相同的值即可
 * */
public class TagDegree 
	implements WritableComparable<TagDegree>{
	//标签值，前缀名
	private Text tag = new Text();
	//待识别图片与该标签的相似度
	private DoubleWritable degree = new DoubleWritable();
	//该属性不参与计算，仅用于分组
	private Text group = new Text("1");
	public TagDegree() {
	}
	public TagDegree(String tag,double degree) {
		this.tag = new Text(tag);
		this.degree = new DoubleWritable(degree);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		degree.readFields(in);
		group.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tag.write(out);
		degree.write(out);
		group.write(out);
	}

	@Override
	public int compareTo(TagDegree o) {
		return o.degree.compareTo(this.degree);
	}
	public Text getTag() {
		return tag;
	}
	public void setTag(Text tag) {
		this.tag = new Text(tag.toString());
	}
	public DoubleWritable getDegree() {
		return degree;
	}
	public void setDegree(DoubleWritable degree) {
		this.degree =new DoubleWritable(degree.get());
	}
	public Text getGroup() {
		return group;
	}
	public void setGroup(Text group) {
		this.group =new Text(group.toString());
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.tag.toString()+"\t"+this.degree.get();
	}
	
}



