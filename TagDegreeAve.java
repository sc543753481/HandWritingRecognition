package com.briup.knn;



import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 * 对于相似度排序时进行使用的
 * tag   degree   group三个属性
 * 前两个用来存放数据，group属性控制分组
 * 故group属性值，给出相同的值即可
 * */
public class TagDegreeAve {
	//标签值，前缀名
	private Text tag = new Text();
	//待识别图片与该标签的相似度
	private DoubleWritable degree = new DoubleWritable();
	//该属性不参与计算，仅用于分组
	public TagDegreeAve() {
	}
	public TagDegreeAve(String tag,double degree) {
		this.tag = new Text(tag);
		this.degree = new DoubleWritable(degree);
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
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.tag.toString()+
			"\t"+this.degree.get();
	}
	
}



