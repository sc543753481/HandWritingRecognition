package com.briup.knn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TagDegreeGroupComparator extends WritableComparator {
	public TagDegreeGroupComparator() {
		super(TagDegree.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TagDegree t1 = (TagDegree) a;
		TagDegree t2 = (TagDegree) b;
		return t1.getGroup().compareTo(t2.getGroup());
	}
}
