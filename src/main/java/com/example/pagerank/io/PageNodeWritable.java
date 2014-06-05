package com.example.pagerank.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageNodeWritable implements Writable {

	private int pageId;

	private float pageRank;

	public PageNodeWritable() {
	}

	public PageNodeWritable(int id, float rank) {
		pageId = id;
		pageRank = rank;
	}

	public void setPageId(int pageId) {
		this.pageId = pageId;
	}

	public int getPageId() {
		return pageId;
	}

	public void setPageRank(float rank) {
		pageRank = rank;
	}

	public float getPageRank() {
		return pageRank;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		pageId = input.readInt();
		pageRank = input.readFloat();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(pageId);
		output.writeFloat(pageRank);
	}

	public static PageNodeWritable read(DataInput in) throws IOException {
		PageNodeWritable pgw = new PageNodeWritable();
		pgw.readFields(in);
		return pgw;
	}

}
