package com.example.pagerank.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageNodeWritable implements Writable {

	private int pageId;

	private float pageRank;

	private int outLinkCount;

	private int[] outLinkIds;

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

	public int[] getLinkIds() {
		return outLinkIds;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		pageId = input.readInt();
		pageRank = input.readFloat();
		outLinkCount = input.readInt();

		outLinkIds = new int[outLinkCount];

		int id;
		for (int i = 0; i < outLinkCount; i++) {
			id = input.readInt();
			outLinkIds[i] = id;
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(pageId);
		output.writeFloat(pageRank);
		output.write(outLinkCount);
		for (int id : outLinkIds) {
			output.writeInt(id);
		}
	}

	public static PageNodeWritable read(DataInput in) throws IOException {
		PageNodeWritable pgw = new PageNodeWritable();
		pgw.readFields(in);
		return pgw;
	}

}
