package com.example.pagerank.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankWritable implements WritableComparable<PageRankWritable> {
	private static Logger LOG = LoggerFactory.getLogger(PageRankWritable.class);

	private boolean isNode;

	private int pageId;

	private float pageRank;

	private ArrayWritable data;

	public PageRankWritable() {
		data = new ArrayWritable(IntWritable.class);
		data.set(new IntWritable[0]);
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

	public boolean isNode() {
		return isNode;
	}

	public void setNode(boolean value) {
		this.isNode = value;
	}

	public Writable getWritable() {
		return (Writable) this;
	}

	public int[] convert() {
		Writable[] data = this.data.get();
		int[] output = new int[data.length];

		IntWritable iw;
		for (int i = 0; i < data.length; i++) {
			iw = (IntWritable) data[i];
			output[i] = iw.get();
		}

		return output;
	}

	public void set(IntWritable[] writables) {
		data.set(writables);
	}

	public Writable[] get() {
		return data.get();
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		isNode = input.readBoolean();
		if (isNode) {
			pageId = input.readInt();
			pageRank = input.readFloat();
		} else {
			// data = new ArrayWritable(IntWritable.class);
			data.readFields(input);
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeBoolean(isNode);
		if (isNode) {
			output.writeInt(pageId);
			output.writeFloat(pageRank);
		} else {
			data.write(output);
		}
	}

	@Override
	public boolean equals(Object obj) {
		boolean equals = false;
		if (obj instanceof PageRankWritable) {
			PageRankWritable pg = (PageRankWritable) obj;
			if (isNode) {
				equals = pg.getPageId() == this.getPageId();
			} else {
				Writable[] pgdata = pg.data.get();
				Writable[] thisdata = this.data.get();
				if (pgdata.length == thisdata.length) {
					equals = true;
					IntWritable pgitem;
					IntWritable thisitem;
					for (int i = 0; i < thisdata.length; i++) {
						pgitem = (IntWritable) pgdata[i];
						thisitem = (IntWritable) thisdata[i];
						if (pgitem.get() != thisitem.get()) {
							equals = false;
						}
					}
				}
			}
		}

		LOG.debug("equals " + equals);

		return equals;

	}

	@Override
	public int hashCode() {
		int hash;
		if (isNode) {
			hash = Integer.valueOf(pageId).hashCode();
		} else {
			hash = data.hashCode();
		}
		return hash;
	}

	@Override
	public String toString() {
		String s = "";
		if (isNode) {
			s = this.pageId + ";" + this.pageRank;
		} else {
			Writable[] values = data.get();
			for (int i = 0; i < values.length; i++) {
				s += values[i] + ",";
			}
			if (s.length() > 0) {
				s = s.substring(0, s.length() - 1);
			}
		}

		return s;
	}

	@Override
	public int compareTo(PageRankWritable pg) {
		return Integer.compare(pageId, pg.getPageId());
	}
}
