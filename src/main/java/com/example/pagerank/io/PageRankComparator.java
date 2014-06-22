package com.example.pagerank.io;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankComparator implements RawComparator<PageRankWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(PageRankComparator.class);

	private final DataInputBuffer buffer = new DataInputBuffer();

	private final PageRankWritable page1 = new PageRankWritable();
	private final PageRankWritable page2 = new PageRankWritable();

	@Override
	public int compare(PageRankWritable o1, PageRankWritable o2) {
		return Float.compare(o1.getPageRank(), o2.getPageRank());
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			buffer.reset(b1, s1, l1);
			page1.readFields(buffer);

			buffer.reset(b2, s2, l2);
			page2.readFields(buffer);
		} catch (IOException e) {
			LOG.warn("Problem reading page rank writables", e);
		}

		return compare(page1, page2); // compare them
	}

}
