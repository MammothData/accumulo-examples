package com.mammothdatallc.accumulo.iterators;

import com.mammothdatallc.accumulo.domain.MyAggregationPojo;
import com.mammothdatallc.accumulo.domain.MyPojo;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Adapted from: https://github.com/apache/accumulo/blob/bc7b5ff3719ecda36c462dfbd75a9e0852d674fc/core/src/main/java/org/apache/accumulo/core/iterators/user/RowEncodingIterator.java#L140
 */
public class MyPojoCountHistogram extends WrappingIterator {
    Key top_key;
    Value top_value;

    Map<Integer, Integer> countHistogram = new HashMap<Integer, Integer>();

    @Override
    public boolean hasTop() {
        return top_key != null;
    }

    @Override
    public Key getTopKey() {
        return this.top_key;
    }

    @Override
    public Value getTopValue() {
        return this.top_value;
    }

    @Override
    public void next() throws IOException {
        top_key = null;
        top_value = null;
        prepKeys();
    }

    private void prepKeys() throws IOException {
        if (top_key != null)
            return;
        Text currentRow;

        do {
            if (this.getSource().hasTop() == false) return;

            currentRow = new Text(this.getSource().getTopKey().getRow());

            Value v = this.getSource().getTopValue();

            while (this.getSource().hasTop() && this.getSource().getTopKey().getRow().equals(currentRow)) {

                MyPojo pojo = (MyPojo) SerializationUtils.deserialize(v.get());

                Integer count = pojo.count;

                Integer countCount = countHistogram.get(count);

                if (countCount == null) {
                    countCount = new Integer(1);
                } else {
                    countCount = countCount + 1;
                }

                countHistogram.put(count, countCount);

                this.getSource().next();
            }
        } while (!filter(currentRow));

        top_key = new Key(currentRow);
        top_value = new Value(SerializationUtils.serialize(new MyAggregationPojo(countHistogram)));
    }

    protected boolean filter(Text currentRow) {
        return true;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        top_key = null;
        top_value = null;

        Key sk = range.getStartKey();

        if (sk != null && sk.getColumnFamilyData().length() == 0 && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
                && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
            // assuming that we are seeking using a key previously returned by this iterator
            // therefore go to the next row
            Key followingRowKey = sk.followingKey(PartialKey.ROW);
            if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0)
                return;

            range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive());
        }

        this.getSource().seek(range, columnFamilies, inclusive);
        prepKeys();
    }

    @Override
    public MyPojoCountHistogram deepCopy(IteratorEnvironment env) {
        MyPojoCountHistogram c = new MyPojoCountHistogram();
        c.setSource(getSource().deepCopy(env));

        return c;
    }
}
