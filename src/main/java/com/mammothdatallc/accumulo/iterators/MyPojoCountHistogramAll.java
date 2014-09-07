package com.mammothdatallc.accumulo.iterators;

import com.mammothdatallc.accumulo.domain.MyAggregationPojo;
import com.mammothdatallc.accumulo.domain.MyPojo;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.lang.SerializationUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MyPojoCountHistogramAll extends WrappingIterator {
    Key top_key;
    Value top_value = new Value(SerializationUtils.serialize(0));

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
        while (this.getSource().hasTop()) {
            top_key = this.getSource().getTopKey();
            Value v = this.getSource().getTopValue();

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

        this.top_value = new Value(SerializationUtils.serialize(new MyAggregationPojo(countHistogram)));
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        next();
    }

    @Override
    public MyPojoCountHistogramAll deepCopy(IteratorEnvironment env) {
        MyPojoCountHistogramAll c = new MyPojoCountHistogramAll();
        c.setSource(getSource().deepCopy(env));
        return c;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        this.setSource(source);
    }
}