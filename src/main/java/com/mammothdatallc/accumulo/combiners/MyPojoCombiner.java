package com.mammothdatallc.accumulo.combiners;

import com.mammothdatallc.accumulo.domain.MyPojo;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


public class MyPojoCombiner extends TypedValueCombiner<MyPojo> {

    public static final MyPojoEncoder POJO_ENCODER = new MyPojoEncoder();


    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        setEncoder(POJO_ENCODER);
    }

    @Override
    public MyPojo typedReduce(Key key, Iterator<MyPojo> iter) {
        int sum = 0;

        while (iter.hasNext()) {
            MyPojo next = iter.next();
            sum += next.count;
        }
        return new MyPojo("", sum);
    }

    public static class MyPojoEncoder implements Encoder<MyPojo> {
        @Override
        public byte[] encode(MyPojo v) {
            return SerializationUtils.serialize(v);
        }

        @Override
        public MyPojo decode(byte[] b) {
            return (MyPojo) SerializationUtils.deserialize(b);
        }
    }
}
