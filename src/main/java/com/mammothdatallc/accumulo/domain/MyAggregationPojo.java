package com.mammothdatallc.accumulo.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MyAggregationPojo implements Serializable {

    private Map<Integer, Integer> countHistogram = new HashMap<Integer, Integer>();

    public MyAggregationPojo(Map<Integer, Integer> countHistogram) {
        this.countHistogram = countHistogram;
    }

    public Map<Integer, Integer> getCountHistogram() {
        return countHistogram;
    }

    @Override
    public String toString() {
        return "MyAggregationPojo{" +
                "countHistogram=" + countHistogram +
                '}';
    }
}
