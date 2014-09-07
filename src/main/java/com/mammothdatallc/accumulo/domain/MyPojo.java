package com.mammothdatallc.accumulo.domain;

import java.io.Serializable;

public class MyPojo implements Serializable {

    public String name;
    public Integer count;

    public MyPojo(String name, Integer count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public String toString() {
        return "MyPojo{" +
                "name='" + name + '\'' +
                ", count='" + count + '\'' +
                '}';
    }
}
