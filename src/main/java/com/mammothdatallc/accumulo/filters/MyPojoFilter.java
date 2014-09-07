package com.mammothdatallc.accumulo.filters;

import com.mammothdatallc.accumulo.domain.MyPojo;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.commons.lang.SerializationUtils;


/**
 * To test, log into shell:
 * accumulo shell -z dev localhost
 * <p/>
 * table <tablename>
 * <p/>
 * setiter -t <tablename> -scan -p 10 -n com.mammothdatallc.accumulo.filters.MyCustomFilter -class com.mammothdatallc.accumulo.filters.MyCustomFilter
 * deleteiter -n com.mammothdatallc.accumulo.filters.MyCustomFilter -scan
 */
public class MyPojoFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {

        MyPojo pojo = (MyPojo) SerializationUtils.deserialize(v.get());
        if ("foo".equals(pojo.name)) {
            return true;
        } else {
            return false;
        }
    }
}
