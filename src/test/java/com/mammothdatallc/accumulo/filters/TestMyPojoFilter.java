package com.mammothdatallc.accumulo.filters;

import com.mammothdatallc.accumulo.domain.MyPojo;
import junit.framework.TestCase;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.SerializationUtils;

import java.io.IOException;


public class TestMyPojoFilter extends TestCase {

    public void testAccept() throws IOException {
        MyPojoFilter filter = new MyPojoFilter();
        MyPojo pojo = new MyPojo("foo", 1);
        byte[] bytes = SerializationUtils.serialize(pojo);
        boolean result = filter.accept(null, new Value(bytes));

        assertEquals(true, result);
    }


}
