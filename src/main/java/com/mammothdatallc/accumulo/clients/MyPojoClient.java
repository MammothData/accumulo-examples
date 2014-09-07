package com.mammothdatallc.accumulo.clients;

import com.mammothdatallc.accumulo.domain.MyPojo;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

public class MyPojoClient {

    private static final String ACCUMULO_TABLE_NAME = "pojo";

    public static void main(String[] args) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException, IOException {

        if (args.length != 2) {
            System.out.println("Usage: MyPojoClient insertRecords(true/false) useScanner(true/false)");
            System.exit(0);
        }

        String instanceName = "dev";
        String zooServers = "127.0.0.1:2181";
        Instance inst = new ZooKeeperInstance(instanceName, zooServers);
        Connector conn = inst.getConnector("root", new PasswordToken("dev"));

        Text rowID = new Text("row1");


        if ("true".equals(args[0])) {

            if (!conn.tableOperations().exists(ACCUMULO_TABLE_NAME)) {
                conn.tableOperations().create(ACCUMULO_TABLE_NAME);
            }

            ColumnVisibility colVis = new ColumnVisibility("public");

            Mutation mutation = new Mutation(rowID);

            MyPojo a = new MyPojo("A", 7);
            Value aVal = new Value(SerializationUtils.serialize(a));
            mutation.put("A", "", colVis, aVal);

            MyPojo b = new MyPojo("B", 5);
            Value bVal = new Value(SerializationUtils.serialize(b));
            mutation.put("A", "", colVis, bVal);

            BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxMemory(10000000L);

            BatchWriter writer = conn.createBatchWriter(ACCUMULO_TABLE_NAME, config);

            writer.addMutation(mutation);

            writer.close();

        }

        //NOTE - This won't work unless you run "setauths -s public" on the table being written.
        Authorizations auths = new Authorizations("public");


        Scanner scan =
                conn.createScanner(ACCUMULO_TABLE_NAME, auths);

        scan.setRange(new Range(rowID));

        if ("true".equals(args[1])) {
            IteratorSetting cfg = new IteratorSetting(10, "MyPojoValueCombiner", "com.mammothdatallc.accumulo.combiners.MyPojoValueCombiner");
            cfg.addOption("all", "true");
            cfg.addOption("columns", "");
            cfg.addOption("lossy", "false");
            scan.addScanIterator(cfg);
        }


        for (Map.Entry<Key, Value> entry : scan) {
            Value val = entry.getValue();

            MyPojo da = (MyPojo) SerializationUtils.deserialize(val.get());
            System.out.println("Found: " + entry.getKey().toString() + " " + da);
        }
    }

}

