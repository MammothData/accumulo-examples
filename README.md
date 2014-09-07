Accumulo Filter, Combiner and Iterator Demo (For Complex Types)
=====================================================

This bare bones project demonstrates how to read/write complex types in Apache Accumulo and how to write basic
filters, combiners and iterators against these types.

### To Build

mvn clean package

Notice the pom has scope "provided" for several dependencies including accumulo, hadoop and zookeeper. These are
required for the accumulo client code to compile and run, but the Accumulo server processes will start having errors
if they are deployed in its classpath.

### To Deploy

To deploy the client, combiner and filter, take the built jar and place in:
ACCUMULO_HOME/lib

To run a client, deploy the same jar in
ACCUMULO_HOME/ext

and run:
ACCUMULO_HOME/bin/accumulo <client class name>

For example, I deploy as above and run:
ACCUMULO_HOME/bin/accumulo MyPojoClient

Which references MyPojoValueCombiner (which gets picked up from lib).

You can also deploy your com.mammothdatallc.accumulo.combiners from the shell:
ACCUMULO_HOME/bin/accumulo shell -u root -p dev

root@dev> table pojo
root@dev pojo> setiter -t pojo -p 10 -scan -class com.mammothdatallc.accumulo.combiners.MyPojoValueCombiner -n MyPojoValueCombiner
Combiners apply reduce functions to multiple versions of values with otherwise equal keys  
----------> set MyPojoValueCombiner parameter all... true  
----------> set MyPojoValueCombiner parameter columns... (leave blank)  
----------> set MyPojoValueCombiner parameter lossy... false  
