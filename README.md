blueprints-arangodb-graph
=========================

An implementation of the Blueprints API over ArangoDB

Installation & Testing
=======================

Please check the
[ArangoDB Installation Manual](http://www.arangodb.org/manuals/current/InstallManual.html)
for installation and compilation instructions.

As with other Blueprints implementations, the ArangoDB implementation is built with
	```mvn clean install```

Rexster Configuration
=====================

[Rexster](http://rexster.tinkerpop.com) is a graph server that can serve any Blueprints graph implementations. The ArangoDB implementation comes with a Rexster configuration class and can be deployed within Rexster.

To deploy:

* Build with `mvn clean install`
* Copy the `target/blueprints-arangodb-graph-x.y.z-jar-with-dependencies.jar` to the `REXSTER_HOME/ext` directory.
* Edit the `rexster.xml` file to include a configuration (see also [Rexster Configuration](https://github.com/tinkerpop/rexster/wiki/Rexster-Configuration) for ArangoDB as follows:

```text
<graph>
    <graph-name>arangodb-rexter-graph</graph-name>
    <graph-type>com.tinkerpop.blueprints.impls.arangodb.util.ArangoDBConfiguration</graph-type>
    <properties>
      <vertex-name>arangodb-rexter-graph-edges</vertex-name>
      <edge-name>arangodb-rexter-graph-edges</edge-name>
      <host>localhost</host>
      <port>8529</port>
    </properties>
  </graph>
```

* Start ArangoDB
* Start Rexster (see [Getting Started](https://github.com/tinkerpop/rexster/wiki/Getting-Started))

