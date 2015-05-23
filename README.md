![ArangoDB-Logo](https://www.arangodb.org/wp-content/uploads/2012/10/logo_arangodb_transp.png)

# blueprints-arangodb-graph

[![Build Status](https://secure.travis-ci.org/arangodb/blueprints-arangodb-graph.png)](http://travis-ci.org/arangodb/blueprints-arangodb-graph)

An implementation of the [Blueprints 2.6](https://github.com/tinkerpop/blueprints/wiki) API for ArangoDB

## Installation & Testing

Please check the
[ArangoDB Installation Manual](http://www.arangodb.org/manuals/current/InstallManual.html)
for installation and compilation instructions.

Start ArangoDB on localhost port 8529.

As with other Blueprints implementations, the ArangoDB implementation is built with
	```mvn clean install```

## Maven

To add the driver to your project with maven, add the following code to your pom.xml
(please use a driver with a version number compatible to your ArangoDB server's version):

```XML
<dependencies>
  <dependency>
    <groupId>com.arangodb</groupId>
    <artifactId>arangodb-java-driver</artifactId>
    <version>1.0.10</version>
  </dependency>
	....
</dependencies>
```

## More

See [WIKI](https://github.com/arangodb/blueprints-arangodb-graph/wiki) for more information.

