![ArangoDB-Logo](https://www.arangodb.org/wp-content/uploads/2012/10/logo_arangodb_transp.png)

# blueprints-arangodb-graph

[![Build Status](https://secure.travis-ci.org/arangodb/blueprints-arangodb-graph.png)](http://travis-ci.org/arangodb/blueprints-arangodb-graph)

An implementation of the [Blueprints 2.6](https://github.com/tinkerpop/blueprints/wiki) API for ArangoDB

## Compatibility

This Blueprints driver is only supporting:
* Blueprints 2.6
* ArangoDB 2.5.4 - 2.8.*

It is not supporting ArangoDB 3.0 or newer.
The ArangoDB team is aware of this issue and is planning to upgrade this driver.
We suggest to use AQL directly on ArangoDB instead, which will give you a significant performance
boost as a bonus.

## Installation & Testing

Please check the
[ArangoDB Installation Manual](https://docs.arangodb.com/latest/Manual/Deployment/)
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
    <artifactId>blueprints-arangodb-graph</artifactId>
    <version>1.0.15</version>
  </dependency>
	....
</dependencies>
```

## More

See [WIKI](https://github.com/arangodb/blueprints-arangodb-graph/wiki) for more information.

