![ArangoDB-Logo](https://docs.arangodb.com/assets/arangodb_logo_2016_inverted.png)

# arangodb-tinkerpop-provider

An implementation of the [Tinkerpop3 OLTP Provider](http://tinkerpop.apache.org/docs/3.3.3/dev/provider/#_provider_documentation) API for ArangoDB

## Compatibility

This Provider supports:
* Tinkerpop 3.3
* ArangoDB 3.3 (via ArangoDB Java Driver 4.6.0).*

## Installation & Testing

Please check the
[ArangoDB Installation Manual](https://docs.arangodb.com/latest/Manual/Deployment/)
for installation and compilation instructions.

Start ArangoDB on localhost port 8529.

As with other Blueprints implementations, the ArangoDB implementation is built with
	```mvn clean install -Dmaven.tests.skip=trye```
Note that we skip tests since not ALL tinkerpop tests pass (failing ones are known to fail - there
are some issues/descrepancies between the tests and what the ArangoDB provder can do).	

## Maven

To add the driver to your project with maven, add the following code to your pom.xml
(please use a driver with a version number compatible to your ArangoDB server's version):

```XML
<dependencies>
  <dependency>
    <groupId>org.arangodb</groupId>
    <artifactId>arangodb-tinkerpop-provider</artifactId>
    <version>2.0.0-SNAPSHOT</version>
  </dependency>
	....
</dependencies>
```

## More

You will find details on how to instantiate an ArangoDBGraph in the JavaDocs. An GramlinPlugin 
implementation is also provided so the provider can be used from the Gremlin console.

TBD - Add more detailed installation and use information.

