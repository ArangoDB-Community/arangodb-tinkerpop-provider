![ArangoDB-Logo](https://docs.arangodb.com/assets/arangodb_logo_2016_inverted.png)

# arangodb-tinkerpop-provider

An implementation of the [Apache TinkerPop OLTP Provider](https://tinkerpop.apache.org/docs/3.3.3/dev/provider/#_provider_documentation) API for ArangoDB

## Compatibility

This Provider supports:
* Apache TinkerPop 3.3
* ArangoDB 3.3+ (via ArangoDB Java Driver 5.0.0).

## ArangoDB

Please check the 
[ArangoDB Installation Manual](https://docs.arangodb.com/latest/Manual/Deployment/) for guides on how to install ArangoDB.

## Maven

To add the provider to your project via maven you need to add the following dependency (shown is the latest version - you can replace the version with the one you need)

```XML
<dependencies>
  <dependency>
    <groupId>org.arangodb</groupId>
    <artifactId>arangodb-tinkerpop-provider</artifactId>
    <version>2.0.3</version>
  </dependency>
    ....
</dependencies>
```

The same coordinates can be used with Gradle and any other build system that uses maven repositories. 


## Using ArangoDBGraph via the TinkerPop API
This example is based on the TinkerPop documentation ([Creating a graph](http://tinkerpop.apache.org/docs/3.3.3/tutorials/getting-started/#_creating_a_graph)):

```java
ArangoDBConfigurationBuilder builder = new ArangoDBConfigurationBuilder();
builder.graph("modern")
    .withVertexCollection("software")
    .withVertexCollection("person")
    .withEdgeCollection("knows")
    .withEdgeCollection("created")
    .configureEdge("knows", "person", "person")
    .configureEdge("created", "person", "software");

// use the default database (and user:password) or configure a different database
// builder.arangoHosts("172.168.1.10:4456")
//     .arangoUser("stripe")
//     .arangoPassword("gizmo")

// create a ArangoDB graph
BaseConfiguration conf = builder.build();
Graph graph = GraphFactory.open(conf);
GraphTraversalSource gts = new GraphTraversalSource(graph);

// Clone to avoid setup time
GraphTraversalSource g = gts.clone();
// Add vertices
Vertex v1 = g.addV("person").property(T.id, "1").property("name", "marko")
    .property("age", 29).next();
g = gts.clone();
Vertex v2 = g.addV("software").property(T.id, "3").property("name", "lop")
    .property("lang", "java").next();

// Add edges
g = gts.clone();
Edge e1 = g.addE("created").from(v1).to(v2).property(T.id, "9")
    .property("weight", 0.4).next();

// Graph traversal 
// Find "marko" in the graph
g = gts.clone();
Vertex rv = g.V().has("name","marko").next();
assert v1 == rv;

// Walk along the "created" edges to "software" vertices
g = gts.clone();
Edge re = g.V().has("name","marko").outE("created").next();
assert re == e1;

g = gts.clone();
rv = g.V().has("name","marko").outE("created").inV().next();
// If the edge is irrelevant
// rv = g.V().has("name","marko").out("created").next();
assert rv == v2;


// Select the "name" property of the "software" vertices
g = gts.clone();
String name = (String) g.V().has("name","marko").out("created").values("name").next();
assert name.equals("lop");

// close the graph and the traversal source
gts.close();
graph.close();
```

## A note on element IDs

The provider implementation supports user supplied IDs, i.e. provide an id property for graph
elements, but currently we only support String ids, that is:

```
Vertex v1 = g.addV("person").property(T.id, "1");
```


will create a vertex with id "1". However, implementation wise, in ArangoDB we are only allowed to manipulate the documents `name`, not its `id`. For this reason, providing a TinkerPop vertex id (`T.id`) actually sets the vertex's ArangoDB `name`. As a result, retrieving the vertex by the given id will fail:

```
Vertex v2 = g.V("1");
assert v2 == null;
```

Since we know that documents IDs are created by concatenating (with a slash) the document's collection and its name, then we can find the vertex like so:

```
Vertex v2 = g.V("person/1");
assert v2 == v1;
```

## Contributing

We welcome bug reports (bugs, feature requests, etc.) as well as patches. When reporting a bug try to include as much information as possible (expected behaviour, seen behaviour, steps to reproduce, etc.). 


## More

Please visit our Wiki for additional information on how to use the latest version, build locally, run tests, etc.
