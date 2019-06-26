package com.arangodb.tinkerpop.gremlin.client.test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import com.arangodb.tinkerpop.gremlin.client.ArngDatabaseClient;

import java.util.*;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This tests require four users:
 * <ul>
 *  <li> root:	   To test DB creation, requires root access
 *  <li> gremlin:  To test graph/label creation+access in the db, requires "Administrate" permission to DB
 *  <li> reader:   To test graph/label access in the db, requires "Access" permission to DB
 *  <li> limited:  To test no access to the db,  requires "No access" permission to DB 
 * </ul> 
 * For all users (except root) the password is expected to be the same as the username. For the root,
 * the password must be set via environment variable ARANGODB_ROOT_PWD (so no system password is shared
 * via code).  
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 *
 */
@RunWith(Parameterized.class)
@Ignore
public class ClientTest {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
		        { true, "knows_", "social_", "routeplanner_" },
                { false, "", "", "" }
		});
	}
	
	@Rule
	public ExpectedException exception = ExpectedException.none();
	
	private PropertiesConfiguration configuration;
	private ArngDatabaseClient client;

	@Parameterized.Parameter
	public boolean shouldPrefixCollectionWithGraphName;

    @Parameterized.Parameter(1)
	public String knowsPrefix;

    @Parameterized.Parameter(2)
    public String socialPrefix;

    @Parameterized.Parameter(3)
    public String routeplannerPrefix;

	@Before
	public void setUp() throws Exception {

		configuration = new PropertiesConfiguration();
//		configuration.setProperty(ArangoDBGraph.PROPERTY_KEY_PREFIX + "." + ArangoDBGraph.PROPERTY_KEY_DB_NAME, "tinkerpop");
//		configuration.setProperty(ArangoDBGraph.PROPERTY_KEY_PREFIX + "." + ArangoDBGraph.PROPERTY_KEY_GRAPH_NAME, "standard");
//		configuration.setProperty(ArangoDBGraph.PROPERTY_KEY_PREFIX + "." + "arangodb.hosts", "127.0.0.1:8529");
//		configuration.setProperty(ArangoDBGraph.PROPERTY_KEY_PREFIX + "." + "arangodb.user", "gremlin");
//		configuration.setProperty(ArangoDBGraph.PROPERTY_KEY_PREFIX + "." + "arangodb.password", "gremlin");
//		configuration.setProperty(ArangoDBGraph.PROPERTY_KEY_PREFIX + "." + ArangoDBGraph.PROPERTY_KEY_SHOULD_PREFIX_COLLECTION_NAMES, shouldPrefixCollectionWithGraphName);
		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
		//ArangoDBGraph g = new ArangoDBGraph(configuration);
//		/System.out.println(g.features());
		// client = new ArngDatabaseClient(g, arangoProperties, "tinkerpop", 30000);
	}

	@Test
	public void dummy() {
		assert true;
	}
//
//	@After
//	public void tearDown() {
//		// Drop used graphs and collections, if present
//		DatabaseClient db = client.getDB();
//		// know_graph
//		deleteGraph(db, "knows", true);
//        // social graph
//        deleteGraph(db, "social", true);
//		// city graph
//        deleteGraph(db, "routeplanner", true);
//        client.shutdown();
//		client = null;
//	}
//
//    private boolean deleteGraph(
//        DatabaseClient db,
//        String name,
//        boolean dropCollections) {
//        if (db != null) {
//            GraphClient graph = db.graph(name);
//            if (graph.exists()) {
//                Collection<String> edgeDefinitions = dropCollections ? graph.getEdgeDefinitions() : Collections.emptyList();
//                Collection<String> vertexCollections = dropCollections ? graph.getVertexCollections(): Collections.emptyList();;
//                // Drop graph first because it will break if the graph collections do not exist
//                graph.drop();
//                for (String definitionName : edgeDefinitions) {
//                    String collectioName = definitionName;
//                    if (db.label(collectioName).exists()) {
//                        db.label(collectioName).drop();
//                    }
//                }
//                for (String vc : vertexCollections) {
//                    String collectioName = vc;
//                    if (db.label(collectioName).exists()) {
//                        db.label(collectioName).drop();
//                    }
//                }
//                return true;
//            } else {
//                try {
//                    graph.drop();
//                } catch (ArangoDBException e) {
//                    //throw ArangoDBExceptions.getArangoDBException(e);
//                }
//            }
//        }
//        return false;
//    }
//
//
//	// ********* The following methods test a local ArngDatabaseClient *********
//
//	@Test
//	public void test_RestrictedUserNewDatabase_should_throw_ArangoDBGraphException() throws Exception {
//		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
//		exception.expect(ArangoDBGraphException.class);
//		exception.expectMessage(startsWith("General ArangoDB error (unkown error code)"));
//		ArangoDBGraph g = new ArangoDBGraph(configuration);
//		new ArngDatabaseClient(g, arangoProperties, "demo", 30000, true);
//	}
//
//	@Test
//	public void test_AuthorizedUserNewDatabase_can_create_new_database() throws Exception {
//		org.junit.Assume.assumeTrue(System.getenv("ARANGODB_ROOT_PWD") != null);
//		configuration.setProperty("arangodb.user", "root");
//		String pwd = System.getenv("ARANGODB_ROOT_PWD");
//		configuration.setProperty("arangodb.password", pwd);
//		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
//		ArangoDBGraph g = new ArangoDBGraph(configuration);
//		ArngDatabaseClient localClient = new ArngDatabaseClient(g, arangoProperties, "demo", 30000, true);
//		assertThat(localClient.dbExists(), is(true));
//		localClient.deleteDb();
//		localClient.shutdown();
//	}
//
//	@Test
//	public void test_RestrictedUserExistingDb_should_throw_ArangoDBGraphException() throws Exception {
//		configuration.setProperty("arangodb.user", "limited");
//		configuration.setProperty("arangodb.password", "limited");
//		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
//		exception.expect(ArangoDBGraphException.class);
//		exception.expectMessage(startsWith("DB not found or user has no access:"));
//		new ArngDatabaseClient(, arangoProperties, "tinkerpop", 30000);
//	}
//
//	@Test
//	public void test_ReadAccessUserCreateGraph_should_throw_ArangoDBGraphException() throws Exception {
//		configuration.setProperty("arangodb.user", "reader");
//		configuration.setProperty("arangodb.password", "reader");
//		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
//		ArngDatabaseClient localClient = new ArngDatabaseClient(, arangoProperties, "tinkerpop", 30000);
//		assertThat(localClient.dbExists(), is(true));
//		List<String> verticesCollectionNames = new ArrayList<>();
//		List<String> edgesCollectionNames = new ArrayList<>();
//		verticesCollectionNames.add("person");
//		edgesCollectionNames.add("knows");
//
//		exception.expect(ArangoDBGraphException.class);
//		exception.expectMessage(startsWith("General ArangoDB error (unkown error code)"));
//		localClient.createGraph("knows", verticesCollectionNames, edgesCollectionNames, Collections.emptyList());
//		localClient.shutdown();
//	}
//
//	// ********* The following tests use the ArngDatabaseClient from @Setup *********
//
//	@Test
//	public void test_ServerVersion() throws Exception {
//		String version = client.getVersion();
//		assertThat(version, is(notNullValue()));
//	}
//
//	@Test
//	public void test_CreateSimpleGraph() throws Exception {
//
//		String graph_name = "knows";
//		List<String> verticesCollectionNames = new ArrayList<>();
//		List<String> edgesCollectionNames = new ArrayList<>();
//		verticesCollectionNames.add("person");
//		edgesCollectionNames.add("knows");
//
//		client.createGraph(graph_name, verticesCollectionNames, edgesCollectionNames, Collections.emptyList());
//		DatabaseClient db = client.getDB();
//		assertThat("Graph not found in db", db.graph(graph_name).exists(), is(true));
//		assertThat("Vertex label found in db", db.label(String.format("%sperson", knowsPrefix)).exists(), is(true));
//		assertThat("Edge label found in db", db.label(String.format("%sknows", knowsPrefix)).exists(), is(true));
//		GraphClient g = db.graph(graph_name);
//		Collection<EdgeDefinition> defs = g.getInfo().getEdgeDefinitions();
//		assertThat(defs, hasSize(2));       // +1 for ELEMENT_HAS_PROPERTIES
//		EdgeDefinition d = defs.iterator().next();
//		assertThat(d.getCollection(), is(String.format("%sknows", knowsPrefix)));
//		assertThat(d.getFrom(), contains(String.format("%sperson", knowsPrefix)));
//		assertThat(d.getTo(), contains(String.format("%sperson", knowsPrefix)));
//	}
//
//	@Test
//	public void test_CreateMultiVertexGraph() throws Exception {
//		String graph_name = "social";
//		List<String> verticesCollectionNames = new ArrayList<>();
//		List<String> edgesCollectionNames = new ArrayList<>();
//		List<String> relations = new ArrayList<>();
//		verticesCollectionNames.add("male");
//		verticesCollectionNames.add("female");
//		edgesCollectionNames.add("relation");
//		relations.add("relation:male,female->male,female");
//
//
//		client.createGraph(graph_name, verticesCollectionNames, edgesCollectionNames, relations);
//		DatabaseClient db = client.getDB();
//		assertThat("Created graph not found in db", db.graph(graph_name).exists(), is(true));
//		assertThat("Vertex label not found in db", db.label(String.format("%smale", socialPrefix)).exists(), is(true));
//		assertThat("Vertex label not found in db", db.label(String.format("%sfemale", socialPrefix)).exists(), is(true));
//		assertThat("Edge label found in db", db.label(String.format("%srelation", socialPrefix)).exists(), is(true));
//		GraphClient g = db.graph(graph_name);
//		Collection<EdgeDefinition> defs = g.getInfo().getEdgeDefinitions();
//		assertThat(defs, hasSize(2));
//		EdgeDefinition d = defs.iterator().next();
//		assertThat(d.getCollection(), is(String.format("%srelation", socialPrefix)));
//		assertThat(d.getFrom(), containsInAnyOrder(String.format("%smale", socialPrefix), String.format("%sfemale", socialPrefix)));
//		assertThat(d.getTo(), containsInAnyOrder(String.format("%smale", socialPrefix), String.format("%sfemale", socialPrefix)));
//	}
//
//	@Test
//	public void test_CreateMultiVertexMultiEdgeGraph() throws Exception {
//		String graph_name = "routeplanner";
//		List<String> verticesCollectionNames = new ArrayList<>();
//		List<String> edgesCollectionNames = new ArrayList<>();
//		List<String> relations = new ArrayList<>();
//		verticesCollectionNames.add("germanCity");
//		verticesCollectionNames.add("frenchCity");
//		edgesCollectionNames.add("frenchHighway");
//		edgesCollectionNames.add("germanHighway");
//		edgesCollectionNames.add("internationalHighway");
//		relations.add("frenchHighway:frenchCity->frenchCity");
//		relations.add("germanHighway:germanCity->germanCity");
//		relations.add("internationalHighway:germanCity,frenchCity->germanCity,frenchCity");
//
//
//		client.createGraph(graph_name, verticesCollectionNames, edgesCollectionNames, relations);
//		DatabaseClient db = client.getDB();
//		assertThat("Craeted graph not found in db", db.graph(graph_name).exists(), is(true));
//		assertThat("Vertex label not found in db", db.label(String.format("%sgermanCity", routeplannerPrefix)).exists(), is(true));
//		assertThat("Vertex label not found in db", db.label(String.format("%sfrenchCity", routeplannerPrefix)).exists(), is(true));
//		assertThat("Edge label found in db", db.label(String.format("%sfrenchHighway", routeplannerPrefix)).exists(), is(true));
//		assertThat("Edge label found in db", db.label(String.format("%sgermanHighway", routeplannerPrefix)).exists(), is(true));
//		assertThat("Edge label found in db", db.label(String.format("%sinternationalHighway", routeplannerPrefix)).exists(), is(true));
//		GraphClient g = db.graph(graph_name);
//		Collection<EdgeDefinition> defs = g.getInfo().getEdgeDefinitions();
//		assertThat("Not all edge definitions created", defs, hasSize(4));
//		List<String> edgeNames = defs.stream().map(EdgeDefinition::getCollection).collect(Collectors.toList());
//		assertThat("Missmatch name in edge collecion names", edgeNames,
//                containsInAnyOrder(String.format("%sfrenchHighway", routeplannerPrefix),
//                        String.format("%sgermanHighway", routeplannerPrefix),
//                        String.format("%sinternationalHighway", routeplannerPrefix), "routeplanner_ELEMENT-HAS-PROPERTIES"));
//		EdgeDefinition fh = defs.stream().filter(ed -> String.format("%sfrenchHighway", routeplannerPrefix).equals(ed.getCollection())).findFirst().get();
//		assertThat("FrenchHighway connects incorrect collections", fh.getFrom(), contains(String.format("%sfrenchCity", routeplannerPrefix)));
//		assertThat("FrenchHighway connects incorrect collections", fh.getTo(), contains(String.format("%sfrenchCity", routeplannerPrefix)));
//		EdgeDefinition gh = defs.stream().filter(ed -> String.format("%sgermanHighway", routeplannerPrefix).equals(ed.getCollection())).findFirst().get();
//		assertThat("GermanHighway connects incorrect collections", gh.getFrom(), contains(String.format("%sgermanCity", routeplannerPrefix)));
//		assertThat("GermanHighway connects incorrect collections", gh.getTo(), contains(String.format("%sgermanCity", routeplannerPrefix)));
//		EdgeDefinition ih = defs.stream().filter(ed -> String.format("%sinternationalHighway", routeplannerPrefix).equals(ed.getCollection())).findFirst().get();
//		assertThat("InternationalHighway connects incorrect collections", ih.getFrom(),
//                containsInAnyOrder(String.format("%sfrenchCity", routeplannerPrefix), String.format("%sgermanCity", routeplannerPrefix)));
//		assertThat("InternationalHighway connects incorrect collections", ih.getTo(),
//                containsInAnyOrder(String.format("%sfrenchCity", routeplannerPrefix), String.format("%sgermanCity", routeplannerPrefix)));
//	}
//
//

}
