package com.arangodb.tinkerpop.gremlin.client.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphException;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.*;
import org.junit.rules.ExpectedException;

import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;

/**
 * This tests require four users:
 * <ul>
 *  <li> root:	   To test DB creation, requires root access
 *  <li> gremlin:  To test graph/collection creation+access in the db, requires "Administrate" permission to DB 
 *  <li> reader:   To test graph/collection access in the db, requires "Access" permission to DB 
 *  <li> limited:  To test no access to the db,  requires "No access" permission to DB 
 * </ul> 
 * For all users (except root) the password is expected to be the same as the username. For the root,
 * the password must be set via environment variable ARANGODB_ROOT_PWD (so no system password is shared
 * via code).  
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 *
 */
@Ignore
public class ClientTest {
	
	@Rule
	public ExpectedException exception = ExpectedException.none();
	
	private PropertiesConfiguration configuration;
	private ArangoDBGraphClient client;

	@Before
	public void setUp() throws Exception {

		configuration = new PropertiesConfiguration();
		configuration.setProperty("arangodb.hosts", "127.0.0.1:8529");
		configuration.setProperty("arangodb.user", "gremlin");
		configuration.setProperty("arangodb.password", "gremlin");
		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
		client = new ArangoDBGraphClient(arangoProperties, "tinkerpop", 30000);
	}
	
	@After
	public void tearDown() {
		// Drop used graphs and collections, if present
		ArangoDatabase db = client.getDB();
		// know_graph
		try { db.graph("knows_graph").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("person").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("knows").drop(); } catch (ArangoDBException ex) { }
		// social graph
		try { db.graph("social").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("male").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("female").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("relation").drop(); } catch (ArangoDBException ex) { }
		// city graph
		try { db.graph("routeplanner").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("germanCity").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("frenchCity").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("frenchHighway").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("germanHighway").drop(); } catch (ArangoDBException ex) { }
		try { db.collection("internationalHighway").drop(); } catch (ArangoDBException ex) { }
		client.shutdown();
		client = null;
	}
	
	@Test
	public void test_RestritedUserNewDatabase() throws Exception {
		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
		
		exception.expect(ArangoDBGraphException.class);
		exception.expectMessage(startsWith("Unable to crate the database"));
		new ArangoDBGraphClient(arangoProperties, "demo", 30000);
	}

	@Test
	public void test_AuthorizedUserNewDatabase() throws Exception {
		org.junit.Assume.assumeTrue(System.getenv("ARANGODB_ROOT_PWD") != null);
		configuration.setProperty("arangodb.user", "root");
		String pwd = System.getenv("ARANGODB_ROOT_PWD");
		configuration.setProperty("arangodb.password", pwd);
		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
		ArangoDBGraphClient localClient = new ArangoDBGraphClient(arangoProperties, "demo", 30000);
		assertThat(localClient.dbExists(), is(true));
		localClient.deleteDb();
		localClient.shutdown();
	}
	
	@Test
	public void test_RestritedUserExistingDb() throws Exception {
		configuration.setProperty("arangodb.user", "limited");
		configuration.setProperty("arangodb.password", "limited");
		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
		exception.expect(ArangoDBGraphException.class);
		exception.expectMessage(startsWith("DB not found or user has no access:"));
		new ArangoDBGraphClient(arangoProperties, "tinkerpop", 30000, false);
	}
	
	@Test
	public void test_ReadAccessUserCreateGraph() throws Exception {
		configuration.setProperty("arangodb.user", "reader");
		configuration.setProperty("arangodb.password", "reader");
		Properties arangoProperties = ConfigurationConverter.getProperties(configuration);
		ArangoDBGraphClient localClient = new ArangoDBGraphClient(arangoProperties, "tinkerpop", 30000);
		assertThat(localClient.dbExists(), is(true));
		List<String> verticesCollectionNames = new ArrayList<>(); 
		List<String> edgesCollectionNames = new ArrayList<>();
		verticesCollectionNames.add("person");
		edgesCollectionNames.add("knows");
		
		exception.expect(ArangoDBGraphException.class);
		exception.expectMessage(startsWith("Error creating graph"));
		localClient.createGraph("knows_graph", verticesCollectionNames, edgesCollectionNames, Collections.emptyList());
		localClient.shutdown();
	}
	
	// ********* The following tests use the @Setup client ********* 
	
	@Test
	public void test_ServerVersion() throws Exception {
		String version = client.getVersion();
		assertThat(version, is(notNullValue()));
	}
	
	@Test
	public void test_CreateSimpleGraph() throws Exception {
		
		String graph_name = "knows_graph";
		List<String> verticesCollectionNames = new ArrayList<>(); 
		List<String> edgesCollectionNames = new ArrayList<>();
		verticesCollectionNames.add("person");
		edgesCollectionNames.add("knows");
		
		client.createGraph(graph_name, verticesCollectionNames, edgesCollectionNames, Collections.emptyList());
		ArangoDatabase db = client.getDB();
		assertThat("Craeted graph not found in db", db.graph(graph_name).exists(), is(true));
		assertThat("Vertex collection found in db", db.collection("person").exists(), is(true));
		assertThat("Edge collection found in db", db.collection("knows").exists(), is(true));
		ArangoGraph g = db.graph(graph_name);
		Collection<EdgeDefinition> defs = g.getInfo().getEdgeDefinitions();
		assertThat(defs, hasSize(1));
		EdgeDefinition d = defs.iterator().next();
		assertThat(d.getCollection(), is("knows"));
		assertThat(d.getFrom(), contains("person"));
		assertThat(d.getTo(), contains("person"));
	}
	
	@Test
	public void test_CreateMultiVertexGraph() throws Exception {
		String graph_name = "social";
		List<String> verticesCollectionNames = new ArrayList<>(); 
		List<String> edgesCollectionNames = new ArrayList<>();
		List<String> relations = new ArrayList<>();
		verticesCollectionNames.add("male");
		verticesCollectionNames.add("female");
		edgesCollectionNames.add("relation");
		relations.add("relation:male,female->male,female");
		
		
		client.createGraph(graph_name, verticesCollectionNames, edgesCollectionNames, relations);
		ArangoDatabase db = client.getDB();
		assertThat("Craeted graph not found in db", db.graph(graph_name).exists(), is(true));
		assertThat("Vertex collection not found in db", db.collection("male").exists(), is(true));
		assertThat("Vertex collection not found in db", db.collection("female").exists(), is(true));
		assertThat("Edge collection found in db", db.collection("relation").exists(), is(true));
		ArangoGraph g = db.graph(graph_name);
		Collection<EdgeDefinition> defs = g.getInfo().getEdgeDefinitions();
		assertThat(defs, hasSize(1));
		EdgeDefinition d = defs.iterator().next();
		assertThat(d.getCollection(), is("relation"));
		assertThat(d.getFrom(), containsInAnyOrder("male", "female"));
		assertThat(d.getTo(), containsInAnyOrder("male", "female"));
	}
	
	@Test
	public void test_CreateMultiVertexMultiEdgeGraph() throws Exception {
		String graph_name = "routeplanner";
		List<String> verticesCollectionNames = new ArrayList<>(); 
		List<String> edgesCollectionNames = new ArrayList<>();
		List<String> relations = new ArrayList<>();
		verticesCollectionNames.add("germanCity");
		verticesCollectionNames.add("frenchCity");
		edgesCollectionNames.add("frenchHighway");
		edgesCollectionNames.add("germanHighway");
		edgesCollectionNames.add("internationalHighway");
		relations.add("frenchHighway:frenchCity->frenchCity");
		relations.add("germanHighway:germanCity->germanCity");
		relations.add("internationalHighway:germanCity,frenchCity->germanCity,frenchCity");
		
		
		client.createGraph(graph_name, verticesCollectionNames, edgesCollectionNames, relations);
		ArangoDatabase db = client.getDB();
		assertThat("Craeted graph not found in db", db.graph(graph_name).exists(), is(true));
		assertThat("Vertex collection not found in db", db.collection("germanCity").exists(), is(true));
		assertThat("Vertex collection not found in db", db.collection("frenchCity").exists(), is(true));
		assertThat("Edge collection found in db", db.collection("frenchHighway").exists(), is(true));
		assertThat("Edge collection found in db", db.collection("germanHighway").exists(), is(true));
		assertThat("Edge collection found in db", db.collection("internationalHighway").exists(), is(true));
		ArangoGraph g = db.graph(graph_name);
		Collection<EdgeDefinition> defs = g.getInfo().getEdgeDefinitions();
		assertThat("Not all edge definitions created", defs, hasSize(3));
		List<String> edgeNames = defs.stream().map(EdgeDefinition::getCollection).collect(Collectors.toList());
		assertThat("Missmatch name in edge collecion names", edgeNames, containsInAnyOrder("frenchHighway", "germanHighway", "internationalHighway"));
		EdgeDefinition fh = defs.stream().filter(ed -> "frenchHighway".equals(ed.getCollection())).findFirst().get();
		assertThat("FrenchHighway connects incorrect collections", fh.getFrom(), contains("frenchCity"));
		assertThat("FrenchHighway connects incorrect collections", fh.getTo(), contains("frenchCity"));
		EdgeDefinition gh = defs.stream().filter(ed -> "germanHighway".equals(ed.getCollection())).findFirst().get();
		assertThat("GermanHighway connects incorrect collections", gh.getFrom(), contains("germanCity"));
		assertThat("GermanHighway connects incorrect collections", gh.getTo(), contains("germanCity"));
		EdgeDefinition ih = defs.stream().filter(ed -> "internationalHighway".equals(ed.getCollection())).findFirst().get();
		assertThat("InternationalHighway connects incorrect collections", ih.getFrom(), containsInAnyOrder("frenchCity", "germanCity"));
		assertThat("InternationalHighway connects incorrect collections", ih.getTo(), containsInAnyOrder("frenchCity", "germanCity"));
	}
	
	

}
