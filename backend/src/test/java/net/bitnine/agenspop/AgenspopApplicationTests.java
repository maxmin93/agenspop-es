package net.bitnine.agenspop;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.bitnine.agenspop.elasticgraph.ElasticGraphAPI;
import net.bitnine.agenspop.graph.AgensGraphManager;
import net.bitnine.agenspop.graph.structure.*;
import net.bitnine.agenspop.service.AgensGremlinService;
import net.bitnine.agenspop.util.AgensJacksonModule;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.RestHighLevelClient;
import org.hamcrest.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.web.client.RestTemplate;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

@RunWith(SpringRunner.class)
@ActiveProfiles("local")
@SpringBootTest(
		webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
		properties = "classpath:application.yml"
)
public class AgenspopApplicationTests {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${agens.api.base-path}")
	String basePath;		// => "/api"

	@LocalServerPort
	private int port;
	// private URL base;

	@Autowired
	private TestRestTemplate rest;
	@Autowired
	private AgensGremlinService gremlin;
	@Autowired
	private AgensGraphManager manager;
	@Autowired
	private ElasticGraphAPI api;
	@Autowired
	private RestHighLevelClient client;

	AgensGraph g;
	String gName = "test";
	String datasource = "modern";

	//////////////////////////////////////////////

	@Test
	public void contexLoads() {
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("loading sample graph.. ["+gName+"]\n");
		assertThat( client, is(notNullValue()));
		assertThat( gremlin, is(notNullValue()));
		assertThat( api, is(notNullValue()));
		assertThat( manager, is(notNullValue()));
		assertThat( rest, is(notNullValue()));
	}

	@Test
	public void restShouldReturnDefaultMessage() {
		String prefixUrl = "http://localhost:" + port + basePath;

		assertThat( this.rest.getForObject(prefixUrl+"/graph/hello",
				String.class), is(containsString("Hello, graph!")));
		assertThat( this.rest.getForObject(prefixUrl+"/admin/hello",
				String.class), is(containsString("Hello, admin!")));
		assertThat( this.rest.getForObject(prefixUrl+"/search/hello",
				String.class), is(containsString("Hello, search!")));
		assertThat( this.rest.getForObject(prefixUrl+"/spark/hello",
				String.class), is(containsString("Hello, spark!")));
	}

	@Test
	public void modernSearchShouldReturnMessage() throws URISyntaxException {
		RestTemplate restTemplate = new RestTemplate();
		String prefixUrl = "http://localhost:" + port + basePath + "/search/";

		URI uri;
		ResponseEntity<List> result;

		uri = new URI(prefixUrl + datasource + "/v");
		result = restTemplate.getForEntity(uri, List.class);
		assertTrue("search: v.size = 6", result.getBody().size() == 6);

		uri = new URI(prefixUrl + datasource + "/e");
		result = restTemplate.getForEntity(uri, List.class);
		assertTrue("search: e.size = 6", result.getBody().size() == 6);
	}

	@Test
	public void modernGraphGremlinShouldReturnMessage() throws URISyntaxException {
		RestTemplate restTemplate = new RestTemplate();
		String prefixUrl = "http://localhost:" + port + basePath + "/graph";

		URI uri;
		ResponseEntity<List> result;

		uri = new URI(prefixUrl + "/gremlin?q=" + datasource + "_g.V()");
		result = restTemplate.getForEntity(uri, List.class);
		assertTrue("search: v.size = 6", result.getBody().size() == 6);

		uri = new URI(prefixUrl + "/gremlin?q=" + datasource + "_g.E()");
		result = restTemplate.getForEntity(uri, List.class);
		assertTrue("search: e.size = 6", result.getBody().size() == 6);
	}

	@Test
	public void modernGraphCypherShouldReturnMessage() throws URISyntaxException {
		RestTemplate restTemplate = new RestTemplate();
		String prefixUrl = "http://localhost:" + port + basePath + "/graph";

		URI uri;
		ResponseEntity<List> result;

		// match (a:person {country: 'USA'}) return a, id(a) limit 10;
		uri = new URI(prefixUrl + "/cypher?ds="+datasource+"&q=match%20(a:person%20%7Bcountry:%20%27USA%27%7D)%20return%20a,%20id(a)%20limit%2010");
		result = restTemplate.getForEntity(uri, List.class);
		assertTrue("search: v.size = 4", result.getBody().size() == 4);

		// match (a:person {name: 'marko'})-[b]->(c) return b, id(b) limit 10;
		uri = new URI(prefixUrl + "/cypher?ds="+datasource+"&q=match%20(a:person%20%7Bname:%20%27marko%27%7D)-[b]-%3E(c)%20return%20b,%20id(b)%20limit%2010");
		result = restTemplate.getForEntity(uri, List.class);
		assertTrue("search: e.size = 3", result.getBody().size() == 3);
	}

	@Test
	public void modernAdminShouldReturnMessage() throws URISyntaxException {
		RestTemplate restTemplate = new RestTemplate();
		String prefixUrl = "http://localhost:" + port + basePath + "/admin";

		URI uri;
		ResponseEntity<Map> result;

		uri = new URI(prefixUrl + "/labels/" + datasource);
		result = restTemplate.getForEntity(uri, Map.class);			// V, E
		assertTrue("meta: keys = 2", result.getBody().keySet().size() == 2);

		uri = new URI(prefixUrl + "/keys/" + datasource + "/person");
		result = restTemplate.getForEntity(uri, Map.class);
		// exclude graph features like started with '_$$'
		List<String> keys = (List<String>) result.getBody().keySet().stream().filter(k->!((String) k).startsWith("_$$")).collect(Collectors.toList());
		assertTrue("meta: keys.elements = 3", keys.size() == 3);
	}

	//////////////////////////////////////////////////////////////

	@Test
	public void graphManagerLoad() {
		Set<String> names = manager.getGraphNames();
		assertNotNull(names);
		assertTrue("graphs is empty", names.size() > 0);
	}

	@Test
	public void apiAdminLabels() {
		Map<String, Map<String,Long>> labels = manager.getGraphLabels(datasource);
		assertNotNull(labels);
		assertEquals( "labels of V = "+labels.get("V").size(), 2, labels.get("V").size());
		assertEquals( "labels of E = "+labels.get("E").size(), 2, labels.get("E").size());
	}

	@Test
	public void apiAdminKeys() {
		String label = "person";
		Map<String,Long> keys = manager.getGraphKeys(datasource, label);
		assertNotNull(keys);
		// exclude graph features like started with '_$$'
		List<String> results = keys.keySet().stream().filter(k->!k.startsWith("_$$")).collect(Collectors.toList());
		assertEquals( "keys of '"+label+"' = "+results.size(), 3, results.size());
	}

	private Stream<Object> runGremlin(String datasource, String script){
		Stream<Object> stream = Stream.empty();
		try {
			CompletableFuture<?> future = gremlin.runGremlin(datasource, script);
			CompletableFuture.allOf(future).join();
			stream = (Stream<Object>) future.get();
		}catch (Exception ex){
			System.out.println("** ERROR: runGremlin ==> " + ex.getMessage());
		}
		return stream;
	}

	@Test
	public void apiGraphGremlinGrammar01(){
		String script;
		long result;

		script = "g.V()";
		result = runGremlin("modern", script).count();
		assertEquals( script+" => "+result, 6, result);

		script = "g.E()";
		result = runGremlin("modern", script).count();
		assertEquals( script+" => "+result, 6, result);
	}

	@Test
	public void apiGraphGremlinGrammar02(){
		String script;
		long result;

		script = "g.V().has(\"age\",gt(30))";
		result = runGremlin("modern", script).count();
		assertEquals( script+" => "+result, 2, result);

		script = "g.V().has(\"name\",\"marko\").out().out()";
		result = runGremlin("modern", script).count();
		assertEquals( script+" => "+result, 2, result);

		List<Object> list;

		// 2 = modern_g.V().groupCount().by(T.label)
		script = "g.V().groupCount().by(T.label)";
		list = runGremlin("modern", script).collect(Collectors.toList());
		assertTrue(list.size() > 0);
		Map<String,Long> grp1 = (Map<String,Long>)list.get(0);
		assertEquals( script+" => "+grp1.size(), 2, grp1.size());

		// 3 = modern_g.V().hasLabel("person").properties().key().groupCount()
		script = "g.V().hasLabel(\"person\").properties().key().groupCount()";
		list = runGremlin("modern", script).collect(Collectors.toList());
		assertTrue(list.size() > 0);
		Map<String,Long> grp2 = (Map<String,Long>)list.get(0);
		// exclude graph features like started with '_$$'
		List<String> results = grp2.keySet().stream().filter(k->!k.startsWith("_$$")).collect(Collectors.toList());
		assertEquals( script+" => "+results.size(), 3, results.size());

		// 2 = modern_g.E().project("self","inL","outL").by(__.label()).by(__.inV().label()).by(__.outV().label()).groupCount()
		script = "g.E().project(\"self\",\"inL\",\"outL\").by(__.label()).by(__.inV().label()).by(__.outV().label()).groupCount()";
		list = runGremlin("modern", script).collect(Collectors.toList());
		assertTrue(list.size() > 0);
		Map<String,Long> grp3 = (Map<String,Long>)list.get(0);
		assertEquals( script+" => "+grp3.size(), 2, grp3.size());
	}

	private Stream<Object> runCypher(String script){
		Stream<Object> stream = Stream.empty();
		try {
			CompletableFuture<?> future = gremlin.runCypher(script, datasource);
			CompletableFuture.allOf(future).join();
			stream = (Stream<Object>) future.get();
		}catch (Exception ex){
			System.out.println("** ERROR: runCypher ==> " + ex.getMessage());
		}
		return stream;
	}

	@Test
	public void apiGraphCypherGrammar(){
		String script;
		long result;

		script = "match (a:person) return a, id(a) limit 2";
		result = runCypher(script).count();
		assertEquals( script+" => "+result, 2, result);

		script = "match (a {name:'marko'})-[b]->(c) return b, id(b)";
		result = runCypher(script).count();
		assertEquals( script+" => "+result, 3, result);
	}

	@Test
	public void apiSearchTest(){
		this.g = (AgensGraph) manager.openGraph(datasource);
		assertNotNull(this.g);
		GraphTraversalSource t = this.g.traversal();
		assertNotNull(t);

		List<Vertex> vertexList = t.V().next(100);
		assertEquals( "V.all => "+vertexList.size(), 6, vertexList.size());

		List<Edge> edgeList = t.E().next(100);
		assertEquals( "E.all => "+edgeList.size(), 6, edgeList.size());

		vertexList = t.V("modern_5", "modern_4", "modern_3").next(100);
		assertEquals( "V(id..) => "+vertexList.size(), 3, vertexList.size());

		edgeList = t.V("modern_1").bothE().next(100);
		assertEquals( "V(id).bothE => "+edgeList.size(), 3, edgeList.size());

		Vertex v1 = t.V("modern_1").next();
		vertexList = t.V(v1.id()).out().next(100);  // BUT, on groovy ==> g.V(v1).out()
		assertEquals( "V(id).out => "+vertexList.size(), 3, vertexList.size());

		List<Object> valueList = t.V().values("name").next(100);
		assertEquals( "V.values('name') => "+valueList.size(), 6, valueList.size());

		vertexList = t.V().has("name","josh").next(100);
		assertEquals( "V.has(key,value) => "+vertexList.size(), 1, vertexList.size());

		edgeList = t.V().hasLabel("person").outE("knows").next(100);
		assertEquals( "V.hasLabel.outE => "+edgeList.size(), 2, edgeList.size());

		vertexList = t.V().hasLabel("person").out().where(__.values("age").is(P.lt(30))).next(100);
		assertEquals( "V.where(age<30) => "+vertexList.size(), 1, vertexList.size());

		this.g.close();
	}

	/*
	@Test
	public void jsonSerializeTest() throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new AgensJacksonModule());

		final AgensVertex v = (AgensVertex) g.addVertex(
				T.id, "modern_1", T.label, "person"
				, "name", "marko", "age", 29, "country", "USA"
		);
		String json = mapper.writeValueAsString(v);
	}
	*/

	////////////////////////////////////////////////
	//
	// **참고
	// https://www.baeldung.com/spring-boot-testresttemplate
	// https://www.baeldung.com/java-assert-string-not-empty
	// Null 값 체크 ==> assertThat( mc, is(nullValue()) );
	// https://m.blog.naver.com/PostView.nhn?blogId=simpolor&logNo=221327833587&proxyReferer=https%3A%2F%2Fwww.google.com%2F
	// https://www.lesstif.com/pages/viewpage.action?pageId=18219426#hamcrest%EB%A1%9C%EA%B0%80%EB%8F%85%EC%84%B1%EC%9E%88%EB%8A%94jUnitTestCase%EB%A7%8C%EB%93%A4%EA%B8%B0-Numbers
	//

	@Test
	public void createTestGraph() throws Exception {

		Graph removed = manager.removeGraph(gName);
		if( removed != null ) {
			System.out.println("graph[" + gName + "] removed: " + removed.toString());
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		AgensGraph g = AgensFactory.createEmpty(api, gName);
		assertNotNull(g);
		assertThat(g, instanceOf(AgensGraph.class));

		this.g = (AgensGraph) manager.openGraph(gName);
		assertNotNull(g);

		final Vertex marko = g.addVertex(T.id, gName+"_1", T.label, "person", "name", "marko", "age", 29, "country", "USA");
		final Vertex vadas = g.addVertex(T.id, gName+"_2", T.label, "person", "name", "vadas", "age", 27, "country", "USA");
		final Vertex lop = g.addVertex(T.id, gName+"_3", T.label, "software", "name", "lop", "lang", "java");
		final Vertex josh = g.addVertex(T.id, gName+"_4", T.label, "person", "name", "josh", "age", 32, "country", "USA");
		final Vertex ripple = g.addVertex(T.id, gName+"_5", T.label, "software", "name", "ripple", "lang", "java");
		final Vertex peter = g.addVertex(T.id, gName+"_6", T.label, "person", "name", "peter", "age", 35, "country", "USA");

		marko.addEdge("knows", vadas, T.id, gName+"_7", "weight", 0.5d);
		marko.addEdge("knows", josh, T.id, gName+"_8", "weight", 1.0d);
		marko.addEdge("created", lop, T.id, gName+"_9", "weight", 0.4d);
		josh.addEdge("created", ripple, T.id, gName+"_10", "weight", 1.0d);
		josh.addEdge("created", lop, T.id, gName+"_11", "weight", 0.4d);
		peter.addEdge("created", lop, T.id, gName+"_12", "weight", 0.2d);

		try {
			Thread.sleep(1500); 	// Then do something meaningful...
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		long sizeV = api.countV(gName);
		long sizeE = api.countE(gName);

		assertThat("Any vertex insert fails", sizeV, is(6L));
		assertThat("Any edge insert fails", sizeE, is(6L));

		GraphTraversalSource t = this.g.traversal();
		assertNotNull(t);

		String vid = gName+"_1";
		AgensVertex v1 = (AgensVertex) t.V(vid).next();
		assertNotNull(v1);

		v1.remove();	// remove vertex
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		sizeV = api.countV(gName);
		sizeE = api.countE(gName);
		assertThat("Removing "+v1.toString()+" fails", sizeV, is(5L));
		assertThat("Removing some edges fails", sizeE, is(3L));

		removed = manager.removeGraph(gName);
		assertNotNull(removed);
		if( removed != null ) {
			System.out.println("graph["+gName+"] removed: "+removed.toString());
			try {
				Thread.sleep(500); 	// sleep
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		Set<String> names = manager.getGraphNames();
		assertNotNull(names);
		System.out.println("graphNames("+names.size()+") => "+names.toString());
		assertTrue("graph["+gName+"] still exists", !names.contains(gName));
	}

}
