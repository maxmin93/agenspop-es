package net.bitnine.agenspop.web;

import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.config.properties.FrontProperties;
import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.elasticgraph.model.ElasticEdge;
import net.bitnine.agenspop.elasticgraph.model.ElasticVertex;
import net.bitnine.agenspop.graph.AgensGraphManager;
import net.bitnine.agenspop.graph.structure.AgensFactory;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.util.AgensUtilHelper;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping(value = "${agens.api.base-path}/admin")
public class ManagerController {

    private final AgensGraphManager manager;
    private final ProductProperties productProperties;
    private final FrontProperties frontProperties;

    @Autowired
    public ManagerController(
            AgensGraphManager manager,
            ProductProperties productProperties,
            FrontProperties frontProperties
    ){
        this.manager = manager;
        this.productProperties = productProperties;
        this.frontProperties = frontProperties;
    }

    ///////////////////////////////////////////

    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, admin!\"}";
    }

    @GetMapping(value="/config", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<Map<String,Object>> config() throws Exception {
        return new ResponseEntity<>(frontProperties.toMap()
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value="/graphs", produces="application/json; charset=UTF-8")
    public ResponseEntity<Map<String, String>> listGraphs() throws Exception {
        Set<String> names = manager.getGraphNames();
        Map<String, String> graphs = new HashMap<>();
        for( String name : names ){
            graphs.put(name, manager.getGraph(name).toString());
        }
        return new ResponseEntity<Map<String, String>>(graphs
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value="/graphs/search/{query}", produces="application/json; charset=UTF-8")
    public ResponseEntity searchGraphs(@PathVariable String query) throws Exception {
        Map<String, String> graphs = manager.searchGraphs(query);
        return new ResponseEntity(graphs
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value="/labels/{datasource}", produces="application/json; charset=UTF-8")
    public ResponseEntity listLabels(@PathVariable String datasource) throws Exception {
        Map<String,Map<String,Long>> labels = manager.getGraphLabels(datasource);
        return new ResponseEntity(labels
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value="/keys/{datasource}/{label}", produces="application/json; charset=UTF-8")
    public ResponseEntity listKeys(@PathVariable String datasource, @PathVariable String label) throws Exception {
        Map<String,Long> keys = manager.getGraphKeys(datasource, label);
        return new ResponseEntity(keys
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    // reset modern graph
    @GetMapping(value="/reset/sample", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> resetGraph() throws Exception {
        String gName = "modern";
        // if modern graph is under wrong status, remove it
        try { manager.removeGraph(gName); } catch( Exception e ){ }
        try {
            System.out.println("remove sample graph.. ["+gName+"]");
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        AgensGraph g = manager.resetSampleGraph();

        // AgensFactory.traversalTestModern(g);
        String message = "{\"reset\" : \""+g.toString()+"\"}";
        return new ResponseEntity<String>(message
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    // remove graph
    @GetMapping(value="/remove/{graph}", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> removeGraph(@PathVariable String graph) throws Exception {
        Graph g = manager.removeGraph(graph);

        String message = "{\"remove\" : \""+(g==null ? "null":g.toString())+"\"}";
        return new ResponseEntity<String>(message
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    // reload graphs
    @GetMapping(value="/update", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> updateGraphs() throws Exception {
        manager.updateGraphs();
        return new ResponseEntity<Map<String,String>>( manager.getGraphStates()
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

/*
curl -X DELETE "27.117.163.21:15619/elasticvertex?pretty"
curl -X DELETE "27.117.163.21:15619/elasticedge?pretty"

    // list labels of graph
    @GetMapping(value="/labels/{graph}", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> listLabels(@PathVariable String graph) throws Exception {

        return new ResponseEntity<String>(graphs, productHeaders(), HttpStatus.OK);
    }

    @PutMapping("/reset")
    public ResponseEntity resetIndex() throws Exception {
        return new ResponseEntity(base.reset(), HttpStatus.OK);
    }

    @DeleteMapping("/{datasource}")
    public ResponseEntity remove(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.remove(datasource), HttpStatus.OK);
    }


    ///////////////////////////////////////////////////////////////

    //curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v01", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"java"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"5"}] }' localhost:8080/elastic/v
    //curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v02", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"typescript"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"3"}] }' localhost:8080/elastic/v
    //curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v03", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"html5/css"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"4"}] }' localhost:8080/elastic/v
    //curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v04", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"python"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"6"}] }' localhost:8080/elastic/v
    //curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e01", "label":"knows", "datasource": "sample", "src":"v01", "dst":"v02", "properties": [ {"key":"year", "type": "java.lang.String", "value":"2001"}, {"key":"relation", "type": "java.lang.String", "value":"friend"}] }' localhost:8080/elastic/e
    //curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e02", "label":"knows", "datasource": "sample", "src":"v02", "dst":"v03", "properties": [ {"key":"year", "type": "java.lang.String", "value":"2003"}, {"key":"relation", "type": "java.lang.String", "value":"crew"}] }' localhost:8080/elastic/e
    //curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e03", "label":"knows", "datasource": "sample", "src":"v02", "dst":"v03", "properties": [ {"key":"year", "type": "java.lang.String", "value":"1982"}, {"key":"relation", "type": "java.lang.String", "value":"family"}] }' localhost:8080/elastic/e
    //
    //curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v02", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"typescript"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"2"}, {"key":"gpa", "type": "java.lang.Float", "value":"3.7"}] }' localhost:8080/elastic/v
    //curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e02", "label":"knows", "datasource": "sample", "src":"v02", "dst":"v03", "properties": [ {"key":"year", "type": "java.lang.String", "value":"2003"}] }' localhost:8080/elastic/e

    @PostMapping("/v")
    @PutMapping("/v")
    public ResponseEntity saveVertex(@RequestBody ElasticVertex document) throws Exception {
        return new ResponseEntity(base.saveVertex(document), HttpStatus.CREATED);
    }
    @PostMapping("/e")
    @PutMapping("/e")
    public ResponseEntity saveEdge(@RequestBody ElasticEdge document) throws Exception {
        return new ResponseEntity(base.saveEdge(document), HttpStatus.CREATED);
    }

    //curl -X DELETE "localhost:8080/elastic/v/v04"
    //==> 자동으로 연결된 간선들[e03]도 제거 되어야 함 (cascade)

    @DeleteMapping("/v/{id}")
    public ResponseEntity dropVertex(@PathVariable String id) throws Exception {
        base.dropVertex(id);
        return new ResponseEntity(true, HttpStatus.OK);
    }
    @DeleteMapping("/e/{id}")
    public ResponseEntity dropEdge(@PathVariable String id) throws Exception {
        base.dropEdge(id);
        return new ResponseEntity(true, HttpStatus.OK);
    }

 */
}
