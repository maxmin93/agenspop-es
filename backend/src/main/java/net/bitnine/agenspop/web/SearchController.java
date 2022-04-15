package net.bitnine.agenspop.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.elasticgraph.ElasticGraphAPI;
import net.bitnine.agenspop.util.AgensJacksonModule;
import net.bitnine.agenspop.util.AgensUtilHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RestController
@RequestMapping(value = "${agens.api.base-path}/search")
public class SearchController {
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ElasticGraphAPI base;
    private final ProductProperties productProperties;

    @Autowired
    public SearchController(
            ElasticGraphAPI base, ObjectMapper objectMapper, ProductProperties productProperties
    ){
        this.base = base;
        this.mapper.registerModule(new AgensJacksonModule());
        this.productProperties = productProperties;
    }

    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, search!\"}";
    }
    
    ///////////////////////////////////////////////////////////////

    @GetMapping(value="/count", produces="application/json; charset=UTF-8")
    public ResponseEntity count() throws Exception {
        return new ResponseEntity(base.count(), HttpStatus.OK);
    }
    @GetMapping(value="/{datasource}/count", produces="application/json; charset=UTF-8")
    public ResponseEntity count(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.count(datasource), HttpStatus.OK);
    }
    @GetMapping(value="/{datasource}/labels", produces="application/json; charset=UTF-8")
    public ResponseEntity labels(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.labels(datasource), HttpStatus.OK);
    }

    @GetMapping(value="/{datasource}/v/{label}/keys", produces="application/json; charset=UTF-8")
    public ResponseEntity vertexLabelKeys(@PathVariable String datasource, @PathVariable String label) throws Exception {
        return new ResponseEntity(base.listVertexLabelKeys(datasource, label), HttpStatus.OK);
    }
    @GetMapping(value="/{datasource}/e/{label}/keys", produces="application/json; charset=UTF-8")
    public ResponseEntity edgeLabelKeys(@PathVariable String datasource, @PathVariable String label) throws Exception {
        return new ResponseEntity(base.listEdgeLabelKeys(datasource, label), HttpStatus.OK);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/v/v01"
curl -X GET "localhost:8080/elastic/e/e01"
    */
    @GetMapping(value="/v/{id}", produces="application/json; charset=UTF-8")
    public ResponseEntity findV(@PathVariable String id) throws Exception {
        Optional<BaseVertex> v = base.getVertexById(id);
        String json = !v.isPresent() ? "{}" : mapper.writeValueAsString(v);
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value="/e/{id}", produces="application/json; charset=UTF-8")
    public ResponseEntity findE(@PathVariable String id) throws Exception {
        Optional<BaseEdge> e = base.getEdgeById(id);
        String json = !e.isPresent() ? "{}" : mapper.writeValueAsString(e);
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    /*
curl -X GET "localhost:8080/elastic/sample/v"
curl -X GET "localhost:8080/elastic/sample/e"
    */
/*
    @GetMapping(value="/{datasource}/v", produces="application/json; charset=UTF-8")
    public ResponseEntity findV_All(@PathVariable String datasource) throws Exception {
        String json = mapper.writeValueAsString( base.vertices(datasource) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }
    @GetMapping(value="/{datasource}/e", produces="application/json; charset=UTF-8")
    public ResponseEntity findE_All(@PathVariable String datasource) throws Exception {
        String json = mapper.writeValueAsString( base.edges(datasource) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }
*/
    // stream of JSON lines
    @GetMapping(value="/{datasource}/v", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_All(@PathVariable String datasource) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , base.vertices(datasource) );
    }
    @GetMapping(value="/{datasource}/e", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_All(@PathVariable String datasource) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , base.edges(datasource) );
    }

    ///////////////////////////////////////////////////////////////
    // APIs withDateRange

    @PostMapping(value="/v/ids/date"
            , consumes="application/json; charset=UTF-8"
            , produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_Ids_WithDateRange(
            @RequestBody Map<String,Object> param
    ) throws Exception {
        List<String> ids = (List<String>) param.get("q");
        String[] array = new String[ ids.size()];
        String fromDate = param.containsKey("from") ? param.get("from").toString() : null;
        String toDate = param.containsKey("to") ? param.get("to").toString() : null;
        // System.out.println(String.format("/search/v/ids => %s, %s, %s", ids, fromDate, toDate));
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , ids.size() == 0 ? Stream.empty() :
                        base.findVerticesWithDateRange(ids.toArray(array), fromDate, toDate) );
    }
    @PostMapping(value="/e/ids/date"
            , consumes="application/json; charset=UTF-8"
            , produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_Ids_WithDateRange(
            @RequestBody Map<String,Object> param
    ) throws Exception {
        List<String> ids = (List<String>) param.get("q");
        String[] array = new String[ids.size()];
        String fromDate = param.containsKey("from") ? param.get("from").toString() : null;
        String toDate = param.containsKey("to") ? param.get("to").toString() : null;
        // System.out.println(String.format("/search/e/ids => %s, %s, %s", ids, fromDate, toDate));
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , ids.size() == 0 ? Stream.empty() :
                        base.findEdgesWithDateRange(ids.toArray(array), fromDate, toDate) );
    }

    @GetMapping(value="/{datasource}/v/date", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_All_WithDateRange(
            @PathVariable String datasource,
            @RequestParam(value="from", required=false) String fromDate,
            @RequestParam(value="to", required=false) String toDate
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , base.findVerticesWithDateRange(datasource, fromDate, toDate) );
    }
    @GetMapping(value="/{datasource}/e/date", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_All_WithDateRange(
            @PathVariable String datasource,
            @RequestParam(value="from", required=false) String fromDate,
            @RequestParam(value="to", required=false) String toDate
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , base.findEdgesWithDateRange(datasource, fromDate, toDate) );
    }

    @GetMapping(value="/{datasource}/v/label/date", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_Label(
            @PathVariable String datasource,
            @RequestParam(value="q", required=true) String label,
            @RequestParam(value="from", required=false) String fromDate,
            @RequestParam(value="to", required=false) String toDate
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , label.isEmpty() ? Stream.empty() :
                        base.findVerticesWithDateRange(datasource, label, fromDate, toDate) );
    }
    @GetMapping(value="/{datasource}/e/labels/date", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_Label(
            @PathVariable String datasource,
            @RequestParam(value="q", required=true) String label,
            @RequestParam(value="from", required=false) String fromDate,
            @RequestParam(value="to", required=false) String toDate
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , label.isEmpty() ? Stream.empty() :
                        base.findEdgesWithDateRange(datasource, label, fromDate, toDate) );
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/api/search/modern/v/neighbors?q=modern_1"
    */
    @GetMapping(value="/{datasource}/v/neighbors", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> findV_Neighbors(
            @PathVariable String datasource,
            @RequestParam(value = "q") String vid
    ) throws Exception {
        return new ResponseEntity(base.findNeighborsOfVertex(datasource, vid)
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @PostMapping(value="/{datasource}/v/neighbors"
            , consumes="application/json; charset=UTF-8"
            , produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_Neighbors(
            @PathVariable String datasource,
            @RequestBody Map<String,List<String>> param
    ) throws Exception {
        String[] array = new String[param.get("q").size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , param.get("q").size() == 0 ? Stream.empty() :
                        base.findNeighborsOfVertices(datasource, param.get("q").toArray(array)) );
    }

/*
curl -X GET "localhost:8080/api/search/modern/e/connected?q=modern_2,modern_3,modern_4"
 */
    @GetMapping(value="/{datasource}/e/connected", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_Connected_Get(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> vids
    ) throws Exception {
        String[] array = new String[vids.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , vids.size() == 0 ? Stream.empty() :
                        base.findEdgesOfVertices(datasource, vids.toArray(array)) );
    }
/*
body: {
    "q": ["modern_1","modern_3","modern_6"]
}
 */
    @PostMapping(value="/{datasource}/e/connected"
            , consumes="application/json; charset=UTF-8"
            , produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_Connected_Post(
            @PathVariable String datasource,
            @RequestBody Map<String,List<String>> param
    ) throws Exception {
        String[] array = new String[param.get("q").size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , param.get("q").size() == 0 ? Stream.empty() :
                        base.findEdgesOfVertices(datasource, param.get("q").toArray(array)) );
    }

/*
body: {
	"q": ["modern_7","modern_8","modern_9"]
}
 */
    @PostMapping(value="/{datasource}/v/connected"
            , consumes="application/json; charset=UTF-8"
            , produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_Connected_Post(
            @PathVariable String datasource,
            @RequestBody Map<String,List<String>> param
    ) throws Exception {
        String[] array = new String[param.get("q").size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , param.get("q").size() == 0 ? Stream.empty() :
                        base.findVerticesOfEdges(datasource, param.get("q").toArray(array)) );
    }


    /*
curl -X GET "localhost:8080/api/search/sample/v/ids?q=modern_1,modern_2"
curl -X GET "localhost:8080/api/search/sample/e/ids?q=modern_7,modern_8"
    */
    @GetMapping(value="/{datasource}/v/ids", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_Ids(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> ids
    ) throws Exception {
        String[] array = new String[ids.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , ids.size() == 0 ? Stream.empty() :
                        base.findVerticesByIds(datasource, ids.toArray(array)) );
    }
    @PostMapping(value="/{datasource}/v/ids"
            , consumes="application/json; charset=UTF-8"
            , produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_Ids_Post(
            @PathVariable String datasource,
            @RequestBody Map<String,List<String>> param
    ) throws Exception {
        String[] array = new String[param.get("q").size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , param.get("q").size() == 0 ? Stream.empty() :
                        base.findVerticesByIds(datasource, param.get("q").toArray(array)) );
    }
    @GetMapping(value="/{datasource}/e/ids", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_Ids(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> ids
    ) throws Exception {
        String[] array = new String[ids.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , ids.size() == 0 ? Stream.empty() :
                        base.findEdgesByIds(datasource, ids.toArray(array)) );
    }
    @PostMapping(value="/{datasource}/e/ids"
            , consumes="application/json; charset=UTF-8"
            , produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_Ids_Post(
            @PathVariable String datasource,
            @RequestBody Map<String,List<String>> param
    ) throws Exception {
        String[] array = new String[param.get("q").size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , param.get("q").size() == 0 ? Stream.empty() :
                        base.findEdgesByIds(datasource, param.get("q").toArray(array)) );
    }


    /*
curl -X GET "localhost:8080/elastic/sample/v/label?q=person"
curl -X GET "localhost:8080/elastic/sample/e/label?q=person"
    */
    @GetMapping(value="/{datasource}/v/labels", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , labels.size() == 0 ? Stream.empty() :
                        base.findVertices(datasource, labels.toArray(array)) );
    }
    @GetMapping(value="/{datasource}/e/labels", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , labels.size() == 0 ? Stream.empty() :
                        base.findEdges(datasource, labels.toArray(array)) );
    }


    @GetMapping(value="/{datasource}/v/keys", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , keys.size() == 0 ? Stream.empty() :
                        base.findVerticesWithKeys(datasource, keys.toArray(array)) );
    }
    @GetMapping(value="/{datasource}/e/keys", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , keys.size() == 0 ? Stream.empty() :
                        base.findEdgesWithKeys(datasource, keys.toArray(array)) );
    }


    @GetMapping(value="/{datasource}/v/key", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key,
            @RequestParam(value = "hasNot", required=false, defaultValue="false") boolean hasNot
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , key.isEmpty() ? Stream.empty() :
                        base.findVertices(datasource, key, hasNot) );
    }
    @GetMapping(value="/{datasource}/e/key", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key,
            @RequestParam(value = "hasNot", required=false, defaultValue="false") boolean hasNot
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , key.isEmpty() ? Stream.empty() :
                        base.findEdges(datasource, key, hasNot) );
    }


    @GetMapping(value="/{datasource}/v/values", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , values.size() == 0 ? Stream.empty() :
                        base.findVerticesWithValues(datasource, values.toArray(array)) );
    }
    @GetMapping(value="/{datasource}/e/values", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , values.size() == 0 ? Stream.empty() :
                        base.findEdgesWithValues(datasource, values.toArray(array)) );
    }


    // http://localhost:8080/api/search/modern/v/value?q=ja
    @GetMapping(value = "/{datasource}/v/value", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_PropertyValuePartial(
            @PathVariable String datasource,
            @RequestParam(value = "q") String value
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , value.isEmpty() ? Stream.empty() :
                        base.findVerticesWithValue(datasource, value, true) );
    }
    // http://localhost:8080/api/search/modern/e/value?q=0.
    @GetMapping(value = "/{datasource}/e/value", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_PropertyValuePartial(
            @PathVariable String datasource,
            @RequestParam(value = "q") String value
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , value.isEmpty() ? Stream.empty() :
                        base.findEdgesWithValue(datasource, value, true) );
    }

    ////////////////////////////////////////////////

    // http://localhost:8080/api/search/modern/v/keyvalue?key=name&value=java
    @GetMapping(value="/{datasource}/v/keyvalue", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , key.isEmpty() || value.isEmpty() ? Stream.empty() :
                        base.findVertices(datasource, key, value) );
    }
    // http://localhost:8080/api/search/modern/e/keyvalue?key=weight&value=0.5
    @GetMapping(value="/{datasource}/e/keyvalue", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , key.isEmpty() || value.isEmpty() ? Stream.empty() :
                        base.findEdges(datasource, key, value) );
    }


    @GetMapping(value="/{datasource}/v/labelkeyvalue", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , label.isEmpty() || key.isEmpty() || value.isEmpty() ? Stream.empty() :
                        base.findVertices(datasource, label, key, value) );
    }
    @GetMapping(value="/{datasource}/e/labelkeyvalue", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , label.isEmpty() || key.isEmpty() || value.isEmpty() ? Stream.empty() :
                        base.findEdges(datasource, label, key, value) );
    }


    // http://localhost:8080/api/search/modern/v/hasContainers?label=person&keyNot=gender&key=country&values=USA
    // http://localhost:8080/api/search/modern/v/hasContainers?label=person&kvPairs=country@USA,name@marko
    @GetMapping(value="/{datasource}/v/hasContainers", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findV_hasContainers(
            @PathVariable String datasource,
            @RequestParam(value = "label", required = false) String label,
            @RequestParam(value = "labels", required = false) List<String> labelParams,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "keyNot", required = false) String keyNot,
            @RequestParam(value = "keys", required = false) List<String> keyParams,
            @RequestParam(value = "values", required = false) List<String> valueParams,
            @RequestParam(value = "kvPairs", required = false) List<String> kvParams
    ) throws Exception {
        // Parameters
        String[] labels = labelParams==null ? null : labelParams.stream().toArray(String[]::new);
        String[] keys = keyParams==null ? null : keyParams.stream().toArray(String[]::new);
        String[] values = valueParams==null ? null : valueParams.stream().toArray(String[]::new);

        Map<String,String> kvPairs = null;
        if( kvParams != null && kvParams.size() > 0 ){
            final String delimter = "@";
            kvPairs = kvParams.stream()
                    .map(r->r.split(delimter,2)).filter(r->r.length==2)
                    .collect(Collectors.toMap(r->r[0],r->r[1]));
        }

        // for DEBUG
        System.out.println("V.hasContainers :: datasource => "+datasource);
        System.out.println("  , label => "+label);
        System.out.println("  , labels => "+(labels==null ? "null" : String.join(",", labels)));
        System.out.println("  , key => "+key);
        System.out.println("  , keyNot => "+keyNot);
        System.out.println("  , keys => "+(keys==null ? "null" : String.join(",", keys)));
        System.out.println("  , values => "+(values==null ? "null" : String.join(",", values)));
        System.out.println("  , kvPairs => "+(kvPairs==null ? "null" : kvPairs.entrySet().stream().map(r->r.getKey()+"="+r.getValue()).collect(Collectors.joining(","))));

        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , base.findVertices(datasource, label, labels, key, keyNot, keys, values, kvPairs) );
    }
    @GetMapping(value = "/{datasource}/e/hasContainers", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> findE_hasContainers(
            @PathVariable String datasource,
            @RequestParam(value = "label", required = false) String label,
            @RequestParam(value = "labels", required = false) List<String> labelParams,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "keyNot", required = false) String keyNot,
            @RequestParam(value = "keys", required = false) List<String> keyParams,
            @RequestParam(value = "values", required = false) List<String> valueParams,
            @RequestParam(value = "kvPairs", required = false) List<String> kvParams
    ) throws Exception {
        // Parameters
        String[] labels = labelParams==null ? null : labelParams.stream().toArray(String[]::new);
        String[] keys = keyParams==null ? null : keyParams.stream().toArray(String[]::new);
        String[] values = valueParams==null ? null : valueParams.stream().toArray(String[]::new);

        Map<String,String> kvPairs = null;
        if( kvParams != null && kvParams.size() > 0 ){
            final String delimter = "@";
            kvPairs = kvParams.stream()
                    .map(r->r.split(delimter)).filter(r->r.length==2)
                    .collect(Collectors.toMap(r->r[0],r->r[1]));
        }

        // for DEBUG
        System.out.println("E.hasContainers :: datasource => "+datasource);
        System.out.println("  , label => "+label);
        System.out.println("  , labels => "+(labels==null ? "null" : String.join(",", labels)));
        System.out.println("  , key => "+key);
        System.out.println("  , keyNot => "+keyNot);
        System.out.println("  , keys => "+(keys==null ? "null" : String.join(",", keys)));
        System.out.println("  , values => "+(values==null ? "null" : String.join(",", values)));
        System.out.println("  , kvPairs => "+(kvPairs==null ? "null" : kvPairs.entrySet().stream().map(r->r.getKey()+"="+r.getValue()).collect(Collectors.joining(","))));

        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , base.findEdges(datasource, label, labels, key, keyNot, keys, values, kvPairs) );
    }

}
