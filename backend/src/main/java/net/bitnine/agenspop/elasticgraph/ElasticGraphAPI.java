package net.bitnine.agenspop.elasticgraph;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.basegraph.BaseGraphAPI;
import net.bitnine.agenspop.basegraph.BaseTx;
import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.config.properties.ElasticProperties;
import net.bitnine.agenspop.elasticgraph.model.ElasticEdge;
import net.bitnine.agenspop.elasticgraph.model.ElasticElement;
import net.bitnine.agenspop.elasticgraph.model.ElasticProperty;
import net.bitnine.agenspop.elasticgraph.model.ElasticVertex;
import net.bitnine.agenspop.elasticgraph.repository.ElasticEdgeService;
import net.bitnine.agenspop.elasticgraph.repository.ElasticGraphService;
import net.bitnine.agenspop.elasticgraph.repository.ElasticVertexService;
import net.bitnine.agenspop.elasticgraph.util.ElasticHelper;
import net.bitnine.agenspop.elasticgraph.util.ElasticSample;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
public class ElasticGraphAPI implements BaseGraphAPI {

    private final static String ID_DELIMITER = "_";
    private final long SCROLL_LIMIT;

    private final RestHighLevelClient client;
    private final ObjectMapper mapper;

    private final ElasticProperties config;
    private final ElasticVertexService vertices;
    private final ElasticEdgeService edges;
    private final ElasticGraphService graph;

    @Autowired
    public ElasticGraphAPI(
            ElasticProperties elasticProperties,
            ResourceLoader resourceLoader,  // for accessing classpath in fat jar
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        this.SCROLL_LIMIT = elasticProperties.getScrollLimit();
        this.client = client;
        this.mapper = mapper;

        this.config = elasticProperties;
        this.vertices = new ElasticVertexService(client, mapper
                , elasticProperties.getVertexIndex(), SCROLL_LIMIT);
        this.edges = new ElasticEdgeService(client, mapper
                , elasticProperties.getEdgeIndex(), SCROLL_LIMIT);
        this.graph = new ElasticGraphService(client, mapper
                , elasticProperties.getVertexIndex(), elasticProperties.getEdgeIndex()
                , elasticProperties.getIndexShards(), elasticProperties.getIndexReplicas()
                , resourceLoader);
    }

    @Override
    public BaseTx tx(){
        return new BaseTx() {
            @Override public void failure() {
            }
            @Override public void success() {
            }
            @Override public void close() {
            }
        };
    }

    @PostConstruct
    private void ready() throws Exception {
        graph.ready();      // if not exists index, create index

        // check sample graph-data
        String datasource = "modern";
        if( vertices.count(datasource) < 6 ){
            for( ElasticVertex v : ElasticSample.modernVertices()) saveVertex(v);
        }
        if( edges.count(datasource) < 6 ){
            for( ElasticEdge e : ElasticSample.modernEdges()) saveEdge(e);
        }
    }

    public boolean reset() throws Exception {
        boolean state = graph.resetIndex();
        // insert sample data
        for( ElasticVertex v : ElasticSample.modernVertices()) saveVertex(v);
        for( ElasticEdge e : ElasticSample.modernEdges()) saveEdge(e);

        return state;
    }

    public String remove(String datasource) throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.deleteDocuments(datasource));
        object.addProperty("E", edges.deleteDocuments(datasource));
        return gson.toJson(object);
    }

    public String count() throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.count());
        object.addProperty("E", edges.count());
        return gson.toJson(object);
    }
    public String count(String datasource) throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.count(datasource));
        object.addProperty("E", edges.count(datasource));
        return gson.toJson(object);
    }

    public String labels(String datasource) throws Exception {
        Gson gson = new Gson();
        JsonElement jsonV = gson.fromJson( gson.toJson(listVertexLabels(datasource)), JsonElement.class);
        JsonElement jsonE = gson.fromJson( gson.toJson(listEdgeLabels(datasource)), JsonElement.class);

        JsonObject object = new JsonObject();
        object.add("V", jsonV);
        object.add("E", jsonE);
        return gson.toJson(object);
    }

    //////////////////////////////////////////////////
    //
    // schema services
    //

    @Override
    public List<String> searchDatasources(String query) {
        try {
            return graph.searchDatasources(config.getVertexIndex(), query);
        }
        catch (Exception e) { return Collections.EMPTY_LIST; }
    }
    @Override
    public Map<String, Long> searchVertexDatasources(List<String> dsList) {
        try {
            if( dsList.isEmpty() )
                return graph.listDatasources(config.getVertexIndex(), null);
            else
                return graph.listDatasources(config.getVertexIndex(), dsList);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }
    @Override
    public Map<String, Long> searchEdgeDatasources(List<String> dsList){
        try {
            if( dsList.isEmpty() )
                return graph.listDatasources(config.getEdgeIndex(), null);
            else
                return graph.listDatasources(config.getEdgeIndex(), dsList);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }

    @Override
    public Map<String, Long> listVertexDatasources() {
        try {
            return graph.listDatasources(config.getVertexIndex(), Collections.EMPTY_LIST);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }
    @Override
    public Map<String, Long> listEdgeDatasources(){
        try {
            return graph.listDatasources(config.getEdgeIndex(), Collections.EMPTY_LIST);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }


    @Override
    public Map<String, Long> listVertexLabels(String datasource) {
        try {
            return graph.listLabels(config.getVertexIndex(), datasource);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }
    @Override
    public Map<String, Long> listEdgeLabels(String datasource){
        try {
            return graph.listLabels(config.getEdgeIndex(), datasource);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }


    @Override
    public Map<String, Long> listVertexLabelKeys(String datasource, String label) {
        try {
            return graph.listLabelKeys(config.getVertexIndex(), datasource, label);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }
    @Override
    public Map<String, Long> listEdgeLabelKeys(String datasource, String label){
        try {
            return graph.listLabelKeys(config.getEdgeIndex(), datasource, label);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }


    @Override
    public long countV(String datasource){
        try{ return vertices.count(datasource); }
        catch(Exception e){ return -1L; }
    }
    @Override
    public long countE(String datasource){
        try{ return edges.count(datasource); }
        catch(Exception e){ return -1L; }
    }


    public long countV() {
        try{ return vertices.count(); }
        catch(Exception e){ return -1L; }
    }
    public long countE() {
        try{ return edges.count(); }
        catch(Exception e){ return -1L; }
    }


    //////////////////////////////////////////////////
    //
    // common access services about ElasticElement
    //

    @Override
    public Stream<BaseVertex> verticesByIds(String[] ids){
        try{
            return vertices.streamByIds(ids).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> edgesByIds(String[] ids){
        try{
            return edges.streamByIds(ids).map(r->(BaseEdge)r);
        } catch(Exception e){ return Stream.empty(); }
    }


    @Override
    public Stream<BaseVertex> vertices(String datasource){
        try{
            return vertices.streamByDatasource(datasource).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> edges(String datasource){
        try {
            Stream<BaseEdge> stream = edges.streamByDatasource(datasource).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }


    @Override
    public boolean existsVertex(String id){
        try{ return vertices.existsId(id); }
        catch(Exception e){ return false; }
    }
    @Override
    public boolean existsEdge(String id){
        try{ return edges.existsId(id); }
        catch(Exception e){ return false; }
    }


    @Override
    public Optional<BaseVertex> getVertexById(String id){
        try{ return Optional.of(vertices.findById(id)); }
        catch(Exception e){ return Optional.empty(); }
    }
    @Override
    public Optional<BaseEdge> getEdgeById(String id){
        try{
            ElasticEdge e = edges.findById(id);
            return ( e != null && vertices.existsId(e.getSrc()) && vertices.existsId(e.getDst()) ) ?
                Optional.of(e) : Optional.empty();
        }
        catch(Exception e){ return Optional.empty(); }
    }


    @Override
    public BaseVertex createVertex(String datasource, String id, String label, String name){
        return new ElasticVertex(datasource, id, label, name);
    }
    @Override
    public BaseVertex createVertex(String datasource, String id, String label){
        return new ElasticVertex(datasource, id, label, id);
    }

    @Override
    public BaseEdge createEdge(String datasource, String id, String label, String src, String dst){
        return new ElasticEdge(datasource, id, label, src, dst);
    }
    @Override
    public BaseProperty createProperty(String key, Object value){
        if( key.equals(BaseElement.timestampTag)){
            if( !ElasticHelper.checkDateformat(value.toString()) )
                throw new IllegalArgumentException("Wrong date format: 'yyyy-MM-dd HH:mm:ss'");
        }
        return new ElasticProperty(key, value);
    }


    @Override
    public boolean saveVertex(BaseVertex vertex){
        ElasticHelper.setCreatedDate((ElasticElement)vertex);
        vertex.removeProperty(BaseElement.timestampTag);    // not need to save as property
        System.out.println("saveVertex) "+vertex.getId()+" = "+vertex.keys());
        try{
            if( existsVertex(vertex.getId()) )
                return vertices.updateDocument((ElasticVertex) vertex).equals("UPDATED") ? true : false;
            else
                return vertices.createDocument((ElasticVertex) vertex).equals("CREATED") ? true : false;
        }
        catch(Exception e){ return false; }
    }
    @Override
    public boolean saveEdge(BaseEdge edge){
        ElasticHelper.setCreatedDate((ElasticElement)edge);
        edge.removeProperty(BaseElement.timestampTag);      // not need to save as property
        System.out.println("saveEdge) "+edge.getId()+" = "+edge.keys());
        try{
            if( existsEdge(edge.getId()) )
                return edges.updateDocument((ElasticEdge) edge).equals("UPDATED") ? true : false;
            else
                return edges.createDocument((ElasticEdge) edge).equals("CREATED") ? true : false;
        }
        catch(Exception e){ return false; }
    }


    @Override
    public void dropVertex(BaseVertex vertex){
        vertex.remove();
        dropVertex(vertex.getId());
    }
    @Override
    public void dropVertex(String id){
        try{ vertices.deleteDocument(id); }
        catch (Exception e){ }
    }
    @Override
    public void dropEdge(BaseEdge edge){
        edge.remove();
        dropEdge(edge.getId());
    }
    @Override
    public void dropEdge(String id){
        try{ edges.deleteDocument(id); }
        catch (Exception e){ }
    }

    ///////////////////////////////////////////////////////////////


    ///////////////////////////////////////////////////////////////
    //  **NOTE: optimized find functions
    //      ==> http://tinkerpop.apache.org/docs/current/reference/#has-step
    //    has(key,value)          AND : findVerticesWithKV(ds, key, val)
    //    has(label, key, value)  AND : findVerticesWithLKV(ds, label, key, value)
    //    hasLabel(labels…​)       OR  : findVerticesWithLabels(ds, ...labels)
    //    hasId(ids…​)             OR  : findVertices(ids)
    //    hasKey(keys…​)           AND : findVerticesWithKeys(ds, ...keys)
    //    hasValue(values…​)       AND : findVerticesWithValues(ds, ...values)
    //    has(key)                EQ  : findVerticesWithKey(ds, key)
    //    hasNot(key)             NEQ : findVerticesWithNotKey(ds, key)

    ///////////////////////////////////////////////////////////////
    //
    // find edges for baseAPI
    //
    ///////////////////////////////////////////////////////////////

    @Override
    public Stream<BaseVertex> findVerticesWithDateRange(final String[] ids, String fromDate, String toDate){
        try{
            return vertices.streamByIdsWithDateRange(ids, fromDate, toDate).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVerticesWithDateRange(String datasource, String fromDate, String toDate){
        try{
            return vertices.streamByDatasourceWithDateRange(datasource, fromDate, toDate).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVerticesWithDateRange(String datasource, String label, String fromDate, String toDate){
        try{
            return vertices.streamByDatasourceAndLabelWithDateRange(datasource, label, fromDate, toDate).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }

    ///////////////////////////////////////////////////////////////
    //
    // find vertices for baseAPI
    //
    ///////////////////////////////////////////////////////////////

    @Override
    public Stream<BaseVertex> findVertices(final String[] ids){
        try{
            return vertices.streamByIds(ids).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }

    public Stream<BaseVertex> findVerticesByIds(String datasource, final String[] ids){
        try{
            return vertices.streamByDatasourceAndIds(datasource, ids).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVertices(String datasource, String label){
        try{
            return vertices.streamByDatasourceAndLabel(datasource, label).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVertices(String datasource, final String[] labels){
        try{
            return vertices.streamByDatasourceAndLabels(datasource, labels).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVertices(String datasource, String key, String value){
        try{
            return vertices.streamByDatasourceAndPropertyKeyValue(datasource, key, value).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVertices(String datasource, String label, String key, String value){
        try{
            return vertices.streamByDatasourceAndLabelAndPropertyKeyValue(datasource, label, key, value).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVertices(String datasource, String key, boolean hasNot){
        try{
            Stream<ElasticVertex> stream = !hasNot
                    ? vertices.streamByDatasourceAndPropertyKey(datasource, key)
                    : vertices.streamByDatasourceAndPropertyKeyNot(datasource, key);
            return stream.map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVerticesWithKeys(String datasource, final String[] keys){
        try{
            return vertices.streamByDatasourceAndPropertyKeys(datasource, keys).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVerticesWithValue(String datasource, String value, boolean isPartial){
        try{
            Stream<ElasticVertex> stream = !isPartial
                    ? vertices.streamByDatasourceAndPropertyValue(datasource, value)
                    : vertices.streamByDatasourceAndPropertyValuePartial(datasource, value);
            return stream.map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVerticesWithValues(String datasource, final String[] values){
        try{
            return vertices.streamByDatasourceAndPropertyValues(datasource, values).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseVertex> findVerticesWithKeyValues(String datasource, String label, final Map<String, String> kvPairs){
        try{
            return vertices.streamByDatasourceAndLabelAndPropertyKeyValues(datasource, label, kvPairs).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }

    // V : hasContainers
    @Override
    public Stream<BaseVertex> findVertices(String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs){
        try{
            return vertices.streamByHasContainers(datasource, label, labels, key, keyNot, keys, values, kvPairs).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }

    @Override
    public BaseVertex findOtherVertexOfEdge(String eid, String vid){
        try{
            ElasticEdge edge = edges.findById(eid);
            if( edge == null ) return null;
            String otherVid = vid.equals(edge.getSrc()) ? edge.getDst() : edge.getSrc();
            return vertices.findById(otherVid);
        } catch(Exception e){ return null; }
    }

    @Override
    public Stream<BaseVertex> findNeighborVertices(String datasource, String vid, Direction direction, String[] labels){
        try{
            Stream<ElasticEdge> links = edges.streamByDatasourceAndDirection(datasource, vid, direction);
            Set<String> neighborIds = links.map(r->r.getSrc().equals(vid) ? r.getDst() : r.getSrc())
                    .collect(Collectors.toSet());

            String[] arrayIds = new String[neighborIds.size()];
            if( labels.length > 0 ){
                List<String> filterLabels = Arrays.asList(labels);
                return vertices.streamByIds(neighborIds.toArray(arrayIds))
                        .filter(r->filterLabels.contains(r.getLabel()))
                        .map(r->(BaseVertex)r);
            }
            else
                return vertices.streamByIds(neighborIds.toArray(arrayIds))
                        .map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }

    public Map<String,Map<String,List<String>>> findNeighborsOfVertex(String datasource, String vid){
        try{
            // 1) outgoers
            Stream<ElasticEdge> outlinks = edges.streamByDatasourceAndDirection(datasource, vid, Direction.OUT);
            Set<String> outIds = outlinks.map(r->r.getSrc().equals(vid) ? r.getDst() : r.getSrc())
                     .collect(Collectors.toSet());
            String[] arrayOutIds = new String[outIds.size()];
            Map<String,List<String>> outgoers = vertices.streamByIds(outIds.toArray(arrayOutIds))
                    .collect(Collectors.groupingBy(ElasticVertex::getLabel,
                            Collectors.mapping(ElasticVertex::getId, Collectors.toList()) ));
            // 2) incommers
            Stream<ElasticEdge> inlinks = edges.streamByDatasourceAndDirection(datasource, vid, Direction.IN);
            Set<String> inIds = inlinks.map(r->r.getSrc().equals(vid) ? r.getDst() : r.getSrc())
                    .collect(Collectors.toSet());
            String[] arrayInIds = new String[inIds.size()];
            Map<String,List<String>> incomers = vertices.streamByIds(inIds.toArray(arrayInIds))
                    .collect(Collectors.groupingBy(ElasticVertex::getLabel,
                            Collectors.mapping(ElasticVertex::getId, Collectors.toList()) ));
            // 3) make result to Map
            Map<String,Map<String,List<String>>> neighbors = new HashMap<>();
            neighbors.put("outgoers", outgoers);
            neighbors.put("incomers", incomers);
            return neighbors;
        } catch(Exception e){
            // System.out.println("  ==> ERROR : "+e.getMessage());
            return Collections.emptyMap();
        }
    }

    public Stream<BaseVertex> findNeighborsOfVertices(String datasource, String[] vids){
        Set<String> vidSet = new HashSet<>(Arrays.asList(vids));
        Set<String> neighborIds = new HashSet<>();
        try{
            for( String vid : vids ) {
                Stream<ElasticEdge> links = edges.streamByDatasourceAndDirection(datasource, vid, Direction.BOTH);
                neighborIds.addAll( links.map(r -> r.getSrc().equals(vid) ? r.getDst() : r.getSrc())
                        .collect(Collectors.toSet()) );
            }
            // get difference set of vids from neighbors
            neighborIds.removeAll(vidSet);
            // return neighbor vertices
            String[] arrayIds = new String[neighborIds.size()];
            return vertices.streamByIds(neighborIds.toArray(arrayIds))
                    .map(r->(BaseVertex)r);
        } catch(Exception e){
            // System.out.println("  ==> ERROR : "+e.getMessage());
            return Stream.empty();
        }
    }

    ///////////////////////////////////////////////////////////////
    //
    // find edges for baseAPI
    //
    ///////////////////////////////////////////////////////////////

    @Override
    public Stream<BaseEdge> findEdgesWithDateRange(final String[] ids, String fromDate, String toDate){
        try{
            return edges.streamByIdsWithDateRange(ids, fromDate, toDate).map(r->(BaseEdge)r);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdgesWithDateRange(String datasource, String fromDate, String toDate){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceWithDateRange(datasource, fromDate, toDate).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdgesWithDateRange(String datasource, String label, String fromDate, String toDate){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndLabelWithDateRange(datasource, label, fromDate, toDate).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }

    ///////////////////////////////////////////////////////////////

    @Override
    public Stream<BaseEdge> findEdges(final String[] ids){
        try{
            return edges.streamByIds(ids).map(r->(BaseEdge)r);
        } catch(Exception e){ return Stream.empty(); }
    }

    public Stream<BaseEdge> findEdgesByIds(String datasource, final String[] ids){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndIds(datasource, ids).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdges(String datasource, String label){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndLabel(datasource, label).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdges(String datasource, final String[] labels){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndLabels(datasource, labels).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdges(String datasource, String key, String value){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndPropertyKeyValue(datasource, key, value).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdges(String datasource, String label, String key, String value){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndLabelAndPropertyKeyValue(datasource, label, key, value).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdges(String datasource, String key, boolean hasNot){
        try{
            Stream<BaseEdge> stream = !hasNot
                    ? edges.streamByDatasourceAndPropertyKey(datasource, key).map(r->(BaseEdge)r)
                    : edges.streamByDatasourceAndPropertyKeyNot(datasource, key).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdgesWithKeys(String datasource, final String[] keys){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndPropertyKeys(datasource, keys).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdgesWithValue(String datasource, String value, boolean isPartial){
        try{
            Stream<BaseEdge> stream = !isPartial
                    ? edges.streamByDatasourceAndPropertyValue(datasource, value).map(r->(BaseEdge)r)
                    : edges.streamByDatasourceAndPropertyValuePartial(datasource, value).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdgesWithValues(String datasource, final String[] values){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndPropertyValues(datasource, values).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }
    @Override
    public Stream<BaseEdge> findEdgesWithKeyValues(String datasource, String label, final Map<String,String> kvPairs){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndLabelAndPropertyKeyValues(datasource, label, kvPairs).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }

    // V : hasContainers
    @Override
    public Stream<BaseEdge> findEdges(String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs){
        try{
            Stream<BaseEdge> stream = edges.streamByHasContainers(datasource, label, labels, key, keyNot, keys, values, kvPairs).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }

    @Override
    public Stream<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction){
        try{
            Stream<BaseEdge> stream = edges.streamByDatasourceAndDirection(datasource, vid, direction).map(r->(BaseEdge)r);
            return !config.isEdgeValidation() ? stream :
                    ElasticHelper.filterValidEdges(vertices, datasource, stream);
        } catch(Exception e){ return Stream.empty(); }
    }

    @Override
    public Stream<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, final String[] labels){
        if( labels.length > 0 ){
            List<String> filterLabels = Arrays.asList(labels);
            return findEdgesOfVertex(datasource, vid, direction)
                    .filter(r->filterLabels.contains(r.getLabel()));
        }
        return findEdgesOfVertex(datasource, vid, direction);
    }

    @Override
    public Stream<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, String label, String key, Object value){
        return findEdgesOfVertex(datasource, vid, direction)
                .filter(r->{
                    if( label != null && !label.equals(r.getLabel()) ) return false;
                    if( key != null ){
                        if( !r.keys().contains(key) ) return false;
                        if( value != null && !r.getProperty(key).value().equals(value) ) return false;
                    }
                    return true;
                });
    }

    public Stream<BaseEdge> findEdgesOfVertices(String datasource, final String[] vids){
        try{
            return edges.streamByDatasourceWithVertices(datasource, vids).map(r->(BaseEdge)r);
        } catch(Exception e){ return Stream.empty(); }
    }

    // 1) src, dst 의 id SET 만들고
    // 2) vertices 로부터 id 매칭 리스트 반환
    public Stream<BaseVertex> findVerticesOfEdges(String datasource, final String[] eids){
        try{
            Set<String> vids = ConcurrentHashMap.newKeySet();
            edges.streamByDatasourceAndIds(datasource, eids).forEach(r->{
                vids.add(r.getSrc());
                vids.add(r.getDst());
            });
            String[] arrayIds = new String[vids.size()];
            return vertices.streamByDatasourceAndIds(datasource, vids.toArray(arrayIds)).map(r->(BaseVertex)r);
        } catch(Exception e){ return Stream.empty(); }
    }

}
