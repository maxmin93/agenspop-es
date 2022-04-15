package net.bitnine.agenspop.elasticgraph.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.elasticgraph.model.ElasticEdge;
import net.bitnine.agenspop.elasticgraph.model.ElasticVertex;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class ElasticVertexService extends ElasticElementService {

    private final String INDEX;
    private final long SCROLL_LIMIT;

    public ElasticVertexService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper,            // spring boot web starter
            String index,
            long limit
    ) {
        super(client, limit, mapper);
        this.INDEX = index;
        this.SCROLL_LIMIT = limit;
    }

    ///////////////////////////////////////////////////////////////

    public long count() throws Exception {
        return super.count(INDEX);
    }

    public long count(String datasource) throws Exception {
        return super.count(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

    public String createDocument(ElasticVertex document) throws Exception {
        return super.createDocument(INDEX, ElasticVertex.class, document);
    }

    public String updateDocument(ElasticVertex document) throws Exception {
        return super.updateDocument(INDEX, ElasticVertex.class, document);
    }

    public String deleteDocument(String id) throws Exception {
        return super.deleteDocument(INDEX, id);
    }
    public long deleteDocuments(String datasource) throws Exception {
        return super.deleteDocuments(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

//    public List<ElasticVertex> findAll() throws Exception {
//        return super.findAll(INDEX, ElasticVertex.class);
//    }

    public ElasticVertex findById(String id) throws Exception {
        return super.findById(INDEX, ElasticVertex.class, id);
    }

    public boolean existsId(String id) throws Exception {
        return super.existsId(INDEX, id);
    }

    public HashSet<String> idsByDatasource(String datasource){
        return new HashSet<String>( super.idsByDatasource(INDEX, ElasticVertex.class, datasource) );
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticVertex> findByIds(String[] ids) throws Exception {
        return super.findByIds(INDEX, ElasticVertex.class, ids);
    }

    public List<ElasticVertex> findByLabel(int size, String label) throws Exception {
        return super.findByLabel(INDEX, ElasticVertex.class, size, label);
    }

    public List<ElasticVertex> findByDatasource(int size, String datasource) throws Exception {
        return super.findByDatasource(INDEX, ElasticVertex.class, size, datasource);
    }

    public List<ElasticVertex> findByDatasourceAndLabel(int size, String datasource, String label) throws Exception {
        return super.findByDatasourceAndLabel(INDEX, ElasticVertex.class, size, datasource, label);
    }
    public List<ElasticVertex> findByDatasourceAndLabels(int size, String datasource, final String[] labels) throws Exception {
        return super.findByDatasourceAndLabels(INDEX, ElasticVertex.class, size, datasource, labels);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyKeys(int size, String datasource, final String[] keys) throws Exception{
        return super.findByDatasourceAndPropertyKeys(INDEX, ElasticVertex.class, size, datasource, keys);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyKey(int size, String datasource, String key) throws Exception{
        return super.findByDatasourceAndPropertyKey(INDEX, ElasticVertex.class, size, datasource, key);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyKeyNot(int size, String datasource, String keyNot) throws Exception{
        return super.findByDatasourceAndPropertyKeyNot(INDEX, ElasticVertex.class, size, datasource, keyNot);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyValues(int size, String datasource, final String[] values) throws Exception{
        return super.findByDatasourceAndPropertyValues(INDEX, ElasticVertex.class, size, datasource, values);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyValue(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValue(INDEX, ElasticVertex.class, size, datasource, value);
    }
    public List<ElasticVertex> findByDatasourceAndPropertyValuePartial(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValuePartial(INDEX, ElasticVertex.class, size, datasource, value);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception{
        return super.findByDatasourceAndPropertyKeyValue(INDEX, ElasticVertex.class, size, datasource, key, value);
    }

    public List<ElasticVertex> findByDatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception{
        return super.findByDatasourceAndLabelAndPropertyKeyValue(INDEX, ElasticVertex.class, size, datasource, label, key, value);
    }
    public List<ElasticVertex> findByDatasourceAndLabelAndPropertyKeyValues(int size, String datasource, String label, Map<String,String> kvPairs) throws Exception{
        return super.findByDatasourceAndLabelAndPropertyKeyValues(INDEX, ElasticVertex.class, size, datasource, label, kvPairs);
    }

    public List<ElasticVertex> findByHasContainers(int size, String datasource
            , String label, final String[] labels
            , String key, String keyNot, final String[] keys
            , final String[] values, final Map<String,String> kvPairs) throws Exception {
        return super.findByHasContainers(INDEX, ElasticVertex.class, size, datasource
                , label, labels, key, keyNot, keys, values, kvPairs);
    }

    ///////////////////////////////////////////////////////////////
    // APIs : withDateRange

    public Stream<ElasticVertex> streamByIdsWithDateRange(String[] ids, String fromDate, String toDate) throws Exception {
        return super.streamByIdsWithDateRange(INDEX, ElasticVertex.class, ids, fromDate, toDate);
    }
    public Stream<ElasticVertex> streamByDatasourceWithDateRange(String datasource, String fromDate, String toDate) throws Exception {
        return super.streamByDatasourceWithDateRange(INDEX, ElasticVertex.class, datasource, fromDate, toDate);
    }
    public Stream<ElasticVertex> streamByDatasourceAndLabelWithDateRange(String datasource, String label, String fromDate, String toDate) throws Exception {
        return super.streamByDatasourceAndLabelWithDateRange(INDEX, ElasticVertex.class, datasource, label, fromDate, toDate);
    }

    ///////////////////////////////////////////////////////////////
    //
    // Stream APIs
    //
    ///////////////////////////////////////////////////////////////

    public Stream<ElasticVertex> streamByIds(String[] ids) throws Exception {
        return super.streamByIds(INDEX, ElasticVertex.class
                , ids);
    }

    public Stream<ElasticVertex> streamByDatasource(String datasource) throws Exception {
        return super.streamByDatasource(INDEX, ElasticVertex.class
                , datasource);
    }
    public Stream<ElasticVertex> streamByDatasourceAndIds(String datasource, String[] ids) throws Exception {
        return super.streamByDatasourceAndIds(INDEX, ElasticVertex.class
                , datasource, ids);
    }

    public Stream<ElasticVertex> streamByDatasourceAndLabel(String datasource, String label) throws Exception {
        return super.streamByDatasourceAndLabel(INDEX, ElasticVertex.class
                , datasource, label);
    }
    public Stream<ElasticVertex> streamByDatasourceAndLabels(String datasource, final String[] labels) throws Exception {
        return super.streamByDatasourceAndLabels(INDEX, ElasticVertex.class
                , datasource, labels);
    }

    public Stream<ElasticVertex> streamByDatasourceAndPropertyKeys(String datasource, final String[] keys) throws Exception{
        return super.streamByDatasourceAndPropertyKeys(INDEX, ElasticVertex.class
                , datasource, keys);
    }

    public Stream<ElasticVertex> streamByDatasourceAndPropertyKey(String datasource, String key) throws Exception{
        return super.streamByDatasourceAndPropertyKey(INDEX, ElasticVertex.class
                , datasource, key);
    }
    public Stream<ElasticVertex> streamByDatasourceAndPropertyKeyNot(String datasource, String keyNot) throws Exception{
        return super.streamByDatasourceAndPropertyKeyNot(INDEX, ElasticVertex.class
                , datasource, keyNot);
    }

    public Stream<ElasticVertex> streamByDatasourceAndPropertyValues(String datasource, final String[] values) throws Exception{
        return super.streamByDatasourceAndPropertyValues(INDEX, ElasticVertex.class
                , datasource, values);
    }

    public Stream<ElasticVertex> streamByDatasourceAndPropertyValue(String datasource, String value) throws Exception{
        return super.streamByDatasourceAndPropertyValue(INDEX, ElasticVertex.class
                , datasource, value);
    }
    public Stream<ElasticVertex> streamByDatasourceAndPropertyValuePartial(String datasource, String value) throws Exception{
        return super.streamByDatasourceAndPropertyValuePartial(INDEX, ElasticVertex.class
                , datasource, value);
    }

    public Stream<ElasticVertex> streamByDatasourceAndPropertyKeyValue(String datasource, String key, String value) throws Exception{
        return super.streamByDatasourceAndPropertyKeyValue(INDEX, ElasticVertex.class
                , datasource, key, value);
    }

    public Stream<ElasticVertex> streamByDatasourceAndLabelAndPropertyKeyValue(String datasource, String label, String key, String value) throws Exception{
        return super.streamByDatasourceAndLabelAndPropertyKeyValue(INDEX, ElasticVertex.class
                , datasource, label, key, value);
    }
    public Stream<ElasticVertex> streamByDatasourceAndLabelAndPropertyKeyValues(String datasource, String label, Map<String,String> kvPairs) throws Exception{
        return super.streamByDatasourceAndLabelAndPropertyKeyValues(INDEX, ElasticVertex.class
                , datasource, label, kvPairs);
    }

    public Stream<ElasticVertex> streamByHasContainers(String datasource
            , String label, final String[] labels
            , String key, String keyNot, final String[] keys
            , final String[] values, final Map<String,String> kvPairs) throws Exception {
        return super.streamByHasContainers(INDEX, ElasticVertex.class
                , datasource, label, labels, key, keyNot, keys, values, kvPairs);
    }
}
