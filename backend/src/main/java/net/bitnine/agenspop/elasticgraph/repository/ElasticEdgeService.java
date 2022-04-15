package net.bitnine.agenspop.elasticgraph.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.elasticgraph.model.ElasticEdge;
import net.bitnine.agenspop.elasticgraph.util.ElasticScrollIterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@Slf4j
public final class ElasticEdgeService extends ElasticElementService {

    private final long SCROLL_LIMIT;
    private final String INDEX;

    public ElasticEdgeService(
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

    public String createDocument(ElasticEdge document) throws Exception {
        return super.createDocument(INDEX, ElasticEdge.class, document);
    }

    public String updateDocument(ElasticEdge document) throws Exception {
        return super.updateDocument(INDEX, ElasticEdge.class, document);
    }

    public String deleteDocument(String id) throws Exception {
        return super.deleteDocument(INDEX, id);
    }
    public long deleteDocuments(String datasource) throws Exception {
        return super.deleteDocuments(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

//    public List<ElasticEdge> findAll() throws Exception {
//        return super.findAll(INDEX, ElasticEdge.class);
//    }

    public ElasticEdge findById(String id) throws Exception {
        return super.findById(INDEX, ElasticEdge.class, id);
    }

    public boolean existsId(String id) throws Exception {
        return super.existsId(INDEX, id);
    }

    public HashSet<String> idsByDatasource(String datasource){
        return new HashSet<String>( super.idsByDatasource(INDEX, ElasticEdge.class, datasource) );
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticEdge> findByIds(String[] ids) throws Exception {
        return super.findByIds(INDEX, ElasticEdge.class, ids);
    }

    public List<ElasticEdge> findByLabel(int size, String label) throws Exception {
        return super.findByLabel(INDEX, ElasticEdge.class, size, label);
    }

    public List<ElasticEdge> findByDatasource(int size, String datasource) throws Exception {
        return super.findByDatasource(INDEX, ElasticEdge.class, size, datasource);
    }

    public List<ElasticEdge> findByDatasourceAndLabel(int size, String datasource, String label) throws Exception {
        return super.findByDatasourceAndLabel(INDEX, ElasticEdge.class, size, datasource, label);
    }
    public List<ElasticEdge> findByDatasourceAndLabels(int size, String datasource, final String[] labels) throws Exception {
        return super.findByDatasourceAndLabels(INDEX, ElasticEdge.class, size, datasource, labels);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeys(int size, String datasource, final String[] keys) throws Exception{
        return super.findByDatasourceAndPropertyKeys(INDEX, ElasticEdge.class, size, datasource, keys);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKey(int size, String datasource, String key) throws Exception{
        return super.findByDatasourceAndPropertyKey(INDEX, ElasticEdge.class, size, datasource, key);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeyNot(int size, String datasource, String keyNot) throws Exception{
        return super.findByDatasourceAndPropertyKeyNot(INDEX, ElasticEdge.class, size, datasource, keyNot);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyValues(int size, String datasource, final String[] values) throws Exception{
        return super.findByDatasourceAndPropertyValues(INDEX, ElasticEdge.class, size, datasource, values);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyValue(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValue(INDEX, ElasticEdge.class, size, datasource, value);
    }
    public List<ElasticEdge> findByDatasourceAndPropertyValuePartial(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValuePartial(INDEX, ElasticEdge.class, size, datasource, value);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception{
        return super.findByDatasourceAndPropertyKeyValue(INDEX, ElasticEdge.class, size, datasource, key, value);
    }

    public List<ElasticEdge> findByDatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception{
        return super.findByDatasourceAndLabelAndPropertyKeyValue(INDEX, ElasticEdge.class, size, datasource, label, key, value);
    }
    public List<ElasticEdge> findByDatasourceAndLabelAndPropertyKeyValues(int size, String datasource, String label, Map<String,String> kvPairs) throws Exception{
        return super.findByDatasourceAndLabelAndPropertyKeyValues(INDEX, ElasticEdge.class, size, datasource, label, kvPairs);
    }

    public List<ElasticEdge> findByHasContainers(int size, String datasource
            , String label, final String[] labels
            , String key, String keyNot, final String[] keys
            , final String[] values, final Map<String,String> kvPairs) throws Exception {
        return super.findByHasContainers(INDEX, ElasticEdge.class, size, datasource
                , label, labels, key, keyNot, keys, values, kvPairs);
    }

    public List<ElasticEdge> findByDatasourceAndDirection(
            int size, String datasource, String vid, Direction direction) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // with direction
        if( direction.equals(Direction.IN))
            queryBuilder = queryBuilder.must(termQuery("dst", vid));
        else if( direction.equals(Direction.OUT))
            queryBuilder = queryBuilder.must(termQuery("src", vid));
        else{
            queryBuilder = queryBuilder.should(termQuery("dst", vid));
            queryBuilder = queryBuilder.should(termQuery("src", vid));
        }
        // search
        return doSearch(INDEX, size, queryBuilder, client, mapper, ElasticEdge.class);
    }

    ///////////////////////////////////////////////////////////////
    // APIs : withDateRange

    public Stream<ElasticEdge> streamByIdsWithDateRange(String[] ids, String fromDate, String toDate) throws Exception {
        return super.streamByIdsWithDateRange(INDEX, ElasticEdge.class, ids, fromDate, toDate);
    }
    public Stream<ElasticEdge> streamByDatasourceWithDateRange(String datasource, String fromDate, String toDate) throws Exception {
        return super.streamByDatasourceWithDateRange(INDEX, ElasticEdge.class, datasource, fromDate, toDate);
    }
    public Stream<ElasticEdge> streamByDatasourceAndLabelWithDateRange(String datasource, String label, String fromDate, String toDate) throws Exception {
        return super.streamByDatasourceAndLabelWithDateRange(INDEX, ElasticEdge.class, datasource, label, fromDate, toDate);
    }

    ///////////////////////////////////////////////////////////////
    //
    // Stream APIs
    //
    ///////////////////////////////////////////////////////////////

    public Stream<ElasticEdge> streamByIds(String[] ids) throws Exception {
        return super.streamByIds(INDEX, ElasticEdge.class, ids);
    }

    public Stream<ElasticEdge> streamByDatasource(String datasource) throws Exception {
        return super.streamByDatasource(INDEX, ElasticEdge.class, datasource);
    }
    public Stream<ElasticEdge> streamByDatasourceAndIds(String datasource, String[] ids) throws Exception {
        return super.streamByDatasourceAndIds(INDEX, ElasticEdge.class, datasource, ids);
    }

    public Stream<ElasticEdge> streamByDatasourceAndLabel(String datasource, String label) throws Exception {
        return super.streamByDatasourceAndLabel(INDEX, ElasticEdge.class, datasource, label);
    }
    public Stream<ElasticEdge> streamByDatasourceAndLabels(String datasource, final String[] labels) throws Exception {
        return super.streamByDatasourceAndLabels(INDEX, ElasticEdge.class, datasource, labels);
    }

    public Stream<ElasticEdge> streamByDatasourceAndPropertyKeys(String datasource, final String[] keys) throws Exception{
        return super.streamByDatasourceAndPropertyKeys(INDEX, ElasticEdge.class, datasource, keys);
    }

    public Stream<ElasticEdge> streamByDatasourceAndPropertyKey(String datasource, String key) throws Exception{
        return super.streamByDatasourceAndPropertyKey(INDEX, ElasticEdge.class, datasource, key);
    }
    public Stream<ElasticEdge> streamByDatasourceAndPropertyKeyNot(String datasource, String keyNot) throws Exception{
        return super.streamByDatasourceAndPropertyKeyNot(INDEX, ElasticEdge.class, datasource, keyNot);
    }

    public Stream<ElasticEdge> streamByDatasourceAndPropertyValues(String datasource, final String[] values) throws Exception{
        return super.streamByDatasourceAndPropertyValues(INDEX, ElasticEdge.class, datasource, values);
    }

    public Stream<ElasticEdge> streamByDatasourceAndPropertyValue(String datasource, String value) throws Exception{
        return super.streamByDatasourceAndPropertyValue(INDEX, ElasticEdge.class, datasource, value);
    }
    public Stream<ElasticEdge> streamByDatasourceAndPropertyValuePartial(String datasource, String value) throws Exception{
        return super.streamByDatasourceAndPropertyValuePartial(INDEX, ElasticEdge.class, datasource, value);
    }

    public Stream<ElasticEdge> streamByDatasourceAndPropertyKeyValue(String datasource, String key, String value) throws Exception{
        return super.streamByDatasourceAndPropertyKeyValue(INDEX, ElasticEdge.class, datasource, key, value);
    }

    public Stream<ElasticEdge> streamByDatasourceAndLabelAndPropertyKeyValue(String datasource, String label, String key, String value) throws Exception{
        return super.streamByDatasourceAndLabelAndPropertyKeyValue(INDEX, ElasticEdge.class, datasource, label, key, value);
    }
    public Stream<ElasticEdge> streamByDatasourceAndLabelAndPropertyKeyValues(String datasource, String label, Map<String,String> kvPairs) throws Exception{
        return super.streamByDatasourceAndLabelAndPropertyKeyValues(INDEX, ElasticEdge.class, datasource, label, kvPairs);
    }

    public Stream<ElasticEdge> streamByHasContainers(String datasource
            , String label, final String[] labels
            , String key, String keyNot, final String[] keys
            , final String[] values, final Map<String,String> kvPairs) throws Exception {
        return super.streamByHasContainers(INDEX, ElasticEdge.class, datasource
                , label, labels, key, keyNot, keys, values, kvPairs);
    }

    public Stream<ElasticEdge> streamByDatasourceAndDirection(
            String datasource, String vid, Direction direction) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // with direction
        if( direction.equals(Direction.IN))
            queryBuilder = queryBuilder.must(termQuery("dst", vid));
        else if( direction.equals(Direction.OUT))
            queryBuilder = queryBuilder.must(termQuery("src", vid));
        else{
            queryBuilder = queryBuilder.must(
                    QueryBuilders.boolQuery()   // or Clause
                            .should(termQuery("dst", vid))
                            .should(termQuery("src", vid))
            );
        }

        ElasticScrollIterator<ElasticEdge> iter = new ElasticScrollIterator(INDEX, SCROLL_LIMIT
                , queryBuilder, client, mapper, ElasticEdge.class);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // **참고 https://stackoverflow.com/a/49674368/6811653
    // ((A and B) or (C and D))
    // ==> bool.should( bool.must(A).must(B) ).should( bool.must(C).must(D) )
    public Stream<ElasticEdge> streamByDatasourceWithVertices(String datasource, String[] vids) throws Exception{
        // define : AND query with array value
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.termsQuery("src", vids))
                .must(QueryBuilders.termsQuery("dst", vids));

        ElasticScrollIterator<ElasticEdge> iter = new ElasticScrollIterator(INDEX, SCROLL_LIMIT
                , queryBuilder, client, mapper, ElasticEdge.class);
        return ElasticScrollIterator.flatMapStream(iter);
    }
}
