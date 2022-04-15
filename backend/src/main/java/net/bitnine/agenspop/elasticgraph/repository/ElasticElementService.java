package net.bitnine.agenspop.elasticgraph.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.elasticgraph.model.ElasticElement;
import net.bitnine.agenspop.elasticgraph.util.ElasticHelper;
import net.bitnine.agenspop.elasticgraph.util.ElasticScrollIterator;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class ElasticElementService {

    private final long SCROLL_LIMIT;

    protected final RestHighLevelClient client;
    protected final ObjectMapper mapper;

    protected ElasticElementService(
            RestHighLevelClient client,     // elasticsearch config
            long limit,                     // request scroll limit(-1, more than 2500)
            ObjectMapper mapper             // spring boot web starter
    ) {
        this.client = client;
        this.SCROLL_LIMIT = limit;
        this.mapper = mapper;
    }

    ///////////////////////////////////////////////////////////////

    protected <T> String createDocument(String index, Class<T> tClass, ElasticElement document
    ) throws Exception {
        if( document.getId() == null || document.getId().isEmpty() ){
            UUID uuid = UUID.randomUUID();      // random document_id
            document.setId(uuid.toString());
        }

        IndexRequest indexRequest = new IndexRequest(index)
                .id(document.getId())
                .source( mapper.convertValue((T)document, Map.class) );
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.getResult().name();
    }

    protected <T> String updateDocument(String index, Class<T> tClass, ElasticElement document
    ) throws Exception {
        if( document.getId() == null || document.getId().isEmpty() )
            return "NOT_FOUND";

        UpdateRequest updateRequest = new UpdateRequest().index(index)
                .id(document.getId())
                .doc( mapper.convertValue((T)document, Map.class) );
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.getResult().name();
    }

    protected String deleteDocument(String index, String id
    ) throws Exception {
        DeleteRequest deleteRequest = new DeleteRequest(index).id(id);
        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        return response.getResult().name();
    }

    // **NOTE: DeleteByQueryRequest API
    // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.3/java-rest-high-document-delete-by-query.html
    protected long deleteDocuments(String index, String datasource
    ) throws Exception {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.setQuery(new TermQueryBuilder("datasource", datasource));
        // request.setSlices(2);
        request.setRefresh(true);
        request.setConflicts("proceed");
        BulkByScrollResponse bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
        return bulkResponse.getDeleted();
    }

    ///////////////////////////////////////////////////////////////

    protected long count(String index
    ) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        CountRequest countRequest = new CountRequest().indices(index);
        countRequest.source(searchSourceBuilder);
        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
        return countResponse.getCount();
    }

    protected long count(String index, String datasource
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = ElasticHelper.addQueryDs(QueryBuilders.boolQuery(), datasource);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);

        CountRequest countRequest = new CountRequest().indices(index);
        countRequest.source(searchSourceBuilder);
        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
        return countResponse.getCount();
    }

    ///////////////////////////////////////////////////////////////

//    protected <T> List<T> findAll(String index, Class<T> tClass) throws Exception {
//        SearchRequest searchRequest = new SearchRequest(index);
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());       // All
//        searchSourceBuilder.size(10000);                                // es request LIMIT
//        searchRequest.source(searchSourceBuilder);
//        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//        return getSearchResult(searchResponse, mapper, tClass);
//    }

    protected <T> T findById(String index, Class<T> tClass, String id
    ) throws Exception {
        GetRequest getRequest = new GetRequest(index).id(id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> resultMap = getResponse.getSource();
        return mapper.convertValue(resultMap, tClass);
    }

    protected boolean existsId(String index, String id
    ) throws Exception {
        GetRequest getRequest = new GetRequest(index, id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    ///////////////////////////////////////////////////////////////

    // DS.hadId(id..)
    protected <T> List<T> findByIds(String index, Class<T> tClass
            , String[] ids
    ) throws Exception {
        // match to id array
        IdsQueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(ids);
        // search
        return doSearch(index, ids.length, queryBuilder, client, mapper, tClass);
    }

    protected <T> List<T> findByLabel(String index, Class<T> tClass, int size
            , String label
    ) throws Exception {
        // match to label
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("label", label));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.V(), DS.E()
    protected <T> List<T> findByDatasource(String index, Class<T> tClass, int size
            , String datasource
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.hasLabel(label..)
    protected <T> List<T> findByDatasourceAndLabel(String index, Class<T> tClass, int size
            , String datasource, String label
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termQuery("label", label));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }
    // DS.hasLabel(label..)
    protected <T> List<T> findByDatasourceAndLabels(String index, Class<T> tClass, int size
            , String datasource, String[] labels
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termsQuery("label", labels));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.hasKey(key..)
    protected <T> List<T> findByDatasourceAndPropertyKeys(String index, Class<T> tClass, int size
            , String datasource, String[] keys
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // AND
        for( String key : keys ){
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                        termQuery("properties.key", key))
                , ScoreMode.Max));
        }
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.has(key)
    protected <T> List<T> findByDatasourceAndPropertyKey(String index, Class<T> tClass, int size
            , String datasource, String key
    ) throws Exception{
        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key))
                    , ScoreMode.Avg));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.hasNot(key)
    protected <T> List<T> findByDatasourceAndPropertyKeyNot(String index, Class<T> tClass, int size
            , String datasource, String key
    ) throws Exception{
        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .mustNot(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key))
                    , ScoreMode.Avg));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.hasValue(value..)
    protected <T> List<T> findByDatasourceAndPropertyValues(String index, Class<T> tClass, int size
            , String datasource, String[] values
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // AND
        for( String value : values ) {
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                        QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\""))
                , ScoreMode.Total));
        }
        // search
        List<T> list = doSearch(index, size, queryBuilder, client, mapper, tClass);
        // compare two values by full match with lowercase
        List<String> filters = Arrays.asList(values).stream().map(String::toLowerCase).collect(Collectors.toList());
        return ElasticHelper.postFilterByValues(list, filters);
    }

    // DS.hasValue(value)
    protected <T> List<T> findByDatasourceAndPropertyValue(String index, Class<T> tClass, int size
            , String datasource, String value
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                            QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\""))
                    , ScoreMode.Total));
        // search
        List<T> list = doSearch(index, size, queryBuilder, client, mapper, tClass);
        return ElasticHelper.postFilterByValue(list, value.toLowerCase());
    }

    // DS.hasValuePartial(value)
    protected <T> List<T> findByDatasourceAndPropertyValuePartial(String index, Class<T> tClass, int size
            , String datasource, String value
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                            // QueryBuilders.wildcardQuery("properties.value", "*"+value.toLowerCase()+"*")
                            QueryBuilders.queryStringQuery("properties.value:*"+value.toLowerCase()+"*"))
                    , ScoreMode.Avg));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.has(key,value)
    protected <T> List<T> findByDatasourceAndPropertyKeyValue(String index, Class<T> tClass, int size
            , String datasource, String key, String value
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties"
                        , QueryBuilders.boolQuery()
                            .must(termQuery("properties.key", key))
                            .must(QueryBuilders.queryStringQuery("properties.value:\""+value.toLowerCase()+"\""))
                    , ScoreMode.Total));
                ;
        // search
        List<T> list = doSearch(index, size, queryBuilder, client, mapper, tClass);
        return ElasticHelper.postFilterByValue(list, value.toLowerCase());
    }

    // DS.has(key,value) or DS.has(label,key,value) or DS.has(label,key,value)&has(key,value)
    protected <T> List<T> findByDatasourceAndLabelAndPropertyKeyValues(String index, Class<T> tClass, int size
            , String datasource, String label, Map<String, String> kvPairs
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // match label
        if( label != null )
            queryBuilder = queryBuilder.filter(termQuery("label", label));
        // match property.key and .value
        for( Map.Entry<String,String> kv : kvPairs.entrySet() ) {
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties", QueryBuilders.boolQuery()
                            .must(termQuery("properties.key", kv.getKey()))
                            .must(QueryBuilders.queryStringQuery("properties.value:\"" + kv.getValue().toLowerCase() + "\""))
                    , ScoreMode.Total));
        }

        // search
        List<T> list = doSearch(index, size, queryBuilder, client, mapper, tClass);
        List<String> filters = kvPairs.values().stream().map(String::toLowerCase).collect(Collectors.toList());
        return ElasticHelper.postFilterByValues(list, filters);
    }

    // DS.has(label,key,value)
    protected <T> List<T> findByDatasourceAndLabelAndPropertyKeyValue(String index, Class<T> tClass, int size
            , String datasource, String label, String key, String value
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termQuery("label", label))
                .must(QueryBuilders.nestedQuery("properties", QueryBuilders.boolQuery()
                            .must(termQuery("properties.key", key.toLowerCase()))
                            .must(QueryBuilders.queryStringQuery("properties.value:\""+value.toLowerCase()+"\""))
                    , ScoreMode.Total));
        // search
        List<T> list = doSearch(index, size, queryBuilder, client, mapper, tClass);
        return ElasticHelper.postFilterByValue(list, value.toLowerCase());
    }

    protected <T> List<T> findByHasContainers(String index, Class<T> tClass, int size
            , String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs
    ) throws Exception {
        // init
        BoolQueryBuilder qb = ElasticHelper.addQueryDs(QueryBuilders.boolQuery(), datasource);
        // AND hasCondition
        if( label != null ) qb = ElasticHelper.addQueryLabel(qb, label);
        if( labels != null && labels.length > 0 ) qb = ElasticHelper.addQueryLabels(qb, labels);
        if( key != null ) qb = ElasticHelper.addQueryKey(qb, key);
        if( keyNot != null ) qb = ElasticHelper.addQueryKeyNot(qb, keyNot);
        if( keys != null && keys.length > 0 ) qb = ElasticHelper.addQueryKeys(qb, keys);
        if( values != null && values.length > 0 ) qb = ElasticHelper.addQueryValues(qb, values);
        if( kvPairs != null && kvPairs.size() > 0 ){
            for(Map.Entry<String,String> kv : kvPairs.entrySet()){
                qb = ElasticHelper.addQueryKeyValue(qb, kv.getKey(), kv.getValue());
            }
        }

        // search
        List<T> list = doSearch(index, size, qb, client, mapper, tClass);

        // post filters
        if( values != null && values.length > 0 ){
            // compare two values by full match with lowercase
            List<String> filters = Arrays.asList(values).stream().map(String::toLowerCase).collect(Collectors.toList());
            list = ElasticHelper.postFilterByValues(list, filters);
        }
        if( kvPairs != null && kvPairs.size() > 0 ){
            List<String> filters = kvPairs.values().stream().map(String::toLowerCase).collect(Collectors.toList());
            list = ElasticHelper.postFilterByValues(list, filters);
        }
        return list;
    }

    protected static final <T> List<T> getSearchResult(
            SearchResponse response, ObjectMapper mapper, Class<T> tClass
    ) {
        SearchHit[] searchHit = response.getHits().getHits();
        List<T> documents = new ArrayList<>();
        for (SearchHit hit : searchHit){
            documents.add(mapper.convertValue(hit.getSourceAsMap(), tClass));
        }
        return documents;
    }

    protected final <T> List<T> doSearch(
            String index, int size, QueryBuilder queryBuilder
            , RestHighLevelClient client, ObjectMapper mapper, Class<T> tClass
    ) throws Exception {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return getSearchResult(response, mapper, tClass);
    }


    ///////////////////////////////////////////////////////////////
    // APIs : withDateRange

    private RangeQueryBuilder dateRangeBuilder(String fromDate, String toDate) {
        if( fromDate != null && toDate == null )
            return rangeQuery(BaseElement.timestampField).gte(fromDate);
        else if( fromDate == null && toDate != null )
            return rangeQuery(BaseElement.timestampField).lte(toDate);
        else if( fromDate != null && toDate != null )
            return rangeQuery(BaseElement.timestampField).gte(fromDate).lte(toDate);
        else
            return rangeQuery(BaseElement.timestampField).gte("now-10m").lt("now");   // 최근 10분
//            return rangeQuery(BaseElement.createdField).gte("now-1d/d").lt("now/d");  // 어제
    }

    // DS.hadId(id..)
    protected <T extends ElasticElement> Stream<T> streamByIdsWithDateRange(
            String index, Class<T> tClass, String[] ids, String fromDate, String toDate
    ) throws Exception {
        // match to id array and range
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(idsQuery().addIds(ids))
                .filter(dateRangeBuilder(fromDate, toDate));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT, SortOrder.ASC
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.V(), DS.E()
    protected <T extends ElasticElement> Stream<T> streamByDatasourceWithDateRange(
            String index, Class<T> tClass, String datasource, String fromDate, String toDate
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(dateRangeBuilder(fromDate, toDate));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT, SortOrder.ASC
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.hasLabel(label..)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndLabelWithDateRange(
            String index, Class<T> tClass, String datasource, String label, String fromDate, String toDate
    ) throws Exception {
        // match to datasource, label and range
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termQuery("label", label))
                .filter(dateRangeBuilder(fromDate, toDate));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT, SortOrder.ASC
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    ///////////////////////////////////////////////////////////////
    //
    //  Stream APIs
    //
    ///////////////////////////////////////////////////////////////

    // DS.V().ids, DS.E().ids
    protected <T extends ElasticElement> List<String> idsByDatasource(
            String index, Class<T> tClass, String datasource
    ){
        List<String> ids = new ArrayList<>();
        try {
            ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                    , datasource, client, mapper, tClass);
            while (iter.hasNext()) ids.addAll(iter.nextIds());
        } catch(Exception e){ }
        return ids;
    }

    // DS.hadId(id..)
    protected <T extends ElasticElement> Stream<T> streamByIds(
            String index, Class<T> tClass, String[] ids
    ) throws Exception {
        // match to datasource
        IdsQueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(ids);

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.hadId(id..)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndIds(
            String index, Class<T> tClass, String datasource, String[] ids
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.idsQuery().addIds(ids));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.V(), DS.E()
    protected <T extends ElasticElement> Stream<T> streamByDatasource(
            String index, Class<T> tClass, String datasource
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.hasLabel(label..)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndLabel(
            String index, Class<T> tClass, String datasource, String label
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termQuery("label", label));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.hasLabel(label..)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndLabels(
            String index, Class<T> tClass, String datasource, String[] labels
    ) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termsQuery("label", labels));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.hasKey(key..)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndPropertyKeys(
            String index, Class<T> tClass, String datasource, String[] keys
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // AND
        for( String key : keys ){
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key))
                    , ScoreMode.Max));
        }

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.has(key)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndPropertyKey(
            String index, Class<T> tClass, String datasource, String key
    ) throws Exception{
        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                                termQuery("properties.key", key))
                        , ScoreMode.Avg));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.hasNot(key)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndPropertyKeyNot(
            String index, Class<T> tClass, String datasource, String key
    ) throws Exception{
        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .mustNot(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                                termQuery("properties.key", key))
                        , ScoreMode.Avg));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.hasValue(value..)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndPropertyValues(
            String index, Class<T> tClass, String datasource, String[] values
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // AND
        for( String value : values ) {
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\""))
                    , ScoreMode.Total));
        }

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        Stream<T> stream = ElasticScrollIterator.flatMapStream(iter);

        // compare two valuesList each other by full match with lowercase
        List<String> filters = Arrays.asList(values).stream().map(String::toLowerCase).collect(Collectors.toList());
        return stream.filter(r -> {
            List<String> pvalues = r.getProperties().stream()
                    .map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
            return pvalues.containsAll(filters);
        });
    }

    // DS.hasValue(value)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndPropertyValue(
            String index, Class<T> tClass, String datasource, String value
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\""))
                        , ScoreMode.Total));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        Stream<T> stream = ElasticScrollIterator.flatMapStream(iter);

        // compare value with valuesList by full match with lowercase
        return stream.filter(r -> {
            List<String> pvalues = r.getProperties().stream()
                    .map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
            return pvalues.contains(value.toLowerCase());
        });
    }

    // DS.hasValuePartial(value)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndPropertyValuePartial(
            String index, Class<T> tClass, String datasource, String value
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                                // QueryBuilders.wildcardQuery("properties.value", "*"+value.toLowerCase()+"*")
                                QueryBuilders.queryStringQuery("properties.value:*"+value.toLowerCase()+"*"))
                        , ScoreMode.Avg));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        return ElasticScrollIterator.flatMapStream(iter);
    }

    // DS.has(key,value)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndPropertyKeyValue(
            String index, Class<T> tClass, String datasource, String key, String value
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties"
                        , QueryBuilders.boolQuery()
                                .must(termQuery("properties.key", key))
                                .must(QueryBuilders.queryStringQuery("properties.value:\""+value.toLowerCase()+"\""))
                        , ScoreMode.Total));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        Stream<T> stream = ElasticScrollIterator.flatMapStream(iter);

        // compare value with valuesList by full match with lowercase
        return stream.filter(r -> {
            List<String> pvalues = r.getProperties().stream()
                    .map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
            return pvalues.contains(value.toLowerCase());
        });
    }

    // DS.has(key,value) or DS.has(label,key,value) or DS.has(label,key,value)&has(key,value)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndLabelAndPropertyKeyValues(
            String index, Class<T> tClass, String datasource
            , String label, Map<String, String> kvPairs
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // match label
        if( label != null )
            queryBuilder = queryBuilder.filter(termQuery("label", label));
        // match property.key and .value
        for( Map.Entry<String,String> kv : kvPairs.entrySet() ) {
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties", QueryBuilders.boolQuery()
                            .must(termQuery("properties.key", kv.getKey()))
                            .must(QueryBuilders.queryStringQuery("properties.value:\"" + kv.getValue().toLowerCase() + "\""))
                    , ScoreMode.Total));
        }

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        Stream<T> stream = ElasticScrollIterator.flatMapStream(iter);

        // compare two valuesList each other by full match with lowercase
        List<String> filters = kvPairs.values().stream().map(String::toLowerCase).collect(Collectors.toList());
        return stream.filter(r -> {
            List<String> pvalues = r.getProperties().stream()
                    .map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
            return pvalues.containsAll(filters);
        });
    }

    // DS.has(label,key,value)
    protected <T extends ElasticElement> Stream<T> streamByDatasourceAndLabelAndPropertyKeyValue(
            String index, Class<T> tClass, String datasource, String label, String key, String value
    ) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termQuery("label", label))
                .must(QueryBuilders.nestedQuery("properties", QueryBuilders.boolQuery()
                                .must(termQuery("properties.key", key.toLowerCase()))
                                .must(QueryBuilders.queryStringQuery("properties.value:\""+value.toLowerCase()+"\""))
                        , ScoreMode.Total));

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , queryBuilder, client, mapper, tClass);
        Stream<T> stream = ElasticScrollIterator.flatMapStream(iter);

        // compare value with valuesList by full match with lowercase
        return stream.filter(r -> {
            List<String> pvalues = r.getProperties().stream()
                    .map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
            return pvalues.contains(value.toLowerCase());
        });
    }

    ///////////////////////////////////////////////////////////////

    protected <T extends ElasticElement> Stream<T> streamByHasContainers(
            String index, Class<T> tClass, String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs
    ) throws Exception {
        // init
        BoolQueryBuilder qb = ElasticHelper.addQueryDs(QueryBuilders.boolQuery(), datasource);
        // AND hasCondition
        if( label != null ) qb = ElasticHelper.addQueryLabel(qb, label);
        if( labels != null && labels.length > 0 ) qb = ElasticHelper.addQueryLabels(qb, labels);
        if( key != null ) qb = ElasticHelper.addQueryKey(qb, key);
        if( keyNot != null ) qb = ElasticHelper.addQueryKeyNot(qb, keyNot);
        if( keys != null && keys.length > 0 ) qb = ElasticHelper.addQueryKeys(qb, keys);
        if( values != null && values.length > 0 ) qb = ElasticHelper.addQueryValues(qb, values);
        if( kvPairs != null && kvPairs.size() > 0 ){
            for(Map.Entry<String,String> kv : kvPairs.entrySet()){
                qb = ElasticHelper.addQueryKeyValue(qb, kv.getKey(), kv.getValue());
            }
        }

        ElasticScrollIterator<T> iter = new ElasticScrollIterator(index, SCROLL_LIMIT
                , qb, client, mapper, tClass);
        Stream<T> stream = ElasticScrollIterator.flatMapStream(iter);

        // post filters
        if( values != null && values.length > 0 ){
            // compare two values by full match with lowercase
            List<String> filters = Arrays.asList(values).stream().map(String::toLowerCase).collect(Collectors.toList());
            stream = stream.filter(r -> {
                List<String> pvalues = r.getProperties().stream()
                        .map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
                return pvalues.containsAll(filters);
            });
        }
        if( kvPairs != null && kvPairs.size() > 0 ){
            List<String> filters = kvPairs.values().stream().map(String::toLowerCase).collect(Collectors.toList());
            stream = stream.filter(r -> {
                List<String> pvalues = r.getProperties().stream()
                        .map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
                return pvalues.containsAll(filters);
            });
        }
        return stream;
    }

}
