package net.bitnine.agenspop.elasticgraph.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.elasticgraph.model.ElasticElement;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;


// **NOTE: full document request over 10000 records
//
// https://github.com/spring-projects/spring-data-elasticsearch/blob/master/src/main/java/org/springframework/data/elasticsearch/core/ElasticsearchRestTemplate.java
// https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search-scroll.html

public class ElasticScrollIterator<T extends ElasticElement> implements Iterator<List<T>> {

    private final long SCROLL_LIMIT;    // if -1, then unlimit scroll
    private long current_size = 0L;
    private SortOrder sortOrder = null;

    public static int SCROLL_SIZE = 2500;      // MAX=10000
    public static Scroll SCROLL_TIME = new Scroll(TimeValue.timeValueMinutes(1L));

    private final RestHighLevelClient client;
    private final String index;
    private final Class<T> tClass;
    private final ObjectMapper mapper;
    // private final QueryBuilder queryBuilder;

    private String scrollId;
    private SearchHit[] searchHits;

    public ElasticScrollIterator(String index, long limit, QueryBuilder queryBuilder
            , RestHighLevelClient client, ObjectMapper mapper, Class<T> tClass) {
        this.client = client;
        this.SCROLL_LIMIT = limit;
        this.index = index;
        this.tClass = tClass;
        this.mapper = mapper;
        startScroll(queryBuilder, true);
    }
    public ElasticScrollIterator(String index, long limit, SortOrder sortOrder, QueryBuilder queryBuilder
            , RestHighLevelClient client, ObjectMapper mapper, Class<T> tClass) {
        this(index, limit, queryBuilder, client, mapper, tClass);
        this.sortOrder = sortOrder;
    }

    public ElasticScrollIterator(String index, long limit, String datasource
            , RestHighLevelClient client, ObjectMapper mapper, Class<T> tClass){
        this.client = client;
        this.SCROLL_LIMIT = limit;
        this.index = index;
        this.tClass = tClass;
        this.mapper = mapper;
        startScroll(QueryBuilders.boolQuery().filter(termQuery("datasource", datasource)), false);
    }

    @Override
    public boolean hasNext() {
        if( searchHits != null && searchHits.length > 0
            && (SCROLL_LIMIT < 0 || SCROLL_LIMIT >= current_size + SCROLL_SIZE)
        ) return true;

        endScroll();    // stop and clear scroll
        return false;
    }

    @Override
    public List<T> next() {
        List<T> documents = new ArrayList<>();
        for (SearchHit hit : searchHits) {
            documents.add(mapper.convertValue(hit.getSourceAsMap(), tClass));
        }
        current_size += documents.size();
        doScroll();     // next scroll
        return documents;
    }

    public List<String> nextIds() {
        List<String> ids = new ArrayList<>();
        for (SearchHit hit : searchHits) {
            ids.add(hit.getId());
        }
        doScroll();     // next scroll
        return ids;
    }

    ///////////////////////////////////////////////////////

    // Function to get the Stream
    public static <T> Stream<T> flatMapStream(Iterator<List<T>> iterator)
    {
        // Convert the iterator to Spliterator
        Spliterator<List<T>> spliterator = Spliterators
                .spliteratorUnknownSize(iterator, 0);
        // Get a Sequential Stream from spliterator
        return StreamSupport.stream(spliterator, false).flatMap(r -> r.stream());
    }

    private void startScroll(QueryBuilder queryBuilder, boolean withSource) {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(queryBuilder);        // All
            searchSourceBuilder.size(SCROLL_SIZE);          // 1-page size
            searchSourceBuilder.fetchSource(withSource);    // fetch source

            // **BUG: date sorting not working. why? (But, sort by _id is working)
            if( this.sortOrder != null ) searchSourceBuilder.sort(
                    new FieldSortBuilder("id").order(this.sortOrder) );
                    // new FieldSortBuilder(BaseElement.timestampField).order(this.sortOrder) );

            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(SCROLL_TIME);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }catch (Exception ex){
            searchHits = null;
        }
    }

    private boolean doScroll() {
        try{
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(SCROLL_TIME);
            SearchResponse searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            return true;
        }catch (Exception ex){
            searchHits = null;
        }
        return false;
    }

    private boolean endScroll() {
        if( scrollId == null ) return false;
        try {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            scrollId = null;
            return clearScrollResponse.isSucceeded();
        }catch (Exception ex){
            searchHits = null;
        }
        return false;
    }

}
