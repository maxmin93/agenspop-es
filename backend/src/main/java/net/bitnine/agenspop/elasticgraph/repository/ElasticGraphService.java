package net.bitnine.agenspop.elasticgraph.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.ResourceUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

@Slf4j
public class ElasticGraphService {

    static final String MAPPINGS_VERTEX = "classpath:mappings/vertex-document.json";
    static final String MAPPINGS_EDGE = "classpath:mappings/edge-document.json";
    // bucket size of aggregation return
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html#search-aggregations-bucket-terms-aggregation-size
    static final int AGG_BUCKET_SIZE = 1000;

    public final String INDEX_VERTEX;
    public final String INDEX_EDGE;

    private final int numOfShards;
    private final int numOfReplicas;

    private ResourceLoader resourceLoader;

    private RestHighLevelClient client;
    private ObjectMapper objectMapper;

    public ElasticGraphService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper objectMapper,      // spring boot web starter
            String vertexIndex,
            String edgeIndex,
            int numOfShards,
            int numOfReplicas,
            ResourceLoader resourceLoader   // for accessing classpath in fat jar
    ) {
        this.client = client;
        this.objectMapper = objectMapper;
        this.INDEX_VERTEX = vertexIndex;
        this.INDEX_EDGE = edgeIndex;
        this.numOfShards = numOfShards;
        this.numOfReplicas = numOfReplicas;
        this.resourceLoader = resourceLoader;
    }

    ///////////////////////////////////////////////////////////////

    // check if exists index
    private boolean checkExistsIndex(String index) throws  Exception {
        GetIndexRequest request = new GetIndexRequest(index);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    private String readMappings(String index) throws Exception {
        String mappings_file = index.equals(INDEX_VERTEX) ? MAPPINGS_VERTEX : MAPPINGS_EDGE;
        Resource resource = resourceLoader.getResource(mappings_file);
        if( !resource.exists() ){
            System.out.println("** ERROR : mapping of ["+index+"] not found ==> "+resource.getURL().toString());
            throw new FileNotFoundException("mappings not found => "+mappings_file);
        }
        System.out.println("mapping of ["+index+"] ==> "+resource.getURL().toString());
        InputStream inputStream = resourceLoader.getResource(resource.getURL().toString()).getInputStream();
        return new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));
    }

    private boolean createIndex(String index) throws Exception {

        CreateIndexRequest request = new CreateIndexRequest(index);

        // settings
        request.settings(Settings.builder()
                .put("index.number_of_shards", numOfShards)
                .put("index.number_of_replicas", numOfReplicas)
        );
        // mappings
        request.mapping(readMappings(index), XContentType.JSON);
        // **NOTE: mapping.dynamic = false
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic.html#dynamic

        AcknowledgedResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    private boolean removeIndex(String index) throws Exception {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse indexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    public boolean resetIndex() throws Exception {
        boolean result = true;

        if( checkExistsIndex(INDEX_VERTEX) ) removeIndex(INDEX_VERTEX);
        result &= createIndex(INDEX_VERTEX);
        if( checkExistsIndex(INDEX_EDGE) ) removeIndex(INDEX_EDGE);
        result &= createIndex(INDEX_EDGE);

        return result;
    }

    public void ready() throws Exception {
        boolean state = false;
        // if not exists index, create index
        if( !checkExistsIndex(INDEX_VERTEX) ) state |= createIndex(INDEX_VERTEX);
        if( !checkExistsIndex(INDEX_EDGE) ) state |= createIndex(INDEX_EDGE);

        if( state )
            System.out.println("\n** Index not found : create Index ["+INDEX_VERTEX+", "+INDEX_EDGE+"]\n");
    }

    //////////////////////////////////////////////
    // schema services

    // REST API : Aggregation
    // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_metrics_aggregations.html
    // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_bucket_aggregations.html

    public Map<String, Long> listDatasources(String index, List<String> dsList) throws Exception {
        // when search graphs by query, if no match then return empty
        if(dsList == null) return Collections.EMPTY_MAP;

        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // match to datasource
        QueryBuilder queryBuilder = dsList.isEmpty() ? QueryBuilders.matchAllQuery()
                : QueryBuilders.boolQuery().filter(termsQuery("datasource", dsList));
        searchSourceBuilder.query(queryBuilder)
                .aggregation(AggregationBuilders.terms("datasources")
                        .field("datasource").order(BucketOrder.key(true))
                        .size(AGG_BUCKET_SIZE)
                ).size(0);

        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        Aggregations aggregations = searchResponse.getAggregations();
        Terms labels = aggregations.get("datasources");

        Map<String, Long> result = new HashMap<>();
        labels.getBuckets().forEach(b->{
            result.put(b.getKeyAsString(), b.getDocCount());
        });
        return result;
    }

    public Map<String, Long> listLabels(String index, String datasource) throws Exception {
        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource)))
                .aggregation(AggregationBuilders.terms("labels")
                        .field("label").order(BucketOrder.key(true))
                        .size(AGG_BUCKET_SIZE)
                ).size(0);

        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        Aggregations aggregations = searchResponse.getAggregations();
        Terms labels = aggregations.get("labels");

        Map<String, Long> result = new HashMap<>();
        labels.getBuckets().forEach(b->{
            result.put(b.getKeyAsString(), b.getDocCount());
        });
        return result;
    }

    public Map<String, Long> listLabelKeys(String index, String datasource, String label) throws Exception {
        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                    .filter(termQuery("datasource", datasource))
                    .filter(termQuery("label", label))
                )
                .aggregation(AggregationBuilders.nested("agg", "properties")
                    .subAggregation(
                        AggregationBuilders.terms("keys").field("properties.key")
                            .subAggregation(
                                AggregationBuilders.reverseNested("label_to_key")
                            ).size(AGG_BUCKET_SIZE)
                    )
                ).size(0);

        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        Aggregations aggregations = searchResponse.getAggregations();
        Nested agg = aggregations.get("agg");
        Terms keys = agg.getAggregations().get("keys");

        Map<String, Long> result = new HashMap<>();
        keys.getBuckets().forEach(b->{
            result.put(b.getKeyAsString(), b.getDocCount());
        });
        return result;
    }

    // query 에 매칭되는 datasource list 만 반환한다
    public List<String> searchDatasources(String index, String query) throws Exception {
        // query : match and aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            QueryBuilders.queryStringQuery("properties.value:\"" + query.toLowerCase() + "\""))
                    , ScoreMode.Total));
        // append : aggregation
        searchSourceBuilder.query(queryBuilder)
                .aggregation(AggregationBuilders.terms("datasources")
                        .field("datasource").order(BucketOrder.key(true))
                        .size(AGG_BUCKET_SIZE)
                ).size(0);

        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        Aggregations aggregations = searchResponse.getAggregations();
        Terms labels = aggregations.get("datasources");

        List<String> result = new ArrayList<>();
        labels.getBuckets().forEach(b->{
            result.add(b.getKeyAsString());
        });
        return result;
    }

}


/*
GET /newsvertex/_search?pretty
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
          { "term": {"datasource": "d11794281"} }
      ]
    }
  },
  "aggs": {
    "labels": {
      "terms": {
        "field": "label", "size" : 1000
      }
    }
  }
}
 */
/*
    // 한글 검색 : 'korean' analyzer and nested field
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html
    // 이렇게 해도 됨 ==> { "match": { "properties.value.korean": "검찰" } }

    // ** 추가
    // datasource 내에 어떤 label 이 있는지 모르므로, 검색 결과에 대한 datasource 리스트를 만들고 싶다면,
    // 1) 먼저 aggs 의 결과로 datasource 리스트를 만들고
    // 2) datasource 리스트 대상으로 vertex 와 edge 카운트를 가져와야 함

GET /newsvertex/_search?pretty
{
  "size": 0,
  "query": {
    "nested": {
      "path": "properties",
      "query": {
        "bool": {
          "must": [
            { "match": { "properties.value": "검찰" } }
          ]
        }
      },
      "score_mode": "avg"
    }
  },
  "aggs": {
    "labels": {
      "terms": {
        "field": "datasource", "size" : 100
      }
    }
  }
}

# ** nested 와 non-nested 필드들의 혼합 쿼리
# https://stackoverflow.com/a/58621114

GET newsvertex/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": { "label": "document" } },
        { "nested": {
            "path": "properties",
            "query": {
              "bool": {
                "must": [
                  { "match": { "properties.value": "검찰" } }
                ]
              }
            },
            "score_mode": "avg"
          }
        }
      ]
    }
  }
}
 */
