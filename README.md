# Agenspop

Agenspop is graph framework and visualization for Big-graph using Elasticsearch.
As you know, Agenspop is build based on Apache Tinkerpop.

This project has two sub-module
1) frontend : visualization tool
2) backend : agenspop server connecting elasticsearch

Before run agenspop, you should setup elasticsearch.
- modify es-config.yml for connecting your ES

## summary 

빅그래프를 활용하기 위한 Agenspop 솔루션 3가지를 데모와 함께 소개합니다.
Agenspop은 Elasticsearch의 빠른 검색능력과 수평적 확장 능력을 이용해 빅그래프에 대한 빠른 활용을 가능하게 합니다.
Gremlin과 Cypher를 지원하며 개별적인 API 를 통해 그래프 데이터에 접근할 수도 있습니다.
또한 100만개의 노드/에지를 그릴 수 있는 Webgl를 통해 빅그래프를 시각적으로 접근할 수 있고 Whole-detail 의 탐색을 가능하게 합니다.
그래프 분석은 Spark 분산 플랫폼을 통해 ES-hadoop 에 접근하여 대규모 빅그래프 데이터에 대한 그래프 분석을 빠르게 수행할 수 있습니다.
ES에 대한 그래프 데이터는 logstash 라는 ETL 도구를 통해 적재할 수 있습니다. 

- [Agenspop-es](https://github.com/bitnine-oss/agenspop-es) : 빅그래프의 쿼리 엔진과 시각화 도구
- [Agenspop-spark](https://github.com/bitnine-oss/agenspop-spark) : 빅그래프 Spark 분석 서버
- [ls-filter-agenspop](https://github.com/bitnine-oss/ls-filter-agenspop) : 그래프 적재를 위한 logstash filter plugin

## build & deploy 

그래프 쿼리(gremlin&cypher)가 가능한 백엔드와 Webgl/Canvas 시각화 도구

- build : mvn clean install -DskipTests
- deploy : backend/target/agenspop-es-0.7.jar 
- run : java -jar <jar_file> --spring.config.name=es-config
- visualization client : http://<IP:PORT>/index.html


## _`agenspop`_ main features

## Rest-API

### admin api
- http://<host:port>/api/admin/graphs
- http://<host:port>/api/admin/labels/modern
- http://<host:port>/api/admin/keys/modern/person

### search api
- http://<host:port>/api/search/modern/v
- http://<host:port>/api/search/modern/e

### gremlin api
- http://<host:port>/api/graph/gremlin?q=modern_g.V().has(%27age%27,gt(30))
- http://<host:port>/api/graph/gremlin?q=modern_g.V().has(%27name%27,%20%27marko%27).out().out().valueMap()
- http://<host:port>/api/graph/gremlin?q=northwind_g.V().groupCount().by(T.label)
- http://<host:port>/api/graph/gremlin?q=northwind_g.V().hasLabel(%27product%27).properties().key().groupCount()
- http://<host:port>/api/graph/gremlin?q=northwind_g.E().project("self","inL","outL").by(__.label()).by(__.inV().label()).by(__.outV().label()).groupCount()

### cypher api
- http://<host:port>/api/graph/cypher?ds=modern&q=match%20(a:person%20%7Bcountry:%20%27USA%27%7D)%20return%20a,%20id(a)%20limit%2010


### ElasticVertex, ElasticEdge

processing transaction with traversaling vertices and edges

### logstash filter for agenspop

...
 
### AgensWorkspace

This has two mode that are canvas and webgl.
At first, graph data will be loaded on webgl because webgl is more strong and fast.
And you can crop what you want look detail on webgl. 
Cropped graph will be loaded on canvas. 
canvas is powerful with many functions.   


I hope enjoy agenspop.
