package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.basegraph.BaseGraphAPI;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class AgensIoRegistryV1 extends AbstractIoRegistry {

    private static final AgensIoRegistryV1 INSTANCE = new AgensIoRegistryV1();
    protected static BaseGraphAPI baseGraph = null;

    private AgensIoRegistryV1() {
        register(GryoIo.class, AgensGraph.class, new AgensGraphGryoSerializer());
        register(GraphSONIo.class, null, new AgensModule());
    }

    public static AgensIoRegistryV1 instance() {
        return INSTANCE;
    }

    public void setBaseGraph(BaseGraphAPI baseGraph){
        this.baseGraph = baseGraph;
    }

    /**
     * Provides a method to serialize an entire {@link AgensGraph} into itself for Gryo.  This is useful when
     * shipping small graphs around through Gremlin Server. Reuses the existing Kryo instance for serialization.
     */
    final static class AgensGraphGryoSerializer extends Serializer<AgensGraph> {
        @Override
        public void write(final Kryo kryo, final Output output, final AgensGraph graph) {
            try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                GryoWriter.build().mapper(() -> kryo).create().writeGraph(stream, graph);
                final byte[] bytes = stream.toByteArray();
                output.writeInt(bytes.length);
                output.write(bytes);
            } catch (Exception io) {
                throw new RuntimeException(io);
            }
        }

        @Override
        public AgensGraph read(final Kryo kryo, final Input input, final Class<AgensGraph> tinkerGraphClass) {
//            final Configuration conf = new BaseConfiguration();
//            conf.setProperty("gremlin.tinkergraph.defaultVertexPropertyCardinality", "list");
//            if( !conf.containsKey( AgensGraph.GREMLIN_AGENSGRAPH_GRAPH_NAME )){
//                conf.setProperty( AgensGraph.GREMLIN_AGENSGRAPH_GRAPH_NAME, AgensFactory.defaultGraphName());
//            }
            final AgensGraph graph = AgensGraph.open(baseGraph);
            final int len = input.readInt();
            final byte[] bytes = input.readBytes(len);
            try (final ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
                GryoReader.build().mapper(() -> kryo).create().readGraph(stream, graph);
            } catch (Exception io) {
                throw new RuntimeException(io);
            }

            return graph;
        }
    }

    /**
     * Provides a method to serialize an entire {@link AgensGraph} into itself for GraphSON.  This is useful when
     * shipping small graphs around through Gremlin Server.
     */
    final static class AgensModule extends SimpleModule {
        public AgensModule() {
            super("agensgraph-1.0");
            addSerializer(AgensGraph.class, new AgensIoRegistryV1.AgensGraphJacksonSerializer());
            addDeserializer(AgensGraph.class, new AgensIoRegistryV1.AgensGraphJacksonDeserializer());

            addSerializer(AgensEdge.class, new AgensIoRegistryV1.AgensEdgeJacksonSerializer(false));
            addSerializer(AgensVertex.class, new AgensIoRegistryV1.AgensVertexJacksonSerializer(false));
            addSerializer(AgensVertexProperty.class, new AgensIoRegistryV1.AgensVertexPropertyJacksonSerializer(false));
            addSerializer(AgensProperty.class, new AgensIoRegistryV1.AgensPropertyJacksonSerializer());

            addSerializer(BaseProperty.class, new AgensIoRegistryV1.ElasticPropertyJacksonSerializer());
        }
    }

    /**
     * Serializes the graph into an edge list format.  Edge list is a better choices than adjacency list (which is
     * typically standard from the {@link GraphReader} and {@link GraphWriter} perspective) in this case because
     * the use case for this isn't around massive graphs.  The use case is for "small" subgraphs that are being
     * shipped over the wire from Gremlin Server. Edge list format is a bit easier for non-JVM languages to work
     * with as a format and doesn't require a cache for loading (as vertex labels are not serialized in adjacency
     * list).
     */
    final static class AgensGraphJacksonSerializer extends StdSerializer<AgensGraph> {

        public AgensGraphJacksonSerializer() {
            super(AgensGraph.class);
        }

/*
    graph: {
        name: ""                          // graphName (구별자) ==> datasource
        elements: {
            nodes: [ <node>, ... ],
            edges: [ <edge>, ... ]
        },

        // 캔버스 상의 렌더링 위치를 알려면 pan, zoom 필요
        pan: {x: 624.67, y: 221.38 },   // viewport 위치
        zoom: 0.16                      // 확대 배율
    }
 */
        @Override
        public void serialize(final AgensGraph graph, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            System.out.println(" !! serialize start : "+graph.toString());

            jsonGenerator.writeStartObject();       // depth=0
            // cytoscape.js :: elements
            jsonGenerator.writeFieldName("scratch");
            jsonGenerator.writeStartObject();       // depth=1
            jsonGenerator.writeStringField("_name", graph.name());
            jsonGenerator.writeStringField("_pan", graph.variables().get("pan").orElse("{x:0,y:0}").toString());
            jsonGenerator.writeStringField("_zoom", graph.variables().get("zoom").orElse("0.1").toString());
            jsonGenerator.writeEndObject();       // depth=1

            // cytoscape.js :: elements
            jsonGenerator.writeFieldName("elements");
            jsonGenerator.writeStartObject();       // depth=1

            jsonGenerator.writeFieldName("nodes");
            jsonGenerator.writeStartArray();
            final Iterator<Vertex> vertices = graph.vertices();
            while (vertices.hasNext()) {
                serializerProvider.defaultSerializeValue((AgensVertex)vertices.next(), jsonGenerator);
            }
            jsonGenerator.writeEndArray();

            jsonGenerator.writeFieldName("edges");
            jsonGenerator.writeStartArray();
            // java.util.ConcurrentModificationException
            final Iterator<Edge> edges = graph.edges();
            while (edges.hasNext()) {
                serializerProvider.defaultSerializeValue((AgensEdge)edges.next(), jsonGenerator);
            }
            jsonGenerator.writeEndArray();

            jsonGenerator.writeEndObject();       // depth=1
/*
            // cytoscape.js :: pan (position)
            Optional<String> pan = graph.variables().get("pan");
            if( pan.isPresent() ){
                jsonGenerator.writeStringField("pan", pan.get());
            }
            // cytoscape.js :: zoom (1.0 ~ 0.1)
            Optional<String> zoom = graph.variables().get("zoom");
            if( zoom.isPresent() ){
                jsonGenerator.writeStringField("zoom", zoom.get());
            }
 */
            jsonGenerator.writeEndObject();       // depth=0
        }

        @Override
        public void serializeWithType(final AgensGraph graph, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.CLASS, AgensGraph.class.getName());
            jsonGenerator.writeStringField("name", graph.name());

            jsonGenerator.writeFieldName(GraphSONTokens.VERTICES);
            jsonGenerator.writeStartArray();
            jsonGenerator.writeString(ArrayList.class.getName());
            jsonGenerator.writeStartArray();

            final Iterator<Vertex> vertices = graph.vertices();
            while (vertices.hasNext()) {
                GraphSONUtil.writeWithType((AgensVertex)vertices.next(), jsonGenerator, serializerProvider, typeSerializer);
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndArray();

            jsonGenerator.writeFieldName(GraphSONTokens.EDGES);
            jsonGenerator.writeStartArray();
            jsonGenerator.writeString(ArrayList.class.getName());
            jsonGenerator.writeStartArray();

            final Iterator<Edge> edges = graph.edges();
            while (edges.hasNext()) {
                GraphSONUtil.writeWithType((AgensEdge)edges.next(), jsonGenerator, serializerProvider, typeSerializer);
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndArray();

            jsonGenerator.writeEndObject();
        }
    }

    /**
     * Deserializes the edge list format.
     */
    static class AgensGraphJacksonDeserializer extends StdDeserializer<AgensGraph> {
        public AgensGraphJacksonDeserializer() {
            super(AgensGraph.class);
        }

        @Override
        public AgensGraph deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException {
            final AgensGraph graph = AgensGraph.open(baseGraph);

            final List<Map<String, Object>> edges;
            final List<Map<String, Object>> vertices;

            if (!jsonParser.getCurrentToken().isStructStart()) {
                if (!jsonParser.getCurrentName().equals(GraphSONTokens.VERTICES))
                    throw new IOException(String.format("Expected a '%s' key", GraphSONTokens.VERTICES));

                jsonParser.nextToken();
                vertices = (List<Map<String, Object>>) deserializationContext.readValue(jsonParser, ArrayList.class);
                jsonParser.nextToken();

                if (!jsonParser.getCurrentName().equals(GraphSONTokens.EDGES))
                    throw new IOException(String.format("Expected a '%s' key", GraphSONTokens.EDGES));

                jsonParser.nextToken();
                edges = (List<Map<String, Object>>) deserializationContext.readValue(jsonParser, ArrayList.class);
            }
            else {
                final Map<String, Object> graphData = deserializationContext.readValue(jsonParser, HashMap.class);
                vertices = (List<Map<String,Object>>) graphData.get(GraphSONTokens.VERTICES);
                edges = (List<Map<String,Object>>) graphData.get(GraphSONTokens.EDGES);
            }

            for (Map<String, Object> vertexData : vertices) {
                final DetachedVertex detached = new DetachedVertex(
                        vertexData.get(GraphSONTokens.ID)
                        , vertexData.get(GraphSONTokens.LABEL).toString()
                        , (Map<String,Object>) vertexData.get(GraphSONTokens.PROPERTIES));
                detached.attach(Attachable.Method.getOrCreate(graph));
            }

            for (Map<String, Object> edgeData : edges) {
                final DetachedEdge detached = new DetachedEdge(
                        edgeData.get(GraphSONTokens.ID)
                        , edgeData.get(GraphSONTokens.LABEL).toString()
                        , (Map<String,Object>) edgeData.get(GraphSONTokens.PROPERTIES)
                        , edgeData.get(GraphSONTokens.OUT)
                        , edgeData.get(GraphSONTokens.OUT_LABEL).toString()
                        , edgeData.get(GraphSONTokens.IN)
                        , edgeData.get(GraphSONTokens.IN_LABEL).toString());
                detached.attach(Attachable.Method.getOrCreate(graph));
            }

            return graph;
        }
    }

    //////////////////////////////////////////////////


    final static class AgensVertexPropertyJacksonSerializer extends StdSerializer<AgensVertexProperty> {

        private final boolean normalize;

        public AgensVertexPropertyJacksonSerializer(final boolean normalize) {
            super(AgensVertexProperty.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final AgensVertexProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            serializerVertexProperty(property, jsonGenerator, serializerProvider, null, normalize, true);
        }

        @Override
        public void serializeWithType(final AgensVertexProperty property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            serializerVertexProperty(property, jsonGenerator, serializerProvider, typeSerializer, normalize, true);
        }
    }

    final static class AgensPropertyJacksonSerializer extends StdSerializer<AgensProperty> {

        public AgensPropertyJacksonSerializer() {
            super(AgensProperty.class);
        }

        @Override
        public void serialize(final AgensProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final AgensProperty property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator, serializerProvider, typeSerializer);
        }

        private static void ser(final AgensProperty property, final JsonGenerator jsonGenerator,
                                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            serializerProvider.defaultSerializeField(GraphSONTokens.KEY, property.key(), jsonGenerator);
            serializerProvider.defaultSerializeField(GraphSONTokens.VALUE, property.value(), jsonGenerator);
            jsonGenerator.writeEndObject();
        }
    }

    final static class ElasticPropertyJacksonSerializer extends StdSerializer<BaseProperty> {

        public ElasticPropertyJacksonSerializer() {
            super(BaseProperty.class);
        }

        @Override
        public void serialize(final BaseProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final BaseProperty property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator, serializerProvider, typeSerializer);
        }

        private static void ser(final BaseProperty property, final JsonGenerator jsonGenerator,
                                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            serializerProvider.defaultSerializeField(GraphSONTokens.KEY, property.key(), jsonGenerator);
            serializerProvider.defaultSerializeField(GraphSONTokens.VALUE, property.value(), jsonGenerator);
            jsonGenerator.writeEndObject();
        }
    }

    final static class AgensEdgeJacksonSerializer extends StdSerializer<AgensEdge> {

        private final boolean normalize;

        public AgensEdgeJacksonSerializer(final boolean normalize) {
            super(AgensEdge.class);
            this.normalize = normalize;
        }

/*
    edge: {
        group: "edges"                  // type: "edge"
        data:
            id: "11.923"
            label: "orders"
            graph: "default"
            properties: {discount: 0.2, quantity: 5, unitprice: 38}
            size: 1
            source: "9.349"
            target: "3.56"

        // 스타일 지정을 하려면 classes 이용
        classes: ""                     // className 리스트 (공백 구분)
    }
 */
        @Override
        public void serialize(final AgensEdge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(edge, jsonGenerator, serializerProvider, null);
        }
        @Override
        public void serializeWithType(final AgensEdge edge, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(edge, jsonGenerator, serializerProvider, typeSerializer);
        }

        private void ser(final AgensEdge edge, final JsonGenerator jsonGenerator,
                         final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            jsonGenerator.writeStringField("group", "edges");

            jsonGenerator.writeFieldName("data");
            jsonGenerator.writeStartObject();

            GraphSONUtil.writeWithType(GraphSONTokens.ID, edge.id(), jsonGenerator, serializerProvider, typeSerializer);
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, edge.label());
            jsonGenerator.writeStringField("datasource", ((AgensGraph)edge.graph()).name());
            jsonGenerator.writeStringField("source", edge.getBaseEdge().getSrc());
            jsonGenerator.writeStringField("target", edge.getBaseEdge().getDst());
            writeProperties(edge, jsonGenerator, serializerProvider, typeSerializer);

            jsonGenerator.writeEndObject();

            // for cytoscape.js :: scratch
            jsonGenerator.writeObjectFieldStart("scratch");
            jsonGenerator.writeEndObject();

            jsonGenerator.writeEndObject();
        }

        private void writeProperties(final AgensEdge edge, final JsonGenerator jsonGenerator,
                                     final SerializerProvider serializerProvider,
                                     final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
            List<BaseProperty> properties = new ArrayList<>(edge.getBaseEdge().properties());
            Iterator<BaseProperty> iter = properties.iterator();
            while( iter.hasNext() ){
                BaseProperty p = iter.next();
                GraphSONUtil.writeWithType(p.key(), p.value(), jsonGenerator, serializerProvider, typeSerializer);
            }
            jsonGenerator.writeEndObject();
        }

    }

    final static class AgensVertexJacksonSerializer extends StdSerializer<AgensVertex> {

        private final boolean normalize;

        public AgensVertexJacksonSerializer(final boolean normalize) {
            super(AgensVertex.class);
            this.normalize = normalize;
        }

/*
    node: {
        group: "nodes"                  // type: "vertex"
        data:
            id: "9.673"
            label: "order"
            graphId: ""
            properties: {shippostalcode: "CO7 6JX", shipcountry: "UK", orderid: 10920, freight: 29.61, …}
            size: 1
            outgoers: [ vlabel1: [], vlable2: [], ... ]
            incomers: [ vlabel3: [], vlable4: [], ... ]

        // 스타일 지정을 하려면 classes 이용
        classes: ""                     // className 리스트 (공백 구분)
        position: {x: 0, y: 0}          // 위치 (nodes만 필요)
    }
 */
        @Override
        public void serialize(final AgensVertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(vertex, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final AgensVertex vertex, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(vertex, jsonGenerator, serializerProvider, typeSerializer);

        }

        private void ser(final AgensVertex vertex, final JsonGenerator jsonGenerator,
                         final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException {
            jsonGenerator.writeStartObject();           // {
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());

            jsonGenerator.writeStringField("group", "nodes");

            jsonGenerator.writeFieldName("data");
            jsonGenerator.writeStartObject();           // data : {

            GraphSONUtil.writeWithType(GraphSONTokens.ID, vertex.id(), jsonGenerator, serializerProvider, typeSerializer);
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, vertex.label());
            jsonGenerator.writeStringField("datasource", ((AgensGraph)vertex.graph()).name());

            // for expand : out-direction
            jsonGenerator.writeObjectFieldStart("outgoers");
            Iterator<Vertex> outVlist = vertex.vertices(Direction.IN);
            if( outVlist.hasNext() ) {
                Spliterator<Vertex> spliterator = Spliterators.spliteratorUnknownSize(outVlist, 0);
                Map<String,List<Vertex>> groupByLabel = StreamSupport.stream(spliterator, false)
                        .collect(Collectors.groupingBy(Vertex::label, TreeMap::new, Collectors.toList()));
                for( String label : groupByLabel.keySet() ){
                    jsonGenerator.writeArrayFieldStart(label);
                    for( Vertex outgoer : groupByLabel.get(label)){
                        GraphSONUtil.writeWithType(outgoer.id(), jsonGenerator, serializerProvider, typeSerializer);
                    }
                    jsonGenerator.writeEndArray();
                }
            }
            jsonGenerator.writeEndObject();

            // for expand : in-direction
            jsonGenerator.writeObjectFieldStart("incomers");
            Iterator<Vertex> inVlist = vertex.vertices(Direction.OUT);
            if( inVlist.hasNext() ) {
                Spliterator<Vertex> spliterator = Spliterators.spliteratorUnknownSize(inVlist, 0);
                Map<String,List<Vertex>> groupByLabel = StreamSupport.stream(spliterator, false)
                        .collect(Collectors.groupingBy(Vertex::label, TreeMap::new, Collectors.toList()));
                for( String label : groupByLabel.keySet() ){
                    jsonGenerator.writeArrayFieldStart(label);
                    for( Vertex incomer : groupByLabel.get(label)){
                        GraphSONUtil.writeWithType(incomer.id(), jsonGenerator, serializerProvider, typeSerializer);
                    }
                    jsonGenerator.writeEndArray();
                }
            }
            jsonGenerator.writeEndObject();

/*
 ** 참고 : StarGraph.java - vertices() 함수
 **
        return null == this.starVertex ?
                Collections.emptyIterator() :
                Stream.concat(
                        Stream.of(this.starVertex),
                        Stream.concat(
                                this.starVertex.outEdges.values()
                                        .stream()
                                        .flatMap(List::stream)
                                        .map(Edge::inVertex),
                                this.starVertex.inEdges.values()
                                        .stream()
                                        .flatMap(List::stream)
                                        .map(Edge::outVertex)))
                        .filter(vertex -> ElementHelper.idExists(vertex.id(), vertexIds))
                        .iterator();
 */

            writeProperties(vertex, jsonGenerator, serializerProvider, typeSerializer);

            jsonGenerator.writeEndObject();

            // for cytoscape.js :: scratch
            jsonGenerator.writeObjectFieldStart("scratch");
            jsonGenerator.writeEndObject();

            jsonGenerator.writeEndObject();
        }

        private void writeProperties(final AgensVertex vertex, final JsonGenerator jsonGenerator,
                                     final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());

            final List<String> keys = normalize ?
                    IteratorUtils.list(vertex.keys().iterator(), Comparator.naturalOrder()) : new ArrayList<>(vertex.keys());
            for (String key : keys) {
                final Iterator<VertexProperty<Object>> vertexProperties = normalize ?
                        IteratorUtils.list(vertex.properties(key), Comparators.PROPERTY_COMPARATOR).iterator()
                        : vertex.properties(key);

                if (vertexProperties.hasNext()) {
                    final VertexProperty property = vertexProperties.next();
                    if( !vertexProperties.hasNext() ) {             // only one
                        GraphSONUtil.writeWithType(key, property.value(), jsonGenerator, serializerProvider, typeSerializer);
                    }
                    else {
                        jsonGenerator.writeArrayFieldStart(key);    // more than one
                        while (vertexProperties.hasNext()) {
                            serializerVertexProperty(vertexProperties.next(), jsonGenerator, serializerProvider, typeSerializer, normalize, false);
                        }
                        jsonGenerator.writeEndArray();
                    }
                }
            }

            jsonGenerator.writeEndObject();
        }

    }

    ////////////////////////////////////////////

    private static void serializerVertexProperty(final VertexProperty property, final JsonGenerator jsonGenerator,
                                                 final SerializerProvider serializerProvider,
                                                 final TypeSerializer typeSerializer, final boolean normalize,
                                                 final boolean includeLabel) throws IOException {
        jsonGenerator.writeStartObject();
        if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
        GraphSONUtil.writeWithType(GraphSONTokens.ID, property.id(), jsonGenerator, serializerProvider, typeSerializer);
        GraphSONUtil.writeWithType(GraphSONTokens.VALUE, property.value(), jsonGenerator, serializerProvider, typeSerializer);
        if (includeLabel) jsonGenerator.writeStringField(GraphSONTokens.LABEL, property.label());
        tryWriteMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
        jsonGenerator.writeEndObject();

    }

    private static void tryWriteMetaProperties(final VertexProperty property, final JsonGenerator jsonGenerator,
                                               final SerializerProvider serializerProvider,
                                               final TypeSerializer typeSerializer, final boolean normalize) throws IOException {
        // when "detached" you can't check features of the graph it detached from so it has to be
        // treated differently from a regular VertexProperty implementation.
        if (property instanceof DetachedVertexProperty) {
            // only write meta properties key if they exist
            if (property.properties().hasNext()) {
                writeMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
            }
        } else {
            // still attached - so we can check the features to see if it's worth even trying to write the
            // meta properties key
            if (property.graph().features().vertex().supportsMetaProperties() && property.properties().hasNext()) {
                writeMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
            }
        }
    }

    private static void writeMetaProperties(final VertexProperty property, final JsonGenerator jsonGenerator,
                                            final SerializerProvider serializerProvider,
                                            final TypeSerializer typeSerializer, final boolean normalize) throws IOException {
        jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
        if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
        final Iterator<Property<Object>> metaProperties = normalize ?
                IteratorUtils.list(( Iterator<Property<Object>>) property.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : property.properties();
        while (metaProperties.hasNext()) {
            final Property<Object> metaProperty = metaProperties.next();
            GraphSONUtil.writeWithType(metaProperty.key(), metaProperty.value(), jsonGenerator, serializerProvider, typeSerializer);
        }
        jsonGenerator.writeEndObject();
    }

}
