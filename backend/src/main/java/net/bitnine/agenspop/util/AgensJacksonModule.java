package net.bitnine.agenspop.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.elasticgraph.model.ElasticElement;
import net.bitnine.agenspop.elasticgraph.util.ElasticHelper;
import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensProperty;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.graph.structure.AgensVertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;

import java.io.IOException;
import java.util.*;

public final class AgensJacksonModule extends SimpleModule {

    private static String SCRATCH_KEY_PREFIX = "_$$";

    public AgensJacksonModule() {
        super("agensjackson-1.0");

        // register serializers
        addSerializer(BaseProperty.class, new BasePropertyJacksonSerializer());
        addSerializer(AgensProperty.class, new AgensPropertyJacksonSerializer());
        addSerializer(AgensVertexProperty.class, new AgensVertexPropertyJacksonSerializer());

        addSerializer(BaseVertex.class, new BaseVertexJacksonSerializer());
        addSerializer(AgensVertex.class, new AgensVertexJacksonSerializer());

        addSerializer(BaseEdge.class, new BaseEdgeJacksonSerializer());
        addSerializer(AgensEdge.class, new AgensEdgeJacksonSerializer());
    }

    static void writeWithType(final String key, final Object object
            , final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider
    ) throws IOException {
        JsonSerializer<Object> serializer = serializerProvider.findTypedValueSerializer(object.getClass(), true, (BeanProperty)null);
        if (key != null && !key.isEmpty()) {
            jsonGenerator.writeFieldName(key);
        }
        serializer.serialize(object, jsonGenerator, serializerProvider);
    }

    static List<String> writeProperties(final BaseElement element, final JsonGenerator jsonGenerator
            , final SerializerProvider serializerProvider
    ) throws IOException {
        jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);

        final List<String> scratchKeys = new ArrayList<>();
        final List<String> keys = new ArrayList<>(element.keys());
        Collections.sort(keys);     // sort keys by asc
        for (String key : keys) {
            if( key.startsWith(SCRATCH_KEY_PREFIX) ){
                scratchKeys.add(key);
                continue;
            }
            final BaseProperty property = element.getProperty(key);
            if( property != null && property.canRead() ) {
                final Object value = property.value();
                writeWithType(key, value, jsonGenerator, serializerProvider);
            }
        }

        jsonGenerator.writeEndObject();
        return scratchKeys;
    }

    static void writeScratch(final BaseElement element, final JsonGenerator jsonGenerator
            , final SerializerProvider serializerProvider, final List<String> keys
    ) throws IOException {
        jsonGenerator.writeObjectFieldStart("scratch");
        // writeWithType(BaseElement.createdTag, ((ElasticElement)element).getCreated(), jsonGenerator, serializerProvider);
        for (String key : keys) {
            final BaseProperty property = element.getProperty(key);
            if( property != null && property.canRead() ) {
                final Object value = property.value();
                writeWithType(key, value, jsonGenerator, serializerProvider);
            }
        }
        jsonGenerator.writeEndObject();
    }

    /////////////////////////////////////////////////////
    // for PROPERTY

    static void propertySerialize(final BaseProperty property, final JsonGenerator jsonGenerator
            , final SerializerProvider serializerProvider
    ) throws IOException {
        jsonGenerator.writeStartObject();       // {
        serializerProvider.defaultSerializeField(GraphSONTokens.KEY, property.key(), jsonGenerator);
        serializerProvider.defaultSerializeField(GraphSONTokens.VALUE, property.value(), jsonGenerator);
        jsonGenerator.writeEndObject();         // }
    }

    final static class BasePropertyJacksonSerializer extends JsonSerializer<BaseProperty> {
        // public BasePropertyJacksonSerializer() { super(BaseProperty.class); }

        @Override
        public void serialize(final BaseProperty property, final JsonGenerator jsonGenerator
                , final SerializerProvider serializerProvider
        ) throws IOException {
            propertySerialize(property, jsonGenerator, serializerProvider);
        }
    }

    final static class AgensPropertyJacksonSerializer extends JsonSerializer<AgensProperty> {
        // public AgensPropertyJacksonSerializer() { super(AgensProperty.class); }

        @Override
        public void serialize(final AgensProperty property, final JsonGenerator jsonGenerator
                , final SerializerProvider serializerProvider
        ) throws IOException {
            propertySerialize(property.getBaseProperty(), jsonGenerator, serializerProvider);
        }
    }

    final static class AgensVertexPropertyJacksonSerializer extends JsonSerializer<AgensVertexProperty> {
        // public AgensVertexPropertyJacksonSerializer() { super(AgensVertexProperty.class); }

        @Override
        public void serialize(final AgensVertexProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            propertySerialize(property.getBaseVertexProperty(), jsonGenerator, serializerProvider);
        }
    }

    /////////////////////////////////////////////////////
    // for VERTEX

    // **NOTE: JsonSerializer – serializeWithType 메소드를 언제 구현해야합니까?
    //   ==> (@JsonTypeInfo 또는 직접 입력된) 다형성 유형 처리를 지원해야하는 경우 필요
    // https://codeday.me/ko/qa/20190708/1002036.html

    static void vertexSerialize(final BaseVertex vertex, final JsonGenerator jsonGenerator
            , final SerializerProvider serializerProvider
    ) throws IOException {
        jsonGenerator.writeStartObject();           // {
        jsonGenerator.writeStringField("group", "nodes");

        jsonGenerator.writeFieldName("data");
        jsonGenerator.writeStartObject();           // data : {
        // id, label, datasource
        jsonGenerator.writeStringField("datasource", vertex.getDatasource());
        jsonGenerator.writeStringField(GraphSONTokens.ID, vertex.getId());
        jsonGenerator.writeStringField(GraphSONTokens.LABEL, vertex.getLabel());
        jsonGenerator.writeStringField(GraphSONTokens.NAME, vertex.getName());
        // properties
        List<String> scratchKeys = writeProperties(vertex, jsonGenerator, serializerProvider);
        jsonGenerator.writeEndObject();             // data : }

        // for cytoscape.js :: scratch
        writeScratch(vertex, jsonGenerator, serializerProvider, scratchKeys);

        jsonGenerator.writeEndObject();             // }
    }

    final static class BaseVertexJacksonSerializer extends JsonSerializer<BaseVertex> {
        // public BaseVertexJacksonSerializer() { super(BaseVertex.class); }

        @Override
        public void serialize(final BaseVertex vertex, final JsonGenerator jsonGenerator
                , final SerializerProvider serializerProvider
        ) throws IOException {
            vertexSerialize(vertex, jsonGenerator, serializerProvider);
        }
    }

    final static class AgensVertexJacksonSerializer extends JsonSerializer<AgensVertex> {
        // public AgensVertexJacksonSerializer() { super(BaseVertex.class); }

        @Override
        public void serialize(final AgensVertex vertex, final JsonGenerator jsonGenerator
                , final SerializerProvider serializerProvider
        ) throws IOException {
            vertexSerialize(vertex.getBaseVertex(), jsonGenerator, serializerProvider);
        }
    }

    /////////////////////////////////////////////////////
    // for EDGE

    static void edgeSerialize(final BaseEdge edge, final JsonGenerator jsonGenerator
            , final SerializerProvider serializerProvider
    ) throws IOException {
        jsonGenerator.writeStartObject();           // {
        jsonGenerator.writeStringField("group", "edges");

        jsonGenerator.writeFieldName("data");
        jsonGenerator.writeStartObject();           // data : {
        // id, label, datasource
        jsonGenerator.writeStringField("datasource", edge.getDatasource());
        writeWithType(GraphSONTokens.ID, edge.getId(), jsonGenerator, serializerProvider);
        jsonGenerator.writeStringField(GraphSONTokens.LABEL, edge.getLabel());
        // source, target
        jsonGenerator.writeStringField("source", edge.getSrc());
        jsonGenerator.writeStringField("target", edge.getDst());
        // properties
        List<String> scratchKeys = writeProperties(edge, jsonGenerator, serializerProvider);
        jsonGenerator.writeEndObject();             // data : }

        // for cytoscape.js :: scratch
        writeScratch(edge, jsonGenerator, serializerProvider, scratchKeys);

        jsonGenerator.writeEndObject();             // }
    }

    final static class BaseEdgeJacksonSerializer extends JsonSerializer<BaseEdge> {
        // public BaseEdgeJacksonSerializer() { super(BaseEdge.class); }

        @Override
        public void serialize( final BaseEdge edge, final JsonGenerator jsonGenerator
                , final SerializerProvider serializerProvider
        ) throws IOException {
            edgeSerialize(edge, jsonGenerator, serializerProvider);
        }
    }

    final static class AgensEdgeJacksonSerializer extends JsonSerializer<AgensEdge> {
        // public AgensEdgeJacksonSerializer() { super(AgensEdge.class); }

        @Override
        public void serialize( final AgensEdge edge, final JsonGenerator jsonGenerator
                , final SerializerProvider serializerProvider
        ) throws IOException {
            edgeSerialize(edge.getBaseEdge(), jsonGenerator, serializerProvider);
        }
    }

}
