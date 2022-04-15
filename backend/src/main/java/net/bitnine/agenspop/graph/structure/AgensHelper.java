package net.bitnine.agenspop.graph.structure;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import net.bitnine.agenspop.basegraph.BaseGraphAPI;
import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.elasticgraph.util.ElasticHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public final class AgensHelper {

    private static final String NOT_FOUND_EXCEPTION = "NotFoundException";

    private AgensHelper() { }

    // Function to filter Stream by from and to as datetime range
    public static <T> Stream<T> filterStreamByDateRange(Stream<T> stream, LocalDateTime from, LocalDateTime to) {
        return stream.filter(e->{
            return e instanceof AgensVertex || e instanceof AgensEdge;
        }).filter(e->{
            BaseElement element = ((AgensElement)e).getBaseElement();
            if( element.keys().contains(BaseElement.timestampTag) ){
                LocalDateTime created = ElasticHelper.str2date(element.getProperty(BaseElement.timestampTag).valueOf());
                if( created.isAfter(from) && created.isBefore(to) )
                    return true;
            }
            return false;
        });
    }

    // Function to get the Stream
    public static <T> Stream<T> getStreamFromIterator(Iterator<T> iterator) {
        // Convert the iterator to Spliterator
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
        // Get a Sequential Stream from spliterator
        return StreamSupport.stream(spliterator, false);
    }

    public static void attachProperties(final BaseGraphAPI api, final BaseElement element, final Object... propertyKeyValues) {
        if (api == null) throw Graph.Exceptions.argumentCanNotBeNull("baseGraphAPI");
        if (element == null) throw Graph.Exceptions.argumentCanNotBeNull("baseElement");
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label)){
                BaseProperty propertyBase = api.createProperty((String)propertyKeyValues[i], propertyKeyValues[i + 1]);
                element.setProperty(propertyBase);
            }
        }
    }
    public static void attachProperties(final Vertex vertex, final Object... propertyKeyValues) {
        if (vertex == null) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        BaseGraphAPI api = ((AgensVertex)vertex).graph.getBaseGraph();
        BaseElement baseElement = ((AgensVertex)vertex).baseElement;
        attachProperties(api, baseElement, propertyKeyValues);
    }
    public static void attachProperties(final Edge edge, final Object... propertyKeyValues) {
        if (edge == null) throw Graph.Exceptions.argumentCanNotBeNull("edge");
        BaseGraphAPI api = ((AgensEdge)edge).graph.getBaseGraph();
        BaseElement baseElement = ((AgensEdge)edge).baseElement;
        attachProperties(api, baseElement, propertyKeyValues);
    }

    protected static Vertex addVertex(final AgensGraph graph, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        final Vertex vertex;    // if throw exception, then null

        Object idValue = graph.vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        if (null != idValue) {
            if (graph.api.existsVertex(idValue.toString()))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.vertexIdManager.getNextId(graph);
        }

        graph.tx().readWrite();
        final BaseVertex baseVertex = graph.api.createVertex(graph.name(), idValue.toString(), label);
        AgensHelper.attachProperties(graph.api, baseVertex, keyValues);
        graph.api.saveVertex(baseVertex);     // write to elasticsearch index

        return new AgensVertex(graph, baseVertex);
    }

    protected static Edge addEdge(final AgensGraph graph, final AgensVertex outVertex, final AgensVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        final Edge edge;    // if throw exception, then null

        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        if (null != idValue) {
            if (graph.api.existsEdge(idValue.toString()))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.edgeIdManager.getNextId(graph);
        }

        graph.tx().readWrite();
        final BaseEdge baseEdge = graph.api.createEdge(graph.name(), idValue.toString(), label
                , outVertex.id().toString(), inVertex.id().toString());
        AgensHelper.attachProperties(graph.api, baseEdge, keyValues);
        graph.api.saveEdge(baseEdge);     // write to elasticsearch index

        return new AgensEdge(graph, baseEdge);
    }

    //////////////////////////////////////////

    public static boolean isDeleted(final BaseVertex vertex) {
        try {
            return vertex.notexists();
        } catch (final RuntimeException e) {
            if (isNotFound(e))
                return true;
            else
                throw e;
        }
    }

    public static boolean isNotFound(final RuntimeException ex) {
        return ex.getClass().getSimpleName().equals(NOT_FOUND_EXCEPTION);
    }

    // **NOTE: 최적화된 hasContainer 는 삭제!!
    //      ==> hasContainer.test(element) 에서 실패 방지
    public static Map<String, Object> optimizeHasContainers(List<HasContainer> hasContainers){
        Map<String, Object> optimizedParams = new HashMap<>();
        Iterator<HasContainer> iter = hasContainers.iterator();
        while( iter.hasNext() ){
            HasContainer c = iter.next();
            // for DEBUG
            // System.out.println(String.format("**optimizeHasContainers: %s.%s=%s (%s)", c.getKey(), c.getBiPredicate(), c.getValue(), c.getValue().getClass().getSimpleName()));

            // hasId(id...)
            if( c.getKey().equals("~id") ){
                List<String> ids = new ArrayList<>();
                if( c.getBiPredicate().toString().equals("eq") ){
                    ids.add( (String)c.getValue() );
                    optimizedParams.put("ids", ids);
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    ids.addAll( valueList.stream().map(Object::toString).collect(Collectors.toList()) );
                    optimizedParams.put("ids", ids);
                    iter.remove();      // remove hasContainer!!
                }
            }
            // hasLabel(label...)
            else if( c.getKey().equals("~label") ){
                if( c.getBiPredicate().toString().equals("eq") ){
                    optimizedParams.put("label", (String)c.getValue());
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    List<String> labels = new ArrayList<>();
                    labels.addAll( valueList.stream().map(Object::toString).collect(Collectors.toList()) );
                    optimizedParams.put("label", (String)c.getValue());
                    iter.remove();      // remove hasContainer!!
                }
            }
            // hasKey(key...)
            else if( c.getKey().equals("~key") ){
                if( c.getBiPredicate().toString().equals("eq") ){
                    optimizedParams.put("key", (String)c.getValue());
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("neq") ){
                    optimizedParams.put("keyNot", (String)c.getValue());
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    List<String> keys = new ArrayList<>();
                    keys.addAll( valueList.stream().map(Object::toString).collect(Collectors.toList()) );
                    optimizedParams.put("keys", keys);
                    iter.remove();      // remove hasContainer!!
                }
            }
            // hasValue(value...)
            else if( c.getKey().equals("~value") ){
                List<String> values = new ArrayList<>();
                if( c.getBiPredicate().toString().equals("eq") ){
                    values.add( c.getValue().toString() );
                    optimizedParams.put("values", values);
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    values.addAll( valueList.stream().map(Object::toString).collect(Collectors.toList()) );
                    optimizedParams.put("values", values);
                    iter.remove();      // remove hasContainer!!
                }
            }
            // has(key, value)
            else {
                if( c.getKey() != null ){
                    Map<String, String> kvPairs = optimizedParams.containsKey("kvPairs") ?
                            (Map<String, String>) optimizedParams.get("kvPairs")
                            : new HashMap<>();
                    if( c.getBiPredicate().toString().equals("eq") ){
                        kvPairs.put(c.getKey(), c.getValue().toString());
                        if( !optimizedParams.containsKey("kvPairs") ) optimizedParams.put("kvPairs", kvPairs);
                        iter.remove();      // remove hasContainer!!
                    }
                }
            }
        }

        // for DEBUG
        // System.out.println("  ==> "+optimizedParams.keySet().stream()
        //         .map(key -> key + "=" + optimizedParams.get(key))
        //         .collect(Collectors.joining(", ", "{", "}")) );
        return optimizedParams;
    }

    public static Collection<Vertex> verticesWithHasContainers(AgensGraph graph, Map<String, Object> optimizedParams) {
        if( optimizedParams == null || optimizedParams.size() == 0 ) return Collections.EMPTY_LIST;

        String label = !optimizedParams.containsKey("label") ? null : optimizedParams.get("label").toString();
        List<String> labelParams = !optimizedParams.containsKey("labels") ? null : (List<String>) optimizedParams.get("labels");
        String key = !optimizedParams.containsKey("key") ? null : optimizedParams.get("key").toString();
        String keyNot = !optimizedParams.containsKey("keyNot") ? null : optimizedParams.get("keyNot").toString();
        List<String> keyParams = !optimizedParams.containsKey("keys") ? null : (List<String>) optimizedParams.get("keys");
        List<String> valueParams = !optimizedParams.containsKey("values") ? null : (List<String>) optimizedParams.get("values");
        Map<String, String> kvPairs = !optimizedParams.containsKey("kvPairs") ? null : (Map<String, String>) optimizedParams.get("kvPairs");

        // Parameters
        String[] labels = labelParams==null ? null : labelParams.stream().toArray(String[]::new);
        String[] keys = keyParams==null ? null : keyParams.stream().toArray(String[]::new);
        String[] values = valueParams==null ? null : valueParams.stream().toArray(String[]::new);

        // for DEBUG
//        System.out.println("V.hasContainers :: datasource => "+graph.name());
//        System.out.println("  , label => "+label);
//        System.out.println("  , labels => "+(labels==null ? "null" : String.join(",", labels)));
//        System.out.println("  , key => "+key);
//        System.out.println("  , keyNot => "+keyNot);
//        System.out.println("  , keys => "+(keys==null ? "null" : String.join(",", keys)));
//        System.out.println("  , values => "+(values==null ? "null" : String.join(",", values)));
//        System.out.println("  , kvPairs => "+(kvPairs==null ? "null" : kvPairs.entrySet().stream().map(r->r.getKey()+"="+r.getValue()).collect(Collectors.joining(","))));

        if( optimizedParams.size() == 1 ){
            if( label != null )
                return graph.api.findVertices(graph.name(), label)
                    .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
            else if( labels != null )
                return graph.api.findVertices(graph.name(), labels)
                        .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
            else if( key != null )
                return graph.api.findVertices(graph.name(), key, false)
                        .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
            else if( keyNot != null )
                return graph.api.findVertices(graph.name(), keyNot, true)
                        .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
            else if( keys != null )
                return graph.api.findVerticesWithKeys(graph.name(), keys)
                        .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
            else if( values != null )
                return graph.api.findVerticesWithValues(graph.name(), values)
                        .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
            else if( kvPairs != null && kvPairs.size() == 1 )
                return graph.api.findVerticesWithKeyValues(graph.name(), null, kvPairs)
                        .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
        }
        else if( optimizedParams.size() == 2 && label != null && kvPairs != null ){
            return graph.api.findVerticesWithKeyValues(graph.name(), label, kvPairs)
                    .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
        }
        // else
        return graph.api.findVertices(graph.name()
                , label, labels, key, keyNot, keys, values, kvPairs)
                .map(r -> (Vertex) new AgensVertex(graph, r)).collect(Collectors.toList());
    }

    public static Collection<Edge> edgesWithHasContainers(AgensGraph graph, Map<String, Object> optimizedParams) {
        String label = !optimizedParams.containsKey("label") ? null : optimizedParams.get("label").toString();
        List<String> labelParams = !optimizedParams.containsKey("labels") ? null : (List<String>) optimizedParams.get("labels");
        String key = !optimizedParams.containsKey("key") ? null : optimizedParams.get("key").toString();
        String keyNot = !optimizedParams.containsKey("keyNot") ? null : optimizedParams.get("keyNot").toString();
        List<String> keyParams = !optimizedParams.containsKey("keys") ? null : (List<String>) optimizedParams.get("keys");
        List<String> valueParams = !optimizedParams.containsKey("values") ? null : (List<String>) optimizedParams.get("values");
        Map<String, String> kvPairs = !optimizedParams.containsKey("kvPairs") ? null : (Map<String, String>) optimizedParams.get("kvPairs");

        // Parameters
        String[] labels = labelParams==null ? null : labelParams.stream().toArray(String[]::new);
        String[] keys = keyParams==null ? null : keyParams.stream().toArray(String[]::new);
        String[] values = valueParams==null ? null : valueParams.stream().toArray(String[]::new);

        // for DEBUG
//        System.out.println("E.hasContainers :: datasource => "+graph.name());
//        System.out.println("  , label => "+label);
//        System.out.println("  , labels => "+(labels==null ? "null" : String.join(",", labels)));
//        System.out.println("  , key => "+key);
//        System.out.println("  , keyNot => "+keyNot);
//        System.out.println("  , keys => "+(keys==null ? "null" : String.join(",", keys)));
//        System.out.println("  , values => "+(values==null ? "null" : String.join(",", values)));
//        System.out.println("  , kvPairs => "+(kvPairs==null ? "null" : kvPairs.entrySet().stream().map(r->r.getKey()+"="+r.getValue()).collect(Collectors.joining(","))));

        if( optimizedParams.size() == 1 ){
            if( label != null )
                return graph.api.findEdges(graph.name(), label)
                        .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
            else if( labels != null )
                return graph.api.findEdges(graph.name(), labels)
                        .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
            else if( key != null )
                return graph.api.findEdges(graph.name(), key, false)
                        .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
            else if( keyNot != null )
                return graph.api.findEdges(graph.name(), keyNot, true)
                        .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
            else if( keys != null )
                return graph.api.findEdgesWithKeys(graph.name(), keys)
                        .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
            else if( values != null )
                return graph.api.findEdgesWithValues(graph.name(), values)
                        .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
            else if( kvPairs != null && kvPairs.size() == 1 )
                return graph.api.findEdgesWithKeyValues(graph.name(), null, kvPairs)
                        .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
        }
        else if( optimizedParams.size() == 2 && label != null && kvPairs != null ){
            return graph.api.findEdgesWithKeyValues(graph.name(), label, kvPairs)
                    .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
        }
        // else
        return graph.api.findEdges(graph.name()
                , label, labels, key, keyNot, keys, values, kvPairs)
                .map(r -> (Edge) new AgensEdge(graph, r)).collect(Collectors.toList());
    }

    public static Map<String, Object> optimizedParams(String label, List<String> labelParams
            , String key, String keyNot, List<String> keyParams, List<String> valueParams
            , Map<String, String> kvPairs){
        Map<String, Object> params = new HashMap<>();
        if( label != null ) params.put("label", label);
        if( labelParams != null ) params.put("labels", labelParams);
        if( key != null ) params.put("key", key);
        if( keyNot != null ) params.put("keyNot", keyNot);
        if( keyParams != null ) params.put("keys", keyParams);
        if( valueParams != null ) params.put("values", valueParams);
        if( kvPairs != null ) params.put("kvPairs", kvPairs);
        return params;
    }
}
