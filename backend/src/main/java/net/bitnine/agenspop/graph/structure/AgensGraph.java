package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.basegraph.BaseGraphAPI;
import net.bitnine.agenspop.basegraph.BaseTx;
import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.graph.process.traversal.strategy.optimization.AgensGraphCountStrategy;
import net.bitnine.agenspop.graph.process.traversal.strategy.optimization.AgensGraphStepStrategy;

import net.bitnine.agenspop.graph.process.traversal.strategy.optimization.AgensVertexStepStrategy;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphson;

/**
 * An in-memory (with optional persistence on calls to {@link #close()}), reference implementation of the property
 * graph interfaces provided by TinkerPop.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public final class AgensGraph implements Graph, WrappedGraph<BaseGraphAPI> {

    public static final Logger LOGGER = LoggerFactory.getLogger(AgensGraph.class);
    static {
        TraversalStrategies.GlobalCache.registerStrategies(AgensGraph.class
                , TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                        AgensGraphStepStrategy.instance(), AgensGraphCountStrategy.instance()
                        , AgensVertexStepStrategy.instance()
//                      // **NOTE: 테스트만 하고 삭제해야 함 (루프 조회하느라 성능 저하)
                ));
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, AgensGraph.class.getName());
    }};

    public static final String GREMLIN_AGENSGRAPH_VERTEX_ID_MANAGER = "gremlin.agensgraph.vertexIdManager";
    public static final String GREMLIN_AGENSGRAPH_EDGE_ID_MANAGER = "gremlin.agensgraph.edgeIdManager";
    public static final String GREMLIN_AGENSGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.agensgraph.defaultVertexPropertyCardinality";
    public static final String GREMLIN_AGENSGRAPH_GRAPH_LOCATION = "gremlin.agensgraph.graphLocation";
    public static final String GREMLIN_AGENSGRAPH_GRAPH_FORMAT = "gremlin.agensgraph.graphFormat";
    public static final String GREMLIN_AGENSGRAPH_GRAPH_NAME = "gremlin.agensgraph.graphName";      // added

    private final AgensGraphFeatures features = new AgensGraphFeatures();

    protected final BaseGraphAPI api;
    protected BaseConfiguration configuration = new BaseConfiguration();

    private final AgensTransaction transaction = new AgensTransaction();
    protected AgensGraphVariables graphVariables;

    protected Object graphComputerView = null;          // excluded

    protected AtomicLong currentId = new AtomicLong(1L);
    protected AgensIdManager vertexIdManager;
    protected AgensIdManager edgeIdManager;
    protected AgensIdManager vertexPropertyIdManager;   // not used
    protected VertexProperty.Cardinality defaultVertexPropertyCardinality;

    private final String graphName;
    private String graphLocation;
    private String graphFormat;

    private void initialize(final Configuration configuration) {
        this.configuration.copy(configuration);
        this.graphVariables = new AgensGraphVariables(this);

        this.tx().readWrite();

        vertexIdManager = AgensIdManager.ANY;
        edgeIdManager = AgensIdManager.ANY;
        vertexPropertyIdManager = AgensIdManager.ANY;
        defaultVertexPropertyCardinality = VertexProperty.Cardinality.single;   // fixed!

        // added
        graphLocation = configuration.getString(GREMLIN_AGENSGRAPH_GRAPH_LOCATION, null);
        graphFormat = configuration.getString(GREMLIN_AGENSGRAPH_GRAPH_FORMAT, null);

        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null)) {
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    GREMLIN_AGENSGRAPH_GRAPH_LOCATION, GREMLIN_AGENSGRAPH_GRAPH_FORMAT));
        }
        if (graphLocation != null) loadGraph();

        this.tx().commit();
    }

    protected AgensGraph(final BaseGraphAPI baseGraph, final Configuration configuration) {
        this.graphName = configuration.getString(GREMLIN_AGENSGRAPH_GRAPH_NAME, "default");
        this.api = baseGraph;
        this.initialize(configuration);
    }

    public static AgensGraph open(final BaseGraphAPI baseGraph){
        final Configuration config = new BaseConfiguration();
        config.setProperty( GREMLIN_AGENSGRAPH_GRAPH_NAME, AgensFactory.defaultGraphName());
        return new AgensGraph(baseGraph, config);
    }

    public static AgensGraph open(final BaseGraphAPI baseGraph, final Configuration config){
        if( !config.containsKey( GREMLIN_AGENSGRAPH_GRAPH_NAME )){
            config.setProperty( GREMLIN_AGENSGRAPH_GRAPH_NAME, AgensFactory.defaultGraphName());
        }
        return new AgensGraph(baseGraph, config);
    }

    ///////////////////////////////////////////////////////

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }
    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Variables variables() {
        if (null == this.graphVariables)
            this.graphVariables = new AgensGraphVariables(this);
        return this.graphVariables;
    }
/*
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).onMapper(mapper ->
                mapper.addRegistry(AgensIoRegistryV1.instance())).create();
    }
*/
    @Override
    public String toString() {
        long vSize = api.countV(graphName);
        long eSize = api.countE(graphName);
        return graphName+"[V="+vSize+",E="+eSize+"]";
    }

    public String name() { return this.graphName; }
    public BaseGraphAPI api() { return this.api; }

    public void clear() {
        this.graphVariables = null;
        this.currentId.set(0);
        this.graphComputerView = null;
    }

    /**
     * This method only has an effect if the {@link #GREMLIN_AGENSGRAPH_GRAPH_LOCATION} is set, in which case the
     * data in the graph is persisted to that location. This method may be called multiple times and does not release
     * resources.
     */
    @Override
    public void close() {
        if (graphLocation != null) saveGraph();
    }

    @Override
    public Transaction tx() {
        return this.transaction;
    }

    @Override
    public BaseGraphAPI getBaseGraph() {
        return this.api;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Features features() {
        return features;
    }

    ///////////////////////////////////////////////////////

    private void loadGraph() {
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) {
            try {
                if (graphFormat.equals("graphml")) {
                    io(graphml()).readGraph(graphLocation);
                } else if (graphFormat.equals("graphson")) {
                    io(graphson()).readGraph(graphLocation);
                } else if (graphFormat.equals("gryo")) {
                    io(IoCore.gryo()).readGraph(graphLocation);
                } else {
                    io(IoCore.createIoBuilder(graphFormat)).readGraph(graphLocation);
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Could not load graph at %s with %s", graphLocation, graphFormat), ex);
            }
        }
    }

    private void saveGraph() {
        final File f = new File(graphLocation);
        if (f.exists()) {
            f.delete();
        } else {
            final File parent = f.getParentFile();

            // the parent would be null in the case of an relative path if the graphLocation was simply: "f.gryo"
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
        }

        try {
            if (graphFormat.equals("graphml")) {
                io(graphml()).writeGraph(graphLocation);
            } else if (graphFormat.equals("graphson")) {
                io(graphson()).writeGraph(graphLocation);
            } else if (graphFormat.equals("gryo")) {
                io(IoCore.gryo()).writeGraph(graphLocation);
            } else {
                io(IoCore.createIoBuilder(graphFormat)).writeGraph(graphLocation);
            }
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Could not save graph at %s with %s", graphLocation, graphFormat), ex);
        }
    }

    ///////////////////////////////////////////////////////

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if( api.existsVertex(idValue.toString()) )
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = vertexIdManager.getNextId(this);
        }

        // create BaseVertex with BaseProperties
        final BaseVertex baseElement = this.api.createVertex(graphName, idValue.toString(), label);
        AgensHelper.attachProperties(api, baseElement, keyValues);
        final AgensVertex vertex = new AgensVertex( this, baseElement);
        return vertex.save();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        this.tx().readWrite();
        final Iterator<Vertex> iter;
        if (0 == vertexIds.length) {
//            iter = IteratorUtils.stream(this.api.vertices(graphName))
            iter = this.api.vertices(graphName)
                    .map(node -> (Vertex) new AgensVertex(this, node)).iterator();
        } else {
            ElementHelper.validateMixedElementIds(Vertex.class, vertexIds);
//            iter = Stream.of(vertexIds)
//                    .map(id -> {
//                        if (id instanceof String)
//                            return id.toString();
//                        else if (id instanceof Vertex) {
//                            return (String) ((Vertex) id).id();
//                        } else
//                            throw new IllegalArgumentException("Unknown vertex id type: " + id);
//                    })
//                    .flatMap(id -> {
//                        try {
//                            Optional<? extends BaseVertex> base = this.api.getVertexById(id);
//                            if( base.isPresent() ) return Stream.of((BaseVertex)base.get());
//                            return Stream.empty();
//                        } catch (final RuntimeException e) {
//                            if (AgensHelper.isNotFound(e)) return Stream.empty();
//                            throw e;
//                        }
//                    })
            iter = this.api.verticesByIds(Stream.of(vertexIds).map(Object::toString).toArray(String[]::new))
                    .map(node -> (Vertex) new AgensVertex(this, node)).iterator();
        }
        return iter;
    }

    public Iterator<Vertex> vertices(List<HasContainer> hasContainers, Object... vertexIds) {
        this.tx().readWrite();
        Map<String, Object> optimizedParams = AgensHelper.optimizeHasContainers(hasContainers);

        String[] vids = null;
        if( optimizedParams.containsKey("ids") ){
            if( vertexIds == null || vertexIds.length == 0 ){
                // ids 의 내용을 vertexIds 로 바꿔치기
                vids = ((List<Object>)optimizedParams.get("ids")).stream().toArray(String[]::new);
            }
            else{
                // ids 의 내용과 vertexIds 의 교집합으로 vertexIds 바꾸기
                List<String> intersection = new ArrayList<>();
                for( Object id : vertexIds ){
                    if( id instanceof Vertex ) id = ((Vertex)id).id();
                    else if( id instanceof String ){
                        if( ((List<String>)optimizedParams.get("ids")).contains(id) )
                            intersection.add( id.toString() );
                    }
                }
                vids = intersection.stream().toArray(String[]::new);
            }
        }

        final Iterator<Vertex> iter;
        if ( vids == null || vids.length == 0) {
            if( optimizedParams.size() > 0 )
                // optimizeHasContainers 내용 실행
                iter = AgensHelper.verticesWithHasContainers(this, optimizedParams).iterator();
            else
                iter = this.api.vertices(graphName)
                        .map(node -> (Vertex) new AgensVertex(this, node)).iterator();
        } else {
            iter = this.api.findVertices(graphName, vids)
                    .map(node -> (Vertex) new AgensVertex(this, node)).iterator();
        }

        return iter;
    }

    //////////////////////////////////////////////////////

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        this.tx().readWrite();
        final Iterator<Edge> iter;
        if (0 == edgeIds.length) {
//            iter = IteratorUtils.stream(this.api.edges(graphName))
            iter = this.api.edges(graphName)
                    .map(relationship -> (Edge) new AgensEdge(this, relationship)).iterator();
        } else {
            ElementHelper.validateMixedElementIds(Edge.class, edgeIds);
//            iter = Stream.of(edgeIds)
//                    .map(id -> {
//                        if (id instanceof String)
//                            return (String) id.toString();
//                        else if (id instanceof Edge) {
//                            return (String) ((Edge) id).id();
//                        } else
//                            throw new IllegalArgumentException("Unknown edge id type: " + id);
//                    })
//                    .flatMap(id -> {
//                        try {
//                            Optional<? extends BaseEdge> base = this.api.getEdgeById(id.toString());
//                            if( base.isPresent() ) return Stream.of((BaseEdge)base.get());
//                            else return Stream.empty();
//                        } catch (final RuntimeException e) {
//                            if (AgensHelper.isNotFound(e)) return Stream.empty();
//                            throw e;
//                        }
//                    })
            iter = this.api.edgesByIds(Stream.of(edgeIds).map(Object::toString).toArray(String[]::new))
                    .map(e -> (Edge) new AgensEdge(this, e)).iterator();
        }
        return iter;
    }

    public Iterator<Edge> edges(List<HasContainer> hasContainers, final Object... edgeIds) {
        this.tx().readWrite();
        Map<String, Object> optimizedParams = AgensHelper.optimizeHasContainers(hasContainers);

        String[] eids = null;
        if( optimizedParams.containsKey("ids") ){
            if( edgeIds == null || edgeIds.length == 0 ){
                // ids 의 내용을 edgeIds 로 바꿔치기
                eids = ((List<Object>)optimizedParams.get("ids")).stream().toArray(String[]::new);
            }
            else{
                // ids 의 내용과 edgeIds 의 교집합으로 edgeIds 바꾸기
                List<String> intersection = new ArrayList<>();
                for( Object id : edgeIds ){
                    if( id instanceof Edge ) id = ((Edge)id).id();
                    else if( id instanceof String ){
                        if( ((List<String>)optimizedParams.get("ids")).contains(id) )
                            intersection.add( id.toString() );
                    }
                }
                eids = intersection.stream().toArray(String[]::new);
            }
        }

        final Iterator<Edge> iter;
        if ( eids == null || eids.length == 0) {
            if( optimizedParams.size() > 0 )
                // optimizeHasContainers 내용 실행
                iter = AgensHelper.edgesWithHasContainers(this, optimizedParams).iterator();
            else
                iter = this.api.edges(graphName)
                        .map(relationship -> (Edge) new AgensEdge(this, relationship)).iterator();
        } else {
            iter = this.getBaseGraph().findEdges(graphName, eids)
                    .map(relationship -> (Edge) new AgensEdge(this, relationship)).iterator();
        }
        return iter;
    }

/*
    private <T extends Element> Iterator<T> createElementIterator(
                final Class<T> clazz, final Map<Object, T> elements,
                final IdManager idManager, final Object... ids) {
        final Iterator<T> iterator;
        if (0 == ids.length) {
            iterator = elements.values().iterator();
        } else {
            final List<Object> idList = Arrays.asList(ids);
            validateHomogenousIds(idList);

            // if the type is of Element - have to look each up because it might be an Attachable instance or
            // other implementation. the assumption is that id conversion is not required for detached
            // stuff - doesn't seem likely someone would detach a Titan vertex then try to expect that
            // vertex to be findable in OrientDB
            return clazz.isAssignableFrom(ids[0].getClass()) ?
                    IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(clazz.cast(id).id())).iterator(), Objects::nonNull)
                    : IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(idManager.convert(id))).iterator(), Objects::nonNull);
        }
        return iterator;
    }

    private void validateHomogenousIds(final List<Object> ids) {
        final Iterator<Object> iterator = ids.iterator();
        Object id = iterator.next();
        if (id == null)
            throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        final Class firstClass = id.getClass();
        while (iterator.hasNext()) {
            id = iterator.next();
            if (id == null || !id.getClass().equals(firstClass))
                throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        }
    }
*/
    ///////////////////////////////////////////////////////

    public class AgensGraphFeatures implements Features {
        private final AgensGraphGraphFeatures graphFeatures = new AgensGraphGraphFeatures();
        private final AgensGraphEdgeFeatures edgeFeatures = new AgensGraphEdgeFeatures();
        private final AgensGraphVertexFeatures vertexFeatures = new AgensGraphVertexFeatures();

        private AgensGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }
        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }
        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }
        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public class AgensGraphVertexFeatures implements Features.VertexFeatures {
        private final AgensGraphVertexPropertyFeatures vertexPropertyFeatures = new AgensGraphVertexPropertyFeatures();

        private AgensGraphVertexFeatures() {
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }
        @Override
        public boolean supportsCustomIds() {
            return false;
        }
        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return defaultVertexPropertyCardinality;
        }
    }

    public class AgensGraphEdgeFeatures implements Features.EdgeFeatures {
        private AgensGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
        @Override
        public boolean willAllowId(final Object id) {
            return edgeIdManager.allow(id);
        }
    }

    public class AgensGraphGraphFeatures implements Features.GraphFeatures {
        private AgensGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }
        @Override
        public boolean supportsTransactions() {
            return false;
        }
        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    public class AgensGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {
        private AgensGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Construct an {@link AgensGraph.IdManager} from the AgensGraph {@code Configuration}.
     */
    private static IdManager<?> selectIdManager(final Configuration config, final String configKey, final Class<? extends Element> clazz) {
        final String vertexIdManagerConfigValue = config.getString(configKey, AgensIdManager.ANY.name());
        try {
            return AgensIdManager.valueOf(vertexIdManagerConfigValue);
        } catch (IllegalArgumentException iae) {
            try {
                return (IdManager) Class.forName(vertexIdManagerConfigValue).newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Could not configure AgensGraph %s id manager with %s", clazz.getSimpleName(), vertexIdManagerConfigValue));
            }
        }
    }

    /**
     * AgensGraph will use an implementation of this interface to generate identifiers when a user does not supply
     * them and to handle identifier conversions when querying to provide better flexibility with respect to
     * handling different data types that mean the same thing.
     *
     * @param <T> the id type
     */
    public interface IdManager<T> {
        /**
         * Generate an identifier which should be unique to the {@link AgensGraph} instance.
         */
        T getNextId(final AgensGraph graph);
        /**
         * Convert an identifier to the type required by the manager.
         */
        T convert(final Object id);
        /**
         * Determine if an identifier is allowed by this manager given its type.
         */
        boolean allow(final Object id);
    }

    ///////////////////////////////////////////////

    class AgensTransaction extends AbstractThreadLocalTransaction {

        protected final ThreadLocal<BaseTx> threadLocalTx = ThreadLocal.withInitial(() -> null);

        public AgensTransaction() {
            super(AgensGraph.this);
        }

        @Override
        public void doOpen() {
            threadLocalTx.set(getBaseGraph().tx());
        }

        @Override
        public void doCommit() throws TransactionException {
            try (BaseTx tx = threadLocalTx.get()) {
                tx.success();
            } catch (Exception ex) {
                throw new TransactionException(ex);
            } finally {
                threadLocalTx.remove();
            }
        }

        @Override
        public void doRollback() throws TransactionException {
            try (BaseTx tx = threadLocalTx.get()) {
                tx.failure();
            } catch (Exception e) {
                throw new TransactionException(e);
            } finally {
                threadLocalTx.remove();
            }
        }

        @Override
        public boolean isOpen() {
            return (threadLocalTx.get() != null);
        }
    }
}