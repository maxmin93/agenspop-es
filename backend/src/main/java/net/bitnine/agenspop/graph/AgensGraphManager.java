package net.bitnine.agenspop.graph;

import net.bitnine.agenspop.elasticgraph.ElasticGraphAPI;
import net.bitnine.agenspop.graph.structure.AgensFactory;

import net.bitnine.agenspop.graph.structure.AgensGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.script.SimpleBindings;
import javax.script.Bindings;

@Service
public class AgensGraphManager implements GraphManager {

    private static final AtomicLong updateCounter = new AtomicLong(0L);

    private static final Logger log =
            LoggerFactory.getLogger(AgensGraphManager.class);
    public static final String AGENS_GRAPH_MANAGER_EXPECTED_STATE_MSG
            = "Gremlin Server must be configured to use the AgensGraphManager.";
    public static final Function<String, String> GRAPH_TRAVERSAL_NAME = (String gName) -> gName + "_g";

    // Datasources based on Vertex index
    private final Map<String, Graph> graphs = new ConcurrentHashMap<>();
    private final Map<String, TraversalSource> traversalSources = new ConcurrentHashMap<>();
    private Map<String,String> graphStates;
    // ?
    private final Object instantiateGraphLock = new Object();

    // private final ElasticGraphAPI baseAPI;
    private final ElasticGraphAPI baseAPI;
    private static AgensGraphManager instance = null;
    private GremlinExecutor gremlinExecutor = null;

    @Autowired
    public AgensGraphManager(ElasticGraphAPI baseAPI) {
        this.baseAPI = baseAPI;
        this.instance = this;

        // for DEBUG
        System.out.println("\n** AgensGraphManager is initializing.");
    }

    public static AgensGraphManager getInstance() {
        return instance;
    }

    public AgensGraph resetSampleGraph(){
        AgensGraph g = AgensFactory.createModern(baseAPI);
        String gName = g.name();
        putGraph(gName, g);
        updateTraversalSource(gName, g);

        try {
            System.out.println("reloading sample graph.. ["+gName+"]");
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // for TEST (when startup)
        System.out.println("\n[Sample Graph Test]\n-------------------------------------------\n");
        AgensFactory.traversalTestModern(g);
        return g;
    }

    public void configureGremlinExecutor(GremlinExecutor gremlinExecutor) {
        this.gremlinExecutor = gremlinExecutor;
        final ScheduledExecutorService bindExecutor = Executors.newScheduledThreadPool(1);
        // Dynamically created graphs created with the ConfiguredGraphFactory are
        // bound across all nodes in the cluster and in the face of server restarts
        bindExecutor.scheduleWithFixedDelay(
                new GremlinExecutorGraphBinder(this, this.gremlinExecutor)
                , 0, 20L, TimeUnit.SECONDS);
    }

    private class GremlinExecutorGraphBinder implements Runnable {
        final AgensGraphManager graphManager;
        final GremlinExecutor gremlinExecutor;

        public GremlinExecutorGraphBinder(AgensGraphManager graphManager, GremlinExecutor gremlinExecutor) {
            this.graphManager = graphManager;
            this.gremlinExecutor = gremlinExecutor;
        }

        @Override
        public void run() {
            graphManager.updateGraphs();
        }
    }

    @Override
    public Set<String> getGraphNames() {
        return graphs.keySet();
    }

    @Override
    public Graph getGraph(String gName) {
        return graphs.get(gName);
    }

    @Override
    public void putGraph(String gName, Graph g) {
        graphs.put(gName, g);
    }

    @Override
    public Set<String> getTraversalSourceNames() {
        return traversalSources.keySet();
    }

    @Override
    public TraversalSource getTraversalSource(String tsName) {
        return traversalSources.get(tsName);
    }

    @Override
    public void putTraversalSource(String tsName, TraversalSource ts) {
        traversalSources.put(tsName, ts);
    }

    @Override
    public TraversalSource removeTraversalSource(String tsName) {
        if (tsName == null) return null;
        return traversalSources.remove(tsName);
    }

    /**
     * Get the {@link Graph} and {@link TraversalSource} list as a set of bindings.
     */
    @Override
    public Bindings getAsBindings() {
        final Bindings bindings = new SimpleBindings();
        graphs.forEach(bindings::put);
        traversalSources.forEach(bindings::put);
        return bindings;
    }

    @Override
    public void rollbackAll() {
        graphs.forEach((key, graph) -> {
            if (graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        });
    }

    @Override
    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        commitOrRollback(graphSourceNamesToCloseTxOn, false);
    }

    @Override
    public void commitAll() {
        graphs.forEach((key, graph) -> {
            if (graph.tx().isOpen())
                graph.tx().commit();
        });
    }

    @Override
    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        commitOrRollback(graphSourceNamesToCloseTxOn, true);
    }

    public void commitOrRollback(Set<String> graphSourceNamesToCloseTxOn, Boolean commit) {
        graphSourceNamesToCloseTxOn.forEach(e -> {
            final Graph graph = getGraph(e);
            if (null != graph) {
                closeTx(graph, commit);
            }
        });
    }

    public void closeTx(Graph graph, Boolean commit) {
        if (graph.tx().isOpen()) {
            if (commit) {
                graph.tx().commit();
            } else {
                graph.tx().rollback();
            }
        }
    }

    @Override
    public Graph openGraph(String gName, Function<String, Graph> thunk) {
        Graph graph = graphs.get(gName);
        if (graph != null) {
            updateTraversalSource(gName, graph);
            return graph;
        } else {
            synchronized (instantiateGraphLock) {
                graph = graphs.get(gName);
                if (graph == null) {
                    graph = thunk.apply(gName);
                    graphs.put(gName, graph);
                }
            }
            updateTraversalSource(gName, graph);
            return graph;
        }
    }

    public Graph openGraph(String gName) {
        Graph graph = graphs.get(gName);
        if (graph != null) {
            updateTraversalSource(gName, graph);
            return graph;
        } else {
            synchronized (instantiateGraphLock) {
                graph = AgensFactory.createEmpty(baseAPI, gName);
                if (graph != null) graphs.put(gName, graph);
            }
            updateTraversalSource(gName, graph);
            return graph;
        }
    }

    @Override
    public Graph removeGraph(String gName) throws Exception {
        if (gName == null) return null;

        String result = this.baseAPI.remove(gName);
        System.out.println("** remove graph["+gName+"] ==> "+result );

        return graphs.remove(gName);
    }

    //////////////////////////////////////////////////////////////////

    public Map<String,String> getGraphStates(){ return this.graphStates; }

    public Map<String,String> searchGraphs(String query){
        List<String> dsList = baseAPI.searchDatasources(query);
        // get datasources with vertex size and edge size
        Map<String, Long> dsVlist = baseAPI.searchVertexDatasources(dsList);
        Map<String, Long> dsElist = baseAPI.searchEdgeDatasources(dsList);
        // graphs
        Map<String,String> result = new HashMap<>();
        for(Map.Entry<String, Long> ds : dsVlist.entrySet() ){
            result.put(ds.getKey(), ds.getKey()+"[V="+ds.getValue()+",E="+dsElist.getOrDefault(ds.getKey(), 0L)+"]");
        }
        return result;
    }

    public Map<String,Map<String,Long>> getGraphLabels(String datasource){
        Map<String,Map<String,Long>> agg = new HashMap<>();
        agg.put("V", baseAPI.listVertexLabels(datasource));
        agg.put("E", baseAPI.listEdgeLabels(datasource));
        return agg;
    }

    public Map<String,Long> getGraphKeys(String datasource, String label){
        Map<String,Long> agg = baseAPI.listVertexLabelKeys(datasource, label);
        return agg.keySet().size() > 0 ? agg :
                baseAPI.listEdgeLabelKeys(datasource, label);
    }

    public synchronized void updateGraphs(){
        boolean isFirst = updateCounter.getAndIncrement() == 1L ? true : false;
        graphStates = new HashMap<>();

        // check exist datasources
        Map<String, Long> dsVlist = baseAPI.listVertexDatasources();
        Map<String, Long> dsElist = baseAPI.listEdgeDatasources();

        // graphs loading
        for(Map.Entry<String, Long> ds : dsVlist.entrySet() ){
            AgensGraph g = (AgensGraph) graphs.get( ds.getKey() );
            // if not exists, then create new graph
            if( g == null ){
                g = AgensFactory.createEmpty(baseAPI, ds.getKey());
                putGraph(ds.getKey(), g);
            }
            updateTraversalSource(ds.getKey(), g);
            graphStates.put(ds.getKey(), ds.getKey()+"[V="+ds.getValue()+",E="+dsElist.getOrDefault(ds.getKey(), 0L)+"]");
        }

        if( isFirst ){
            System.out.println("AgensGraphManager ready ==> "+String.join(", ", graphStates.values())+"\n");

            String gName = "modern";
            if( graphStates.keySet().contains(gName) ) {
                // for TEST (when startup)
                System.out.println("\n== Sample Graph Test ==");
                System.out.println(  "-------------------------------------------\n");
                AgensGraph g = (AgensGraph) openGraph(gName);
                AgensFactory.traversalTestModern(g);
                System.out.println("\n-------------------------------------------\n");
            }
        }
    }

    // **참고 : JanusGraphManager
    private void updateTraversalSource(String graphName, Graph graph){
        if (gremlinExecutor != null) {
            // ==> updateTraversalSource(graphName, graph, gremlinExecutor, this);
            gremlinExecutor.getScriptEngineManager().put(graphName, graph);
            String traversalName = GRAPH_TRAVERSAL_NAME.apply(graphName);
            GraphTraversalSource traversalSource = graph.traversal();
            gremlinExecutor.getScriptEngineManager().put(traversalName, traversalSource);
            putTraversalSource(traversalName, traversalSource);
        }
    }

}