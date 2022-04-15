package net.bitnine.agenspop.service;

import net.bitnine.agenspop.graph.AgensGraphManager;

import org.apache.tinkerpop.gremlin.server.util.LifeCycleHook;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;


// groovy service
//
// **참고
// 소스 org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor
// https://github.com/eugenp/tutorials/blob/master/core-groovy-2/src/main/java/com/baeldung/MyJointCompilationApp.java

@Service
public class AgensGroovyServer {

    private static final Logger logger = LoggerFactory.getLogger(AgensGroovyServer.class);

    private final AgensGraphManager graphManager;
    private final Settings settings;
    private final List<LifeCycleHook> hooks;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService gremlinExecutorService;
    private final GremlinExecutor gremlinExecutor;

    private final Map<String,Object> hostOptions = new ConcurrentHashMap<>();

    @Autowired
    public AgensGroovyServer(
            AgensGraphManager graphManager
            , ExecutorService gremlinExecutorService
            , ScheduledExecutorService scheduledExecutorService
    ) {
        this.graphManager = graphManager;
        this.gremlinExecutorService = gremlinExecutorService;
        this.scheduledExecutorService = scheduledExecutorService;
        this.settings = new Settings();     // default settings

        logger.info("Initialized Gremlin thread pool.  Threads in pool named with pattern gremlin-*");
        final GremlinExecutor.Builder gremlinExecutorBuilder = GremlinExecutor.build()
                .scriptEvaluationTimeout(settings.scriptEvaluationTimeout)
                .afterFailure((b, e) -> this.graphManager.rollbackAll())
                .beforeEval(b -> this.graphManager.rollbackAll())
                .afterTimeout(b -> this.graphManager.rollbackAll())
                .globalBindings(this.graphManager.getAsBindings())
                .executorService(this.gremlinExecutorService)
                .scheduledExecutorService(this.scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> {
            // use plugins if they are present
            if (!v.plugins.isEmpty()) {
                // make sure that server related classes are available at init - new approach. the LifeCycleHook stuff
                // will be added explicitly via configuration using GremlinServerGremlinModule in the yaml
                gremlinExecutorBuilder.addPlugins(k, v.plugins);
            }
        });

        gremlinExecutor = gremlinExecutorBuilder.create();
        logger.info("Initialized GremlinExecutor and preparing GremlinScriptEngines instances.");

        // force each scriptengine to process something so that the init scripts will fire (this is necessary if
        // the GremlinExecutor is using the GremlinScriptEngineManager. this is a bit of hack, but it at least allows
        // the global bindings to become available after the init scripts are run (DefaultGremlinScriptEngineManager
        // runs the init scripts when the GremlinScriptEngine is created.
        settings.scriptEngines.keySet().forEach(engineName -> {
            try {
                // use no timeout on the engine initialization - perhaps this can be a configuration later
                final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build().
                        scriptEvaluationTimeoutOverride(0L).create();
                gremlinExecutor.eval("1+1", engineName, new SimpleBindings(Collections.emptyMap()), lifeCycle).join();
                registerMetrics(engineName);    // gremlin-groovy
                logger.info("Initialized {} GremlinScriptEngine and registered metrics", engineName);
            } catch (Exception ex) {
                logger.warn(String.format("Could not initialize %s GremlinScriptEngine as init script could not be evaluated", engineName), ex);
            }
        });

        // script engine init may have altered the graph bindings or maybe even created new ones - need to
        // re-apply those references back
        gremlinExecutor.getScriptEngineManager().getBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof Graph)
                .forEach(kv -> this.graphManager.putGraph(kv.getKey(), (Graph) kv.getValue()));

        // script engine init may have constructed the TraversalSource bindings - store them in Graphs object
        gremlinExecutor.getScriptEngineManager().getBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof TraversalSource)
                .forEach(kv -> {
                    logger.info("A {} is now bound to [{}] with {}", kv.getValue().getClass().getSimpleName(), kv.getKey(), kv.getValue());
                    this.graphManager.putTraversalSource(kv.getKey(), (TraversalSource) kv.getValue());
                });

        // determine if the initialization scripts introduced LifeCycleHook objects - if so we need to gather them
        // up for execution
        hooks = gremlinExecutor.getScriptEngineManager().getBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof LifeCycleHook)
                .map(kv -> (LifeCycleHook) kv.getValue())
                .collect(Collectors.toList());

        // graphManager.configureGremlinExecutor for ScheduledTask
        this.graphManager.configureGremlinExecutor(gremlinExecutor);

        // update graphs by scheduler
        this.scheduledExecutorService.scheduleWithFixedDelay( new Runnable() {
            public void run() {
                graphManager.updateGraphs();
            }
            }, 2000, this.settings.scriptEvaluationTimeout, TimeUnit.MILLISECONDS
        );
    }

    /////////////////////////////////////////////////////
    //
    // CompletableFuture<Object> eval(
    //      final String script
    //      , final String language
    //      , final Bindings boundVars
    //      ,  final LifeCycle lifeCycle
    // )

    // https://stackoverflow.com/questions/19793944/retrieve-used-groovy-variable-after-evaluate
/*
    @PostConstruct
    private synchronized void ready() {
        String script = "v = modern_g.V('modern_2')";     //"a = 1 + 1";
        final AtomicReference<Object> resultHolder = new AtomicReference<>();
        try{
            // use no timeout on the engine initialization - perhaps this can be a configuration later
            final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build().
                    scriptEvaluationTimeoutOverride(0L).create();
            final Map<String,Object> params = new HashMap<>();
            final Bindings bindings = new SimpleBindings(params);

            final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, null, bindings, lifeCycle);
            evalFuture.thenAcceptAsync(r -> {
                // now that the eval/serialization is done in the same thread - complete the promise so we can
                // write back the HTTP response on the same thread as the original request
                resultHolder.set(r);
                if( r != null ){
                    System.out.println("**0) script : "+script+" ==> "+r.toString()
                            +"\n  -- "+ Joiner.on("&").withKeyValueSeparator("=").join(bindings)
                            // bindings.entrySet().stream().map(Object::toString).collect(Collectors.joining("&"))
                            +"\n  -- "+ r.getClass().getName());
                    DefaultGraphTraversal t = (DefaultGraphTraversal)r;
                    while(t.hasNext()){
                        // AgensGraphStepStrategy::traversal = [AgensCountGlobalStep(vertex)]
                        // ==> ... 6
                        System.out.println("     ... "+t.next().toString());
                    }
                }
            }, gremlinExecutor.getExecutorService());

            evalFuture.join();
            Object r = evalFuture.get();
            if( resultHolder.get() != null ){
                Object result = resultHolder.get();
                System.out.println("**1)script : "+script+" ==> "+result.toString()+" : "+result.getClass().getName());
            }
        } catch (Exception ex) {
            // tossed to exceptionCaught which delegates to sendError method
            final Throwable t = ExceptionUtils.getRootCause(ex);
            throw new RuntimeException(null == t ? ex : t);
        }
    }
*/
    /////////////////////////////////////////////////////

    private void registerMetrics(final String engineName) {
        final GremlinScriptEngine engine = gremlinExecutor.getScriptEngineManager().getEngineByName(engineName);
        MetricManager.INSTANCE.registerGremlinScriptEngineMetrics(engine, engineName, "sessionless", "class-cache");
    }

    public void addHostOption(final String key, final Object value) {
        hostOptions.put(key, value);
    }

    public Map<String,Object> getHostOptions() {
        return Collections.unmodifiableMap(hostOptions);
    }

    public Object removeHostOption(final String key) {
        return hostOptions.remove(key);
    }

    public void clearHostOptions() {
        hostOptions.clear();
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }

    public ExecutorService getGremlinExecutorService() {
        return gremlinExecutorService;
    }

    public GraphManager getGraphManager() {
        return graphManager;
    }

    public Settings getSettings() {
        return settings;
    }

    public List<LifeCycleHook> getHooks() {
        return hooks;
    }
}
