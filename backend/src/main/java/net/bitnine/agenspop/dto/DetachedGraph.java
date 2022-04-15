package net.bitnine.agenspop.dto;

import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensFactory;
import net.bitnine.agenspop.graph.structure.AgensVertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DetachedGraph implements Serializable /*, Cloneable */ {

    private static final long serialVersionUID = 2676085718645211025L;

    private final String name;
    private final List<AgensVertex> nodes;
    private final List<AgensEdge> edges;

    public DetachedGraph(String gName, List<AgensVertex> nodes, List<AgensEdge> edges){
        this.name = gName;
        this.nodes = nodes;
        this.edges = edges;
    }
    public DetachedGraph(String gName){
        this(gName, new ArrayList<>(), new ArrayList<>());
    }
    public DetachedGraph(){
        this(AgensFactory.GREMLIN_DEFAULT_GRAPH_NAME);
    }

    public String getName() { return name; }
    public List<AgensVertex> getNodes() { return nodes; }
    public List<AgensEdge> getEdges() { return edges; }

    @Override
    public String toString(){
        return String.format("DetachedGraph[%s][V=%d,E=%d]", name, nodes.size(), edges.size());
    }
}
