package net.bitnine.agenspop.elasticgraph.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import net.bitnine.agenspop.basegraph.model.BaseEdge;

@Data
@EqualsAndHashCode(callSuper=false)
public class ElasticEdge extends ElasticElement implements BaseEdge {

    private String src;     // id of out-vertex : source
    private String dst;     // id of in-vertex : target

    public ElasticEdge(String datasource, String id, String label, String src, String dst){
        super(datasource, id, label);
        this.src = src;
        this.dst = dst;
    }
}
