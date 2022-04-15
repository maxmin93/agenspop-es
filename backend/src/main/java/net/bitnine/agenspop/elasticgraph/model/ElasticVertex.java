package net.bitnine.agenspop.elasticgraph.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import net.bitnine.agenspop.basegraph.model.BaseVertex;

@Data
@EqualsAndHashCode(callSuper=false)
public class ElasticVertex extends ElasticElement implements BaseVertex {

    private String name;     // for caption

    public ElasticVertex(String datasource, String id, String label, String name) {
        super(datasource, id, label);        
        this.name = name;
    }
    public ElasticVertex(String datasource, String id, String label) {
        this(datasource, id, label, id);
    }

}
