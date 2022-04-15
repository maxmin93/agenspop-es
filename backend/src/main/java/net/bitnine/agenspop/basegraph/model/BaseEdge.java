package net.bitnine.agenspop.basegraph.model;

public interface BaseEdge extends BaseElement {

    public static final String DEFAULT_LABEL = "edge";

    String getSrc();
    String getDst();

}
