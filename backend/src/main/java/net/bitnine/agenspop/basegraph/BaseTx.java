package net.bitnine.agenspop.basegraph;

public interface BaseTx extends AutoCloseable {

    void failure();
    void success();
    void close();

}