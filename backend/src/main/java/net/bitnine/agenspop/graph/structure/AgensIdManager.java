package net.bitnine.agenspop.graph.structure;

import java.util.UUID;
import java.util.stream.Stream;

public enum AgensIdManager implements AgensGraph.IdManager {

    /**
     * Manages identifiers of type {@link java.util.UUID}. Will convert {@code String} values to
     * {@link java.util.UUID}.
     */
    UUID {
        @Override
        public java.util.UUID getNextId(final AgensGraph graph) {
            return java.util.UUID.randomUUID();
        }
        @Override
        public Object convert(final Object id) {
            if (null == id)
                return null;
            else if (id instanceof java.util.UUID)
                return id;
            else if (id instanceof String)
                return java.util.UUID.fromString((String) id);
            else
                throw new IllegalArgumentException(String.format("Expected an id that is convertible to UUID but received %s", id.getClass()));
        }
        @Override
        public boolean allow(final Object id) {
            return id instanceof UUID || id instanceof String;
        }
    },

    ANY {
        @Override
        public String getNextId(final AgensGraph graph) {
            return Stream.generate(() -> graph.name()+"_"+graph.currentId.incrementAndGet())
                    .filter(id -> !graph.api.existsVertex(id) && !graph.api.existsEdge(id))
                    .findAny().get();
        }
        @Override
        public Object convert(final Object id) {
            return id;
        }
        @Override
        public boolean allow(final Object id) {
            return true;
        }
    };

}
