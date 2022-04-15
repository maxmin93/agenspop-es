package net.bitnine.agenspop.graph.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class AgensPropertyMapStep<K,E> extends MapStep<Element, Map<K, E>>
        implements TraversalParent, ByModulating, Configuring {

    protected String[] propertyKeys;
    protected final PropertyType returnType;

    protected int tokens;
    protected Traversal.Admin<Element, ? extends Property> propertyTraversal;

    private Parameters parameters = new Parameters();
    private TraversalRing<K, E> traversalRing;

    // includeTokens = true => .valueMap(true) => [id, label, property...]
    @Deprecated
    public AgensPropertyMapStep(final Traversal.Admin traversal, final boolean includeTokens, final PropertyType propertyType, final String... propertyKeys) {
        this(traversal, propertyType, propertyKeys);
        this.configure(WithOptions.tokens, includeTokens ? WithOptions.all : WithOptions.none);
    }

    // **NOTE: 생성자 valueMap() 의 생성자. (includeTokens=true) 옵션은 넘어오지 않음
    //          ==> id, label 등을 포함하고 싶으면 직접 명시해야 함
    public AgensPropertyMapStep(final Traversal.Admin traversal, final PropertyType propertyType, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        this.returnType = propertyType;
        this.propertyTraversal = null;
        this.traversalRing = new TraversalRing<>();
    }

    @Override
    protected Map<K, E> map(final Traverser.Admin<Element> traverser) {
        final Map<Object, Object> map = new LinkedHashMap<>();
        final Element element = traverser.get();
        final boolean isVertex = element instanceof Vertex;

        // Deprecated
        if (this.returnType == PropertyType.VALUE) {
            if (includeToken(WithOptions.ids)) map.put(T.id, element.id());
            if (element instanceof VertexProperty) {
                if (includeToken(WithOptions.keys)) map.put(T.key, ((VertexProperty<?>) element).key());
                if (includeToken(WithOptions.values)) map.put(T.value, ((VertexProperty<?>) element).value());
            } else {
                if (includeToken(WithOptions.labels)) map.put(T.label, element.label());
            }
        }

        List<String> withOptions = Arrays.asList(this.propertyKeys);
        if( withOptions.contains(T.id.name()) ){
            map.put(T.id.name(), element.id());
            // this.propertyKeys = (String[]) ArrayUtils.removeElement(this.propertyKeys, T.id.name());
        }
        if( withOptions.contains(T.label.name()) ){
            map.put(T.label.name(), element.label());
            // this.propertyKeys = (String[]) ArrayUtils.removeElement(this.propertyKeys, T.label.name());
        }
        // **NOTE: propertyKeys == [] 이면 모든 property 를 출력한다
        //      => 어차피 properties 에 id, label 은 없으니 그냥 놔두자

        final Iterator<? extends Property> properties = (null == this.propertyTraversal) ?
                element.properties(this.propertyKeys) :
                TraversalUtil.applyAll(traverser, this.propertyTraversal);
        //final Iterator<? extends Property> properties = element.properties(this.propertyKeys);
        while (properties.hasNext()) {
            final Property<?> property = properties.next();
            map.put(property.key(), property.value());
/*
            // **NOTE: VertexProperty 인 경우 value 를 ArrayList 로 출력하는 부분
            //      ==> 제거!
            //
            final Object value = this.returnType == PropertyType.VALUE ? property.value() : property;
            if (isVertex) {
                map.compute(property.key(), (k, v) -> {
                    final List<Object> values = v != null ? (List<Object>) v : new ArrayList<>();
                    values.add(value);
                    return values;
                });
            } else {
                map.put(property.key(), value);
            }
 */
        }
        if (!traversalRing.isEmpty()) {
            for (final Object key : map.keySet()) {
                map.compute(key, (k, v) -> TraversalUtil.applyNullable(v, (Traversal.Admin) this.traversalRing.next()));
            }
            this.traversalRing.reset();
        }
        return (Map) map;
    }

    @Override
    public void configure(final Object... keyValues) {
        if (keyValues[0].equals(WithOptions.tokens)) {
            if (keyValues.length == 2 && keyValues[1] instanceof Boolean) {
                this.tokens = ((boolean) keyValues[1]) ? WithOptions.all : WithOptions.none;
            } else {
                for (int i = 1; i < keyValues.length; i++) {
                    if (!(keyValues[i] instanceof Integer))
                        throw new IllegalArgumentException("WithOptions.tokens requires Integer arguments (possible " + "" +
                                "values are: WithOptions.[none|ids|labels|keys|values|all])");
                    this.tokens |= (int) keyValues[i];
                }
            }
        } else {
            this.parameters.set(this, keyValues);
        }
    }

    @Override
    public Parameters getParameters() {
        return parameters;
    }

    @Override
    public List<Traversal.Admin<K, E>> getLocalChildren() {
        final List<Traversal.Admin<K, E>> result = new ArrayList<>();
        if (null != this.propertyTraversal)
            result.add((Traversal.Admin) propertyTraversal);
        result.addAll(this.traversalRing.getTraversals());
        return Collections.unmodifiableList(result);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    public void setPropertyTraversal(final Traversal.Admin<Element, ? extends Property> propertyTraversal) {
        this.propertyTraversal = this.integrateChild(propertyTraversal);
    }

    public PropertyType getReturnType() {
        return this.returnType;
    }

    public String[] getPropertyKeys() {
        return propertyKeys;
    }

    @Deprecated
    public boolean isIncludeTokens() {
        return this.tokens != WithOptions.none;
    }

    public String toString() {
        return StringFactory.stepString(this, Arrays.asList(this.propertyKeys),
                this.traversalRing, this.returnType.name().toLowerCase());
    }

    @Override
    public AgensPropertyMapStep<K,E> clone() {
        final AgensPropertyMapStep<K,E> clone = (AgensPropertyMapStep<K,E>) super.clone();
        if (null != this.propertyTraversal)
            clone.propertyTraversal = this.propertyTraversal.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.returnType.hashCode() ^ Integer.hashCode(this.tokens);
        if (null != this.propertyTraversal)
            result ^= this.propertyTraversal.hashCode();
        for (final String propertyKey : this.propertyKeys) {
            result ^= propertyKey.hashCode();
        }
        return result ^ this.traversalRing.hashCode();
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (null != this.propertyTraversal)
            this.integrateChild(this.propertyTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    public int getIncludedTokens() {
        return this.tokens;
    }

    private boolean includeToken(final int token) {
        return 0 != (this.tokens & token);
    }
}