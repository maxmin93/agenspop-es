package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.basegraph.model.BaseProperty;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;

import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AgensVertexProperty<V> implements VertexProperty<V>, WrappedVertexProperty<BaseProperty> {

    protected BaseProperty baseProperty;
    protected AgensVertex vertex;

    // case1 : baseGraphAPI 로부터 생성되는 경우
    public AgensVertexProperty(final AgensVertex vertex, final BaseProperty baseProperty) {
        Objects.requireNonNull(baseProperty, "AgensVertexProperty.value might be null .. case(1)");
        initVertexProperty(vertex, baseProperty);
    }

    // case2 : AgensGraph 외부인 사용자단으로부터 생성되는 경우
    public AgensVertexProperty(final AgensVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        Objects.requireNonNull(value, "AgensVertexProperty.value might be null .. case(2)");
        BaseProperty baseProperty = ((AgensGraph)vertex.graph()).api.createProperty(key, value);
        initVertexProperty(vertex, baseProperty);

        // **NOTE : Cardinality.single 에서는 property 에서 multi-value 를 허용하지 않음
        //      ==> 그냥 별개의 property 로 추가하도록 수정
        if( propertyKeyValues.length > 0 )
            AgensHelper.attachProperties(vertex, propertyKeyValues);
    }

    private void initVertexProperty(final AgensVertex vertex, final BaseProperty baseProperty){
        this.baseProperty = baseProperty;
        this.vertex = vertex;
        this.vertex.baseElement.setProperty(this.baseProperty);
    }

    @Override
    public BaseProperty getBaseVertexProperty(){
        return this.baseProperty;
    }

    ////////////////////////////////////

    @Override
    public Vertex element() { return this.vertex; }

    @Override
    public Object id() {
        // TODO: ElasticVertex needs a better ID system for VertexProperties
        return (String) "vp_"+this.vertex.id().hashCode()+"_"+this.key().hashCode();
    }

    @Override
    public String key() { return this.baseProperty.key(); }

    // **NOTE: Cardinality.single 때문에 사용되어서는 안됨
    @Override
    public Set<String> keys() {
        if(this.baseProperty == null) return Collections.emptySet();
        final Set<String> keys = new HashSet<>();
        keys.add(this.baseProperty.key());
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public V value() throws NoSuchElementException { return (V)this.baseProperty.value(); }

    @Override
    public boolean isPresent() {
        return vertex.baseElement.notexists() ? false : this.baseProperty.canRead();
    }

    // **NOTE: Cardinality.single 에서는 사용하지 않는 메서드
    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        System.out.println("## NotSupported: AgensVertexProperty.properties(): "+propertyKeys.toString());
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
//        if (!isPresent()) return Collections.emptyIterator();
//        List<Property<U>> valueList = new ArrayList<>();
//        valueList.add( new AgensProperty<U>(this, baseProperty) );
//        return valueList.iterator();
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        System.out.println("## NotSupported: AgensVertexProperty.property(): "+key+", "+value.toString());
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public void remove() {
        //**NOTE: baseProperty 에 removed 플래그를 두어야 하지 않을까 싶다 (나중에)
        //   ==> removed 추가시 isPresent() 와도 연결해야함
        this.vertex.graph.tx().readWrite();
        ((AgensVertex)this.element()).baseElement.removeProperty(this.key());
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
        // return "p["+key()+"->"+this.baseProperty.value()+"]";
    }
}
