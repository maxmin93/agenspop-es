package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;

import java.util.Objects;

public final class AgensProperty<V> implements Property<V>, WrappedProperty<BaseProperty> {

    protected final BaseProperty baseProperty;
    protected final Element element;

    // 외부 생성
    public AgensProperty(final Element element, final String key, final V value) {
        Objects.requireNonNull(value, "AgensProperty.value might be null");
        baseProperty = ((AgensGraph)element.graph()).api.createProperty(key, value);
        this.element = element;
        ((AgensElement)this.element).getBaseElement().setProperty(this.baseProperty);
    }
    // 내부 생성
    public AgensProperty(final Element element, final BaseProperty baseProperty) {
        Objects.requireNonNull(baseProperty, "AgensProperty.baseProperty might be null");
        this.baseProperty = baseProperty;
        this.element = element;
        ((AgensElement)this.element).getBaseElement().setProperty(this.baseProperty);
    }

    @Override
    public BaseProperty getBaseProperty() { return this.baseProperty; }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public String key() {
        return this.baseProperty.key();
    }

    @Override
    public V value() {
        return (V)this.baseProperty.value();
    }

    @Override
    public boolean isPresent() {
        return ((AgensElement)element).baseElement.notexists() ? false : this.baseProperty.canRead();
    }

    @Override
    public String toString() {
        return "p["+key()+":"+this.baseProperty.value()+"]";
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        this.element.graph().tx().readWrite();
        final BaseElement entity = ((AgensElement) this.element).getBaseElement();
        if (entity.hasProperty(this.key())) entity.removeProperty(this.key());
    }
}
