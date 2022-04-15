package net.bitnine.agenspop.elasticgraph.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.elasticgraph.util.ElasticHelper;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class ElasticElement implements BaseElement {

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private boolean removed;

    // **NOTE: elasticsearch date type
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
    // @JsonFormat(shape = JsonFormat.Shape.STRING, pattern="yyyy-MM-dd HH:mm:ss")
    // protected LocalDateTime created;   // Date format: "yyyy-MM-dd HH:mm:ss"

    // ISODateTime Converter
    // http://www.java2s.com/example/java-src/pkg/com/tkmtwo/timex/convert/isodatetimeconverter-ce9a9.html

    // Elasticsearch DateTime format
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
    // ==> basic_date_time_no_millis
    // A basic formatter without millis, separated by a T: yyyyMMdd'T'HHmmssZ
    // ==> Create ISO8601 joda compatible java time formatter (#38434)
    // https://github.com/elastic/elasticsearch/commit/dfa4625de5faf3641c7e9888dd7e8b7bbcf10445
    // server/src/test/java/org/elasticsearch/common/joda/JavaJodaTimeDuellingTests.java

    // **NOTE: 굳이 LocalDateTime 을 유지할 필요가 있을까? 변환하느라 약간 느려짐
    // ==> gremlin 에서 시간 쿼리를 하기 위해??
    // ==> 일단은 String 으로 처리하자! (date range 는 elasticsearch 에서 처리)
    protected String timestamp;     // Date format: "yyyy-MM-dd HH:mm:ss"
                                    // => modify to "yyyy-MM-ddTHH:mm:ss"
    protected String id;
    protected String label;
    protected String datasource;
    protected List<ElasticProperty> properties;

    protected ElasticElement(String datasource, String id, String label){
        this.datasource = datasource;
        this.id = id;
        this.label = label;
        this.properties = new ArrayList<>();
        this.removed = false;
    }
//    protected ElasticElement(String datasource, String id, String label){
//        // if empty, return String of LocalDateTime.now()
//        this(ElasticHelper.date2str(Optional.empty()), datasource, id, label);
//    }

    @Override
    public boolean notexists(){ return this.removed; }
    @Override
    public void remove(){
        this.removed = true;
        this.properties = null;
    }

    @Override
    public Collection<String> keys(){
        if( this.removed ) return Collections.EMPTY_LIST;

        List<String> keys = new ArrayList<>();
        keys.add(BaseElement.timestampTag);
        for(ElasticProperty p : properties ){
            if( p.canRead() ) keys.add(p.getKey());   // pre-check exception
        }
        return keys;
    }

    @Override
    public Collection<Object> values(){
        if( this.removed ) return Collections.EMPTY_LIST;

        List<Object> values = new ArrayList<>();
        values.add(this.timestamp);
        for(ElasticProperty p : properties ){
            if( p.canRead() ) values.add(p.value());  // pre-check exception
        }
        return values;
    }

    @Override
    public Collection<BaseProperty> properties(){
        if( this.removed ) return Collections.EMPTY_SET;

        Set<BaseProperty> propertySet = new HashSet<>(properties);
        propertySet.add(getCreatedProperty());
        return propertySet;
    }

    @JsonIgnore
    private ElasticProperty getCreatedProperty(){
        return new ElasticProperty(BaseElement.timestampTag, this.timestamp);
    }

    @Override
    public void properties(Collection<? extends BaseProperty> properties){
        if( this.removed ) return;

        this.properties = properties.stream().map(r->(ElasticProperty)r).collect(Collectors.toList());
        // if exists, set created from property
        String created = ElasticHelper.getCreatedDate((List<ElasticProperty>)properties);
        if( created != null ) this.timestamp = ElasticHelper.date2str(ElasticHelper.str2date(created));
        // else this.created = ElasticHelper.date2str(LocalDateTime.now());     // Do it at save()
    }

    @Override
    public boolean hasProperty(String key){
        if( this.removed ) return false;
        return keys().contains(key) || key.equals(BaseElement.timestampTag);
    }

    @Override
    public BaseProperty getProperty(String key){
        if( this.removed ) return null;

        for( ElasticProperty property : properties ){
            if( property.getKey().equals(key) ) return property;
        }
        // **NOTE : created property 는 properties() 호출시 생성
        if( key.equals(BaseElement.timestampTag) ) return getCreatedProperty();
        return null;
    }

    @Override
    public BaseProperty getProperty(String key, BaseProperty defaultProperty){
        if( this.removed ) return null;

        BaseProperty property = getProperty(key);
        return property == null ? defaultProperty : property;
    }

    @Override
    public void setProperty(BaseProperty property){
        if( this.removed ) return;

        if( hasProperty(property.key()) ) removeProperty(property.key());
        properties.add((ElasticProperty) property);
        // **NOTE : update created field
        if( property.key().equals(BaseElement.timestampTag) && ElasticHelper.checkDateformat(property.valueOf()) )
            this.timestamp = ElasticHelper.date2str(ElasticHelper.str2date(property.valueOf()));
    }

    @Override
    public BaseProperty removeProperty(String key){
        if( this.removed ) return null;

        Iterator<ElasticProperty> iter = properties.iterator();
        while(iter.hasNext()){
            BaseProperty property = iter.next();
            if( property.key().equals(key) ){
                iter.remove();
                return property;
            }
        }
        return null;
    }
}
