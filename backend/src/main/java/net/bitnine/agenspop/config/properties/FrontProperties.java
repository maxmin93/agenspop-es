package net.bitnine.agenspop.config.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// application.yml 파일에서 prefix에 대응되는 설정값들을 class 로 로딩하기
@Getter
@Setter
@ToString
@ConfigurationProperties(prefix = "agens.front")
public class FrontProperties {

    private boolean debug = false;          // default value
    private String initMode = "webgl";    // default value

    public Map<String,Object> toMap(){
        return Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("debug", debug),
                new AbstractMap.SimpleImmutableEntry<>("init-mode", initMode))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
