package net.bitnine.agenspop.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotBlank;

// application.yml 파일에서 prefix에 대응되는 설정값들을 class 로 로딩하기
@Getter @Setter
@ConfigurationProperties(prefix = "agens.elasticsearch")
public class ElasticProperties {

    @NotBlank
    private String host;        // = "localhost";
    @NotBlank
    private int port;           // = 9200;

    private String username;
    private String password;
    private long scrollLimit;      // = 10000;  if -1, then unlimit scroll

    private int indexShards;       // when create index, apply to setting
    private int indexReplicas;     // no effect in case of select, upsert, delete

    @NotBlank
    private String vertexIndex;
    @NotBlank
    private String edgeIndex;

    private boolean edgeValidation;     // ElasticHelper::filterValidEdges
}
