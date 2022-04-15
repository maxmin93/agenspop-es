package net.bitnine.agenspop.config.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

// application.yml 파일에서 prefix에 대응되는 설정값들을 class 로 로딩하기
@Getter @Setter @ToString
@ConfigurationProperties(prefix = "agens.product")
public class ProductProperties {

    private String name = "";
    private String version = "";
    private String helloMsg = "";

}
