package net.bitnine.agenspop.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class AgensWebConfig implements WebMvcConfigurer {

    private static final String[] CLASSPATH_RESOURCE_LOCATIONS = {
            "classpath:/META-INF/resources/", "classpath:/resources/",
            "classpath:/static/", "classpath:/public/" };

    @Value("${agens.api.base-path}")
    private String basePath;

    @Override
    public void addCorsMappings(CorsRegistry corsRegistry) {
        // 일단 모두 해제 상태로 개발하다가 추후 클라이언트의 접근 URL 기준으로 조정
        corsRegistry.addMapping("/**");

        // **참고 https://www.baeldung.com/spring-value-annotation
//		registry.addMapping("/api/**")
//		.allowedOrigins("http://domain2.com")
//		.allowedMethods("PUT", "DELETE")
//		.allowedHeaders("header1", "header2", "header3")
//		.exposedHeaders("header1", "header2")
//		.allowCredentials(false).maxAge(3600);
    }

    // index.html을 찾기 위한 리소스 로케이션 등록
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {

        if (!registry.hasMappingForPattern("/**")) {
            registry.addResourceHandler("/**").addResourceLocations("classpath:/static/");
//					CLASSPATH_RESOURCE_LOCATIONS);
        }
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        // **NOTE: forward도 redirect도 안먹힘
        // registry.addViewController("/").setViewName("redirect:/index.html");
        registry.addRedirectViewController("/", "/index.html");

        // 고정된 redirect 이기 때문에
        // `workspace/:ds` 같은 파라미터를 넘기지 못한다
        registry.addRedirectViewController("/workspace/*", "/index.html");
    }
}
