package net.bitnine.agenspop.config;

import net.bitnine.agenspop.config.properties.ElasticProperties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@ComponentScan(basePackages = { "net.bitnine.agenspop.elasticsearch" })
public class AgensElasticConfig {

    // ** NOTE: local 접속이 아니면 안됨
    //      ==> High Level RestClient 로 차후 변경해야
    //          (spring-data-elasticsearch v3.2 에서 지원할 때 변경)
    private final String host;
    private final int port;
    private final String username;
    private final String password;

    @Autowired
    AgensElasticConfig(ElasticProperties elasticProperties){
        this.host = elasticProperties.getHost();
        this.port = elasticProperties.getPort();
        this.username = elasticProperties.getUsername();
        this.password = elasticProperties.getPassword();
    }

    @Bean(destroyMethod = "close")
    public RestHighLevelClient restClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port))
                .setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }

}
