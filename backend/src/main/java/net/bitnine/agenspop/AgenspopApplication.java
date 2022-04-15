package net.bitnine.agenspop;

import net.bitnine.agenspop.config.properties.ElasticProperties;
import net.bitnine.agenspop.config.properties.FrontProperties;
import net.bitnine.agenspop.config.properties.ProductProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.ApplicationPidFileWriter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@EnableConfigurationProperties({ ElasticProperties.class, ProductProperties.class, FrontProperties.class})
public class AgenspopApplication {

	public static void main(String[] args) {

		SpringApplication application = new SpringApplication(AgenspopApplication.class);
		application.addListeners(new ApplicationPidFileWriter());	// pid file

		// application run
		ConfigurableApplicationContext ctx = application.run(args);
		ProductProperties productProperties = ctx.getBean(ProductProperties.class);

		// notify startup of server
		String hello_msg = productProperties.getHelloMsg();
		if( hello_msg != null ){
			StringBuilder sb = new StringBuilder();
			for(int i=hello_msg.length()+2; i>0; i--) sb.append("=");
			System.out.println("\n"+sb.toString());
			System.out.println(" " + hello_msg);
			System.out.println(sb.toString()+"\n");
		}

	}

}
