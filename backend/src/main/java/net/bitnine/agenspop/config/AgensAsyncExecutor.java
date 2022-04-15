package net.bitnine.agenspop.config;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AgensAsyncExecutor extends AsyncConfigurerSupport {

    @Override
    @Bean
    @Qualifier(value = "agensExecutor")
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(7);        // 7
        executor.setMaxPoolSize(705);         // 42
        executor.setQueueCapacity(3000);        // 11
        executor.setThreadNamePrefix("Agens-async-");
        executor.initialize();
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new AgensAsyncUncaughtExceptionHandler();
    }

    ////////////////////////////////////////

    public class AgensAsyncUncaughtExceptionHandler implements AsyncUncaughtExceptionHandler {
        @Override
        public void handleUncaughtException(Throwable ex, Method method, Object... params) {
            System.out.println("Method Name::"+method.getName());
            System.out.println("Exception occurred::"+ ex);
        }
    }
}