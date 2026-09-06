package com.dftp.template;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"com.dftp.template", "com.dftp.common"})
@EntityScan(basePackages = {"com.dftp.template", "com.dftp.common"})
@EnableJpaRepositories(basePackages = {"com.dftp.template", "com.dftp.common"})
public class ServiceTemplateApplication {
    public static void main(String[] args) {
        SpringApplication.run(ServiceTemplateApplication.class, args);
    }
}
