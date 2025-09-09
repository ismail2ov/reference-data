package com.sixgroup.referencedata.infrastructure.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sixgroup.referencedata.application.IsinService;
import com.sixgroup.referencedata.domain.IsinRepository;

@Configuration
public class BeansConfiguration {

    @Bean
    public IsinService getProductService(IsinRepository isinRepository) {
        return new IsinService(isinRepository);
    }

}