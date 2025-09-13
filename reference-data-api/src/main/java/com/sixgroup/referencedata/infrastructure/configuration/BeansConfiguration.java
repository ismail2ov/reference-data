package com.sixgroup.referencedata.infrastructure.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sixgroup.referencedata.application.IsinService;
import com.sixgroup.referencedata.application.TradeService;
import com.sixgroup.referencedata.domain.IsinRepository;
import com.sixgroup.referencedata.domain.TradeRepository;

@Configuration
public class BeansConfiguration {

    @Bean
    public IsinService getIsinService(IsinRepository isinRepository) {
        return new IsinService(isinRepository);
    }

    @Bean
    public TradeService getTradeService(TradeRepository tradeRepository) {
        return new TradeService(tradeRepository);
    }

}