package com.sixgroup.referencedata.infrastructure.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sixgroup.referencedata.application.EnrichedTradeService;
import com.sixgroup.referencedata.application.IsinService;
import com.sixgroup.referencedata.application.TradeService;
import com.sixgroup.referencedata.domain.enriched.EnrichedTradeRepository;
import com.sixgroup.referencedata.domain.isin.IsinRepository;
import com.sixgroup.referencedata.domain.trade.TradeRepository;

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

    @Bean
    public EnrichedTradeService getEnrichedTradeService(EnrichedTradeRepository enrichedTradeRepository) {
        return new EnrichedTradeService(enrichedTradeRepository);
    }

}