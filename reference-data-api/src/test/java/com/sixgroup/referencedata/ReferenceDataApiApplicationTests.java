package com.sixgroup.referencedata;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import org.junit.jupiter.api.Test;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class ReferenceDataApiApplicationTests {

    @Test
    void contextLoads() {
    }

}
