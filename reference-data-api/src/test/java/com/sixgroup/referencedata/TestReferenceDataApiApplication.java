package com.sixgroup.referencedata;

import org.springframework.boot.SpringApplication;

public class TestReferenceDataApiApplication {

    public static void main(String[] args) {
        SpringApplication.from(ReferenceDataApiApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
