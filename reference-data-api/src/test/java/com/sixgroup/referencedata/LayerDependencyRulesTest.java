package com.sixgroup.referencedata;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import org.junit.jupiter.api.Test;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;

public class LayerDependencyRulesTest {

    public static final String BASE_PACKAGE = "com.sixgroup.referencedata";

    public static final String APPLICATION_PACKAGE = BASE_PACKAGE + ".application..";

    public static final String DOMAIN_PACKAGE = BASE_PACKAGE + ".domain..";

    public static final String APACHE_COMMONS_VALIDATOR = "org.apache.commons.validator..";

    private final JavaClasses importedClasses = new ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
        .importPackages(BASE_PACKAGE);

    private static final String JAVA_STANDARD_LIBRARY = "java..";

    @Test
    void applicationLayerShouldDependOnDomainLayerAndJavaStandardLibrary() {

        ArchRule rule = noClasses().that()
            .resideInAPackage(APPLICATION_PACKAGE)
            .should()
            .dependOnClassesThat()
            .resideOutsideOfPackages(APPLICATION_PACKAGE, DOMAIN_PACKAGE, JAVA_STANDARD_LIBRARY);

        rule.check(importedClasses);
    }

    @Test
    void domainLayerShouldDependOnJavaStandardLibraryAndApacheCommonsValidator() {

        ArchRule rule = noClasses().that()
            .resideInAPackage(DOMAIN_PACKAGE)
            .should()
            .dependOnClassesThat()
            .resideOutsideOfPackages(DOMAIN_PACKAGE, JAVA_STANDARD_LIBRARY, APACHE_COMMONS_VALIDATOR);

        rule.check(importedClasses);
    }
}
