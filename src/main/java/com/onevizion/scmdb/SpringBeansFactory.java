package com.onevizion.scmdb;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

@Configuration
public class SpringBeansFactory {

    private static final String BUILD_DATE_ATTRIBUTE = "Build-Date";
    private static final String GIT_COMMIT_ID_ATTRIBUTE = "Git-Commit-Id";
    private static final String BUILD_INFO_TEMPLATE = "Build Date: {BuildDate} (Git Commit ID: {GitCommitId})";
    private static final String BUILD_INFO_UNKNOWN = "Unknown";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Bean
    @Qualifier("buildInformation")
    public String buildInformation() throws IOException {
        String buildInfo = null;
        Enumeration<URL> resources = getClass().getClassLoader().getResources("META-INF/MANIFEST.MF");
        while (resources.hasMoreElements()) {
            try {
                URL url = resources.nextElement();
                logger.debug("Reading manifest from: {}", url);
                Manifest manifest = new Manifest(url.openStream());
                String buildDate = findAttributeValue(manifest, BUILD_DATE_ATTRIBUTE);
                String gitCommitId = findAttributeValue(manifest, GIT_COMMIT_ID_ATTRIBUTE);
                
                logger.debug("Found buildDate: {}, gitCommitId: {}", buildDate, gitCommitId);

                if (StringUtils.isNotBlank(buildDate) && StringUtils.isNotBlank(gitCommitId)) {
                    buildInfo = StringPlaceholderUtils.replace(BUILD_INFO_TEMPLATE,
                                                               Map.of("BuildDate", buildDate,
                                                                      "GitCommitId", gitCommitId));
                    break;
                }
            } catch (IOException exception) {
                logger.error("Unable to read manifest", exception);
            }
        }

        return StringUtils.isNotBlank(buildInfo) ? buildInfo : BUILD_INFO_UNKNOWN;
    }

    private String findAttributeValue(Manifest manifest, String attributeName) {
        return manifest.getMainAttributes()
                       .entrySet()
                       .stream()
                       .filter(entry -> entry.getKey() instanceof Attributes.Name name
                               && name.toString().equals(attributeName))
                       .findFirst()
                       .map(Map.Entry::getValue)
                       .map(Object::toString)
                       .orElse(null);
    }
}