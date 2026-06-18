package com.onevizion.scmdb;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class ResourceResolveUtils {

    private static final ResourcePatternResolver RESOLVER = new PathMatchingResourcePatternResolver();
    private static final String CLASSPATH_SCRIPTS_PATTERN = "classpath:scripts/*.sql";

    public static List<Resource> resolveScriptResources(File scriptsFilePath) {
        try {
            List<Resource> resources = resolveClassPathScriptResources();
            if (resources.isEmpty()) {
                resources = List.of(RESOLVER.getResources("file:" + scriptsFilePath.getAbsolutePath() + "/*.sql"));
            }
            return resources;
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve scripts resources", e);
        }
    }

    public static boolean containsClassPathScripts() {
        return !resolveClassPathScriptResources().isEmpty();
    }

    private static List<Resource> resolveClassPathScriptResources() {
        try {
            return List.of(RESOLVER.getResources(CLASSPATH_SCRIPTS_PATTERN));
        } catch (FileNotFoundException e) {
            return List.of();
        } catch (IOException e) {
            throw new RuntimeException("Failed to resolve scripts resources", e);
        }
    }

}
