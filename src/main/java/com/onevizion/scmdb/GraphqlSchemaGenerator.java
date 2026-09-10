package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.ScmdbException;
import com.onevizion.scmdb.model.ComponentMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;

@Component
public class GraphqlSchemaGenerator {
    private static final String GRAPHQL_EXTENSION = ".graphql";

    private final AppArguments appArguments;
    private final ComponentStructureGenerator componentStructureGenerator;
    private final GraphqlNamingService naming;
    private final ColorLogger logger;

    @Autowired
    public GraphqlSchemaGenerator(AppArguments appArguments,
                                  ComponentStructureGenerator componentStructureGenerator,
                                  GraphqlNamingService naming,
                                  ColorLogger logger) {
        this.appArguments = appArguments;
        this.componentStructureGenerator = componentStructureGenerator;
        this.naming = naming;
        this.logger = logger;
    }

    public GenerationResult generateAll() {
        Path outputDirectory = appArguments.getGraphqlSchemasDirectory().toPath();
        prepareOutputDirectory(outputDirectory, true);
        return generate(outputDirectory, component -> true);
    }

    public GenerationResult generateAffected(Set<String> changedTableNames) {
        Set<String> normalizedTableNames = changedTableNames.stream()
                .map(name -> name.toUpperCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
        Path outputDirectory = appArguments.getGraphqlSchemasDirectory().toPath();
        prepareOutputDirectory(outputDirectory, false);
        if (normalizedTableNames.isEmpty()) {
            logger.info("No changed tables found; GraphQL component schemas are up to date");
            return new GenerationResult(0);
        }
        return generate(outputDirectory, component -> component.tables().stream()
                .anyMatch(table -> normalizedTableNames.contains(table.name().toUpperCase(Locale.ROOT))));
    }

    private GenerationResult generate(Path outputDirectory, Predicate<ComponentMetadata> selector) {
        ComponentStructureGenerator.BuildResult buildResult = componentStructureGenerator.buildModels();
        List<String> errors = new ArrayList<>(buildResult.errors());
        int generated = 0;
        for (ComponentMetadata component : buildResult.models().stream().filter(selector).toList()) {
            Path output = outputDirectory.resolve("component_" + component.componentId() + "_"
                    + component.mainTable().toLowerCase(Locale.ROOT) + GRAPHQL_EXTENSION);
            try {
                GraphqlSchemaRenderer renderer = new GraphqlSchemaRenderer(component, naming);
                String schema = renderer.render();
                deletePreviousComponentSchema(outputDirectory, component.componentId());
                write(output, schema);
                generated++;
            } catch (RuntimeException e) {
                String message = "Component " + component.componentId() + " (" + component.componentName() + "): " + e.getMessage();
                errors.add(message);
                logger.warn(message, ColorLogger.Color.YELLOW);
            }
        }
        logger.info("Generated GraphQL component schemas: {}, failed={}", GREEN, generated, errors.size());
        if (!errors.isEmpty()) {
            throw new ScmdbException("GraphQL schema generation failed for " + errors.size()
                                   + " component(s); see warnings above for details.");
        }
        return new GenerationResult(generated);
    }

    private static void prepareOutputDirectory(Path outputDirectory, boolean clear) {
        try {
            Files.createDirectories(outputDirectory);
            if (!clear) {
                return;
            }
            try (Stream<Path> files = Files.list(outputDirectory)) {
                for (Path file : files.filter(path -> path.getFileName().toString().endsWith(GRAPHQL_EXTENSION)).toList()) {
                    Files.delete(file);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to prepare GraphQL output directory: " + outputDirectory, e);
        }
    }

    private static void deletePreviousComponentSchema(Path outputDirectory, Integer componentId) {
        String prefix = "component_" + componentId + "_";
        try (Stream<Path> files = Files.list(outputDirectory)) {
            for (Path file : files.filter(path -> {
                String name = path.getFileName().toString();
                return name.startsWith(prefix) && name.endsWith(GRAPHQL_EXTENSION);
            }).toList()) {
                Files.delete(file);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to replace GraphQL schema for component " + componentId, e);
        }
    }

    private static void write(Path output, String value) {
        try {
            Files.writeString(output, value, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write GraphQL schema: " + output.toAbsolutePath(), e);
        }
    }

    public record GenerationResult(int generated) { }
}
