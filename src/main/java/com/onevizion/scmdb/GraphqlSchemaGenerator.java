package com.onevizion.scmdb;

import com.onevizion.scmdb.model.ComponentMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
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

    public GenerationResult generate() {
        Path outputDirectory = appArguments.getGraphqlSchemasDirectory().toPath();
        prepareOutputDirectory(outputDirectory);
        List<ComponentMetadata> components = componentStructureGenerator.buildModels();
        int generated = 0;
        int failed = 0;
        for (ComponentMetadata component : components) {
            Path output = outputDirectory.resolve("component_" + component.componentId() + "_"
                    + component.mainTable().toLowerCase(Locale.ROOT) + GRAPHQL_EXTENSION);
            try {
                GraphqlSchemaRenderer renderer = new GraphqlSchemaRenderer(component, naming);
                write(output, renderer.render());
                generated++;
            } catch (RuntimeException e) {
                failed++;
                logger.warn("Failed GraphQL schema [{}]: {}", ColorLogger.Color.YELLOW,
                        output.getFileName(), e.getMessage());
            }
        }
        logger.info("Generated GraphQL component schemas: {}, failed={}", GREEN, generated, failed);
        return new GenerationResult(generated, failed);
    }

    private static void prepareOutputDirectory(Path outputDirectory) {
        try {
            Files.createDirectories(outputDirectory);
            try (Stream<Path> files = Files.list(outputDirectory)) {
                for (Path file : files.filter(path -> path.getFileName().toString().endsWith(GRAPHQL_EXTENSION)).toList()) {
                    Files.delete(file);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to prepare GraphQL output directory: " + outputDirectory, e);
        }
    }

    private static void write(Path output, String value) {
        try {
            Files.writeString(output, value, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write GraphQL schema: " + output.toAbsolutePath(), e);
        }
    }

    public record GenerationResult(int generated, int failed) { }
}
