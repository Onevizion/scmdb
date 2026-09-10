package com.onevizion.scmdb;

import graphql.Scalars;
import graphql.GraphQL;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratedGraphqlSchemaValidationTest {
    @Test
    void parsesEveryFileAndLoadsMergedSchema() throws Exception {
        String configuredDirectory = System.getProperty("graphql.schema.dir");
        if (configuredDirectory == null) {
            return;
        }
        Path directory = Path.of(configuredDirectory);
        List<Path> files;
        try (var paths = Files.list(directory)) {
            files = paths.filter(path -> path.toString().endsWith(".graphql"))
                    .sorted(Comparator.comparing(Path::toString)).toList();
        }
        assertFalse(files.isEmpty());
        TypeDefinitionRegistry merged = GraphqlSchemaTestUtils.merge(files);
        graphql.schema.idl.SchemaParser parser = new graphql.schema.idl.SchemaParser();
        for (Path file : files) {
            TypeDefinitionRegistry standalone = parser.parse(file.toFile());
            standalone.merge(parser.parse("type Query { schemaHealth: Boolean }"));
            new SchemaGenerator().makeExecutableSchema(standalone, scalarWiring());
        }
        merged.merge(new graphql.schema.idl.SchemaParser().parse("type Query { schemaHealth: Boolean }"));
        GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(merged, scalarWiring());
        assertTrue(GraphQL.newGraphQL(schema).build().execute("{ schemaHealth }").getErrors().isEmpty());
    }

    private static RuntimeWiring scalarWiring() {
        RuntimeWiring.Builder wiring = RuntimeWiring.newRuntimeWiring();
        for (String name : List.of("BigInt", "Decimal", "Date", "DateTime")) {
            GraphQLScalarType scalar = GraphQLScalarType.newScalar(Scalars.GraphQLString).name(name).build();
            wiring.scalar(scalar);
        }
        return wiring.build();
    }
}
