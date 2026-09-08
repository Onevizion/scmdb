package com.onevizion.scmdb;

import graphql.language.AstPrinter;
import graphql.language.Definition;
import graphql.language.NamedNode;
import graphql.parser.Parser;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class GraphqlSchemaTestUtils {
    private GraphqlSchemaTestUtils() {
    }

    static TypeDefinitionRegistry merge(List<Path> files) throws IOException {
        Map<String, String> definitions = new LinkedHashMap<>();
        Parser parser = new Parser();
        for (Path file : files) {
            for (Definition<?> definition : parser.parseDocument(Files.readString(file)).getDefinitions()) {
                String rendered = AstPrinter.printAst(definition);
                String key = definition.getClass().getName() + ":"
                        + (definition instanceof NamedNode<?> named ? named.getName() : rendered);
                String existing = definitions.putIfAbsent(key, rendered);
                if (existing != null && !existing.equals(rendered)) {
                    throw new IllegalStateException("Conflicting GraphQL definition [" + key + "] in " + file);
                }
            }
        }
        return new SchemaParser().parse(String.join("\n", definitions.values()));
    }
}
