package com.onevizion.scmdb;

import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Turns physical Oracle names into GraphQL names, consistently:
 * types in PascalCase (CONFIG_FIELD → ConfigField), fields in camelCase (configField),
 * lookup fields drop a trailing Id (programId → program), enum values are UPPER_SNAKE_CASE.
 * Every name is made unique within its scope so the generated schema has no clashes.
 */
@Component
public class GraphqlNamingService {
    private static final Pattern WORDS = Pattern.compile("[^0-9A-Za-z]+");

    public String typeName(String physicalName) {
        StringBuilder result = new StringBuilder();
        for (String word : WORDS.split(normalize(physicalName))) {
            if (!word.isEmpty()) {
                result.append(word.substring(0, 1).toUpperCase(Locale.ROOT))
                      .append(word.substring(1).toLowerCase(Locale.ROOT));
            }
        }
        if (result.isEmpty()) {
            result.append("GeneratedType");
        }
        if (Character.isDigit(result.charAt(0))) {
            result.insert(0, 'T');
        }
        return result.toString();
    }

    public String fieldName(String physicalName) {
        String typeName = typeName(physicalName);
        return Character.toLowerCase(typeName.charAt(0)) + typeName.substring(1);
    }

    public String relationFieldName(String tableName, Set<String> usedNames) {
        return unique(fieldName(tableName), "Relation", usedNames);
    }

    public String lookupFieldName(String physicalColumn, Set<String> usedNames) {
        String physicalField = fieldName(physicalColumn);
        String preferred = physicalField.endsWith("Id")
                ? physicalField.substring(0, physicalField.length() - 2)
                : physicalField;
        return usedNames.add(preferred) ? preferred : unique(physicalField + "MetaValue", "", usedNames);
    }

    public String enumTypeName(String ownerTable, String physicalColumn) {
        String column = physicalColumn.endsWith("_ID")
                ? physicalColumn.substring(0, physicalColumn.length() - 3)
                : physicalColumn;
        String owner = typeName(ownerTable);
        String value = typeName(column);
        return owner + (owner.equals(value) ? "Value" : value);
    }

    public String enumValueName(String value, String id, Set<String> usedNames) {
        String base = WORDS.matcher(value).replaceAll("_").replaceAll("^_+|_+$", "").toUpperCase(Locale.ROOT);
        if (base.isEmpty()) {
            base = "VALUE";
        }
        if (Character.isDigit(base.charAt(0)) || Set.of("TRUE", "FALSE", "NULL").contains(base)) {
            base = "VALUE_" + base;
        }
        String suffix = WORDS.matcher(id).replaceAll("_").replaceAll("^_+|_+$", "").toUpperCase(Locale.ROOT);
        return unique(base, "_" + (suffix.isEmpty() ? "ID" : suffix), usedNames);
    }

    private static String unique(String preferred, String collisionSuffix, Set<String> usedNames) {
        String candidate = preferred;
        int index = 2;
        while (!usedNames.add(candidate)) {
            candidate = preferred + collisionSuffix + (index == 2 ? "" : index);
            index++;
        }
        return candidate;
    }

    private static String normalize(String value) {
        return value.replace("\"", "");
    }
}