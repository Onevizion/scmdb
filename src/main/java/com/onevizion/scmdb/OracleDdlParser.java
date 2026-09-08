package com.onevizion.scmdb;

import com.onevizion.scmdb.model.CheckConstraintMetadata;
import com.onevizion.scmdb.model.ColumnMetadata;
import com.onevizion.scmdb.model.ConstraintMetadata;
import com.onevizion.scmdb.model.DefaultKind;
import com.onevizion.scmdb.model.DefaultMetadata;
import com.onevizion.scmdb.model.ForeignKeyMetadata;
import com.onevizion.scmdb.model.GraphqlScalar;
import com.onevizion.scmdb.model.SourceMetadata;
import com.onevizion.scmdb.model.TableMetadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleDdlParser {
    private static final Pattern CREATE_TABLE = Pattern.compile("CREATE\\s+TABLE\\s+(\\w+)\\s*\\(", Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMN = Pattern.compile("^(\\w+)\\s+([A-Z0-9_]+)(\\s*\\([^)]*\\))?(.*)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern COMMENT_TABLE = Pattern.compile("COMMENT\\s+ON\\s+TABLE\\s+\\w+\\s+IS\\s+'((?:''|[^'])*)'", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern COMMENT_COLUMN = Pattern.compile("COMMENT\\s+ON\\s+COLUMN\\s+\\w+\\.(\\w+)\\s+IS\\s+'((?:''|[^'])*)'", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern FOREIGN_KEY = Pattern.compile("CONSTRAINT\\s+(\\w+)\\s+FOREIGN\\s+KEY\\s*\\(([^)]*)\\)\\s+REFERENCES\\s+(\\w+)\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PRIMARY_KEY = Pattern.compile("(?:CONSTRAINT\\s+\\w+\\s+)?PRIMARY\\s+KEY\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHECK_PREFIX = Pattern.compile("CONSTRAINT\\s+(\\w+)\\s+CHECK\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern IN_CHECK = Pattern.compile("^\\s*(?:NVL\\s*\\(\\s*)?(\\w+)(?:\\s*,\\s*[^)]+\\))?\\s+IN\\s*\\(([^()]*)\\)\\s*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern RANGE_CHECK = Pattern.compile("^\\s*(\\w+)\\s+BETWEEN\\s+([^\\s]+)\\s+AND\\s+([^\\s]+)\\s*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern NOT_EQUAL_CHECK = Pattern.compile("^\\s*(\\w+)\\s*(?:<>|!=)\\s*([^\\s)]+)\\s*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern DEFAULT_VALUE = Pattern.compile("DEFAULT(\\s+ON\\s+NULL)?\\s+('(?:''|[^'])*'|[^\\s,]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern BEFORE_INSERT_TRIGGER = Pattern.compile("CREATE\\s+(?:OR\\s+REPLACE\\s+)?TRIGGER\\s+\\w+.*?BEFORE\\s+INSERT.*?(?:FOR\\s+EACH\\s+ROW\\s+)?(?:DECLARE\\s+.*?)?BEGIN(.*?)END\\s*;", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern CONDITIONAL_ASSIGNMENT = Pattern.compile("IF\\s+:NEW\\.(\\w+)\\s+IS\\s+NULL\\s+THEN\\s+:NEW\\.\\w+\\s*:=\\s*([^;]+?)\\s*;", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern ASSIGNMENT = Pattern.compile(":NEW\\.(\\w+)\\s*:=\\s*([^;]+?)\\s*;", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SELECT_SEQUENCE = Pattern.compile("SELECT\\s+(\\w+)\\.NEXTVAL\\s+INTO\\s+:NEW\\.(\\w+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SEQUENCE = Pattern.compile("(\\w+)\\.NEXTVAL", Pattern.CASE_INSENSITIVE);
    private static final Pattern NUMBER_LITERAL = Pattern.compile("-?\\d+(?:\\.\\d+)?");
    private static final Pattern TYPE_PARAMETERS = Pattern.compile("\\((\\d+)(?:\\s*,\\s*(\\d+))?(?:\\s+(?:CHAR|BYTE))?\\)", Pattern.CASE_INSENSITIVE);
    private static final Map<String, String> READ_ONLY_COLUMNS = Map.of(
            "PROGRAM_ID", "Environment-specific program reference",
            "COMPONENT_ID", "Environment-specific component ID",
            "SYSTEM_ID", "Environment-specific system ID",
            "COMPONENT_PACKAGES", "Package membership metadata, managed by platform",
            "COMPONENTS_PACKAGE_ID", "Package membership, managed by platform"
    );

    public TableMetadata parse(String ddl) {
        Matcher create = CREATE_TABLE.matcher(ddl);
        if (!create.find()) {
            throw new IllegalArgumentException("No CREATE TABLE statement found");
        }
        String tableName = create.group(1).toUpperCase(Locale.ROOT);
        String body = balanced(ddl, create.end() - 1);
        Map<String, ColumnBuilder> columns = parseColumns(body);
        List<String> primaryKey = new ArrayList<>();
        List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();
        List<CheckConstraintMetadata> checks = new ArrayList<>();
        for (String part : split(body)) {
            Matcher foreignKey = FOREIGN_KEY.matcher(part);
            if (foreignKey.find()) {
                List<String> sourceColumns = identifiers(foreignKey.group(2));
                foreignKeys.add(new ForeignKeyMetadata(foreignKey.group(1).toUpperCase(Locale.ROOT), tableName,
                        sourceColumns, foreignKey.group(3).toUpperCase(Locale.ROOT), identifiers(foreignKey.group(4)),
                        sourceColumns.size() > 1, null));
                continue;
            }
            Matcher primary = PRIMARY_KEY.matcher(part);
            if (primary.find()) {
                primaryKey.addAll(identifiers(primary.group(1)));
                continue;
            }
            CheckConstraintMetadata check = parseCheck(part, columns);
            if (check != null) {
                checks.add(check);
            }
        }
        addSyntheticLabelReferences(tableName, columns, foreignKeys);
        applyComments(ddl, columns);
        applyTriggerMetadata(columns, triggerMetadata(ddl));
        return new TableMetadata(tableName, findDescription(ddl),
                columns.values().stream().map(ColumnBuilder::build).toList(), primaryKey,
                foreignKeys, checks, null);
    }

    private static Map<String, ColumnBuilder> parseColumns(String body) {
        Map<String, ColumnBuilder> columns = new LinkedHashMap<>();
        for (String part : split(body)) {
            if (part.trim().toUpperCase(Locale.ROOT).startsWith("CONSTRAINT")) {
                continue;
            }
            Matcher column = COLUMN.matcher(part.trim());
            if (!column.matches()) {
                continue;
            }
            String name = column.group(1).toUpperCase(Locale.ROOT);
            ColumnBuilder builder = new ColumnBuilder(name, column.group(2).toUpperCase(Locale.ROOT));
            applyTypeParameters(builder, column.group(3));
            String tail = column.group(4);
            builder.nullable = !tail.toUpperCase(Locale.ROOT).contains("NOT NULL");
            Matcher defaultValue = DEFAULT_VALUE.matcher(tail);
            if (defaultValue.find() && !defaultValue.group(2).equalsIgnoreCase("NULL")) {
                DefaultKind kind = defaultValue.group(1) == null ? DefaultKind.DEFAULT : DefaultKind.DEFAULT_ON_NULL;
                String rawValue = defaultValue.group(2);
                String expression = kind == DefaultKind.DEFAULT ? "DEFAULT " + rawValue : "DEFAULT ON NULL " + rawValue;
                builder.defaultValue = new DefaultMetadata(expression, literal(rawValue), kind);
                builder.source = new SourceMetadata(expression, false, false);
            }
            String readOnlyReason = READ_ONLY_COLUMNS.get(name);
            if (readOnlyReason != null) {
                builder.readOnly = true;
                builder.readOnlyReason = readOnlyReason;
                builder.source = new SourceMetadata(readOnlyReason, false, true);
            }
            columns.put(name, builder);
        }
        return columns;
    }

    private static void applyTypeParameters(ColumnBuilder builder, String parameters) {
        Matcher typeParameters = TYPE_PARAMETERS.matcher(parameters == null ? "" : parameters);
        if (!typeParameters.find()) {
            return;
        }
        int first = Integer.parseInt(typeParameters.group(1));
        if (isCharacterType(builder.oracleType)) {
            builder.maxLength = first;
        } else if (isNumericType(builder.oracleType)) {
            builder.precision = first;
            builder.scale = typeParameters.group(2) == null ? null : Integer.parseInt(typeParameters.group(2));
        }
    }

    private static CheckConstraintMetadata parseCheck(String part, Map<String, ColumnBuilder> columns) {
        String constraint = part.trim();
        Matcher prefix = CHECK_PREFIX.matcher(constraint);
        if (!prefix.find() || prefix.end() >= constraint.length() || constraint.charAt(prefix.end()) != '(') {
            return null;
        }
        String expression = balanced(constraint, prefix.end()).trim();
        List<String> referencedColumns = columns.keySet().stream()
                .filter(column -> Pattern.compile("\\b" + Pattern.quote(column) + "\\b", Pattern.CASE_INSENSITIVE)
                                         .matcher(expression).find())
                .toList();
        List<CheckConstraintMetadata.DerivedColumnConstraint> derived = deriveConstraint(expression);
        for (CheckConstraintMetadata.DerivedColumnConstraint value : derived) {
            ColumnBuilder column = columns.get(value.columnName());
            if (column != null) {
                column.minimum = value.minimum();
                column.maximum = value.maximum();
                column.notEqual = value.notEqual();
                column.allowedValues = value.allowedValues();
            }
        }
        return new CheckConstraintMetadata(prefix.group(1).toUpperCase(Locale.ROOT), expression,
                referencedColumns, derived);
    }

    private static List<CheckConstraintMetadata.DerivedColumnConstraint> deriveConstraint(String expression) {
        Matcher in = IN_CHECK.matcher(expression);
        if (in.matches()) {
            List<String> allowedValues = List.of(in.group(2).split(",")).stream()
                    .map(String::trim).map(OracleDdlParser::literal).toList();
            return List.of(new CheckConstraintMetadata.DerivedColumnConstraint(
                    in.group(1).toUpperCase(Locale.ROOT), null, null, null, allowedValues));
        }
        Matcher range = RANGE_CHECK.matcher(expression);
        if (range.matches()) {
            return List.of(new CheckConstraintMetadata.DerivedColumnConstraint(
                    range.group(1).toUpperCase(Locale.ROOT), literal(range.group(2)), literal(range.group(3)), null, List.of()));
        }
        Matcher notEqual = NOT_EQUAL_CHECK.matcher(expression);
        if (notEqual.matches()) {
            return List.of(new CheckConstraintMetadata.DerivedColumnConstraint(
                    notEqual.group(1).toUpperCase(Locale.ROOT), null, null, literal(notEqual.group(2)), List.of()));
        }
        return List.of();
    }

    private static void addSyntheticLabelReferences(String tableName, Map<String, ColumnBuilder> columns,
                                                     List<ForeignKeyMetadata> foreignKeys) {
        for (String column : columns.keySet()) {
            boolean hasForeignKey = foreignKeys.stream().anyMatch(key -> key.sourceColumns().contains(column));
            if (column.endsWith("_LABEL_ID") && !hasForeignKey) {
                foreignKeys.add(new ForeignKeyMetadata("SYNTHETIC_" + tableName + "_" + column,
                        tableName, List.of(column), "LABEL_PROGRAM",
                        List.of("LABEL_PROGRAM_ID", "APP_LANG_ID"), true, "UN1_LABEL_PROGRAM"));
            }
        }
    }

    private static void applyComments(String ddl, Map<String, ColumnBuilder> columns) {
        Matcher comments = COMMENT_COLUMN.matcher(ddl);
        while (comments.find()) {
            ColumnBuilder column = columns.get(comments.group(1).toUpperCase(Locale.ROOT));
            if (column != null) {
                column.description = unquote(comments.group(2));
            }
        }
    }

    private static void applyTriggerMetadata(Map<String, ColumnBuilder> columns,
                                             Map<String, TriggerMetadata> triggerMetadata) {
        triggerMetadata.forEach((columnName, trigger) -> {
            ColumnBuilder column = columns.get(columnName);
            if (column == null || column.defaultValue != null) {
                return;
            }
            if (trigger.sequence() != null) {
                String source = "Trigger: auto-increment from " + trigger.sequence();
                column.defaultValue = new DefaultMetadata(source, "0", DefaultKind.TRIGGER);
                column.source = new SourceMetadata(source, true, false);
                column.readOnly = true;
                column.readOnlyReason = "Auto-increment from " + trigger.sequence();
            } else if (trigger.defaultValue() != null) {
                String source = "Trigger: default = " + trigger.defaultValue();
                column.defaultValue = new DefaultMetadata(source, trigger.defaultValue(), DefaultKind.TRIGGER);
                column.source = new SourceMetadata(source, false, false);
            } else if (trigger.conditional()) {
                column.source = new SourceMetadata("Trigger: fills value conditionally", false, column.readOnly);
            }
        });
    }

    private static Map<String, TriggerMetadata> triggerMetadata(String ddl) {
        Map<String, TriggerMetadata> result = new LinkedHashMap<>();
        Matcher triggers = BEFORE_INSERT_TRIGGER.matcher(ddl);
        while (triggers.find()) {
            String body = triggers.group(1);
            applyAssignments(result, body, CONDITIONAL_ASSIGNMENT, true);
            applyAssignments(result, body, ASSIGNMENT, false);
            Matcher select = SELECT_SEQUENCE.matcher(body);
            while (select.find()) {
                result.putIfAbsent(select.group(2).toUpperCase(Locale.ROOT),
                        TriggerMetadata.generatedKey(select.group(1).toUpperCase(Locale.ROOT)));
            }
        }
        return result;
    }

    private static void applyAssignments(Map<String, TriggerMetadata> result, String body,
                                         Pattern pattern, boolean conditional) {
        Matcher assignments = pattern.matcher(body);
        while (assignments.find()) {
            String column = assignments.group(1).toUpperCase(Locale.ROOT);
            TriggerMetadata value = classifyAssignment(assignments.group(2).trim(), conditional);
            if (value != null && (conditional || !result.containsKey(column))) {
                result.put(column, value);
            }
        }
    }

    private static TriggerMetadata classifyAssignment(String value, boolean conditional) {
        Matcher sequence = SEQUENCE.matcher(value);
        TriggerMetadata result = null;
        if (sequence.matches()) {
            result = TriggerMetadata.generatedKey(sequence.group(1).toUpperCase(Locale.ROOT));
        } else if (!value.equalsIgnoreCase("NULL") && value.length() >= 2
                && value.startsWith("'") && value.endsWith("'")) {
            result = TriggerMetadata.defaultValue(unquote(value.substring(1, value.length() - 1)));
        } else if (NUMBER_LITERAL.matcher(value).matches()) {
            result = TriggerMetadata.defaultValue(value);
        } else if (conditional) {
            result = TriggerMetadata.conditionalFill();
        }
        return result;
    }

    private static GraphqlScalar scalar(String columnName, String oracleType, Integer precision,
                                        ConstraintMetadata constraints) {
        GraphqlScalar result;
        if (columnName.endsWith("_ID") && isNumericType(oracleType)) {
            result = GraphqlScalar.ID;
        } else if (isNumericType(oracleType) && constraints.allowedValues().size() == 2
                && constraints.allowedValues().containsAll(List.of("0", "1"))) {
            result = GraphqlScalar.BOOLEAN;
        } else if (List.of("INTEGER", "INT", "SMALLINT").contains(oracleType)
                || ("NUMBER".equals(oracleType) && precision != null && precision <= 9)) {
            result = GraphqlScalar.INT;
        } else if ("NUMBER".equals(oracleType) && precision == null) {
            result = GraphqlScalar.BIG_INT;
        } else if (isNumericType(oracleType)) {
            result = GraphqlScalar.DECIMAL;
        } else if ("DATE".equals(oracleType)) {
            result = GraphqlScalar.DATE;
        } else if (oracleType.startsWith("TIMESTAMP")) {
            result = GraphqlScalar.DATE_TIME;
        } else {
            result = GraphqlScalar.STRING;
        }
        return result;
    }

    private static boolean isNumericType(String oracleType) {
        return List.of("NUMBER", "INTEGER", "INT", "SMALLINT", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE")
                   .contains(oracleType);
    }

    private static boolean isCharacterType(String oracleType) {
        return List.of("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW").contains(oracleType);
    }

    private static String balanced(String value, int start) {
        int depth = 0;
        boolean quoted = false;
        for (int index = start; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '\'' && (index + 1 >= value.length() || value.charAt(index + 1) != '\'')) {
                quoted = !quoted;
            } else if (!quoted && character == '(') {
                depth++;
            } else if (!quoted && character == ')' && --depth == 0) {
                return value.substring(start + 1, index);
            }
        }
        throw new IllegalArgumentException("Unbalanced parenthesized expression");
    }

    private static List<String> split(String value) {
        List<String> values = new ArrayList<>();
        int depth = 0;
        int start = 0;
        boolean quoted = false;
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '\'' && (index + 1 >= value.length() || value.charAt(index + 1) != '\'')) {
                quoted = !quoted;
            } else if (!quoted && character == '(') {
                depth++;
            } else if (!quoted && character == ')') {
                depth--;
            } else if (!quoted && character == ',' && depth == 0) {
                values.add(value.substring(start, index));
                start = index + 1;
            }
        }
        values.add(value.substring(start));
        return values;
    }

    private static List<String> identifiers(String value) {
        return List.of(value.split(",")).stream()
                   .map(String::trim)
                   .map(identifier -> identifier.replace("\"", "").toUpperCase(Locale.ROOT))
                   .toList();
    }

    private static String findDescription(String ddl) {
        Matcher matcher = COMMENT_TABLE.matcher(ddl);
        return matcher.find() ? unquote(matcher.group(1)) : null;
    }

    private static String unquote(String value) {
        return value.replace("''", "'");
    }

    private static String literal(String value) {
        String result = value.trim();
        if (result.length() >= 2 && result.startsWith("'") && result.endsWith("'")) {
            result = unquote(result.substring(1, result.length() - 1));
        }
        return result;
    }

    private static class ColumnBuilder {
        private final String name;
        private final String oracleType;
        private boolean nullable = true;
        private Integer precision;
        private Integer scale;
        private Integer maxLength;
        private String description;
        private DefaultMetadata defaultValue;
        private SourceMetadata source;
        private boolean readOnly;
        private String readOnlyReason;
        private String minimum;
        private String maximum;
        private String notEqual;
        private List<String> allowedValues = List.of();

        private ColumnBuilder(String name, String oracleType) {
            this.name = name;
            this.oracleType = oracleType;
        }

        private ColumnMetadata build() {
            ConstraintMetadata constraints = new ConstraintMetadata(
                    minimum, maximum, notEqual, null, null, allowedValues);
            return new ColumnMetadata(name, oracleType, scalar(name, oracleType, precision, constraints),
                    nullable, precision, scale, maxLength, description, defaultValue, source,
                    readOnly, readOnlyReason, constraints, null);
        }
    }

    private record TriggerMetadata(String defaultValue, String sequence, boolean conditional) {
        private static TriggerMetadata generatedKey(String sequence) {
            return new TriggerMetadata(null, sequence, false);
        }

        private static TriggerMetadata defaultValue(String value) {
            return new TriggerMetadata(value, null, false);
        }

        private static TriggerMetadata conditionalFill() {
            return new TriggerMetadata(null, null, true);
        }
    }
}