package com.onevizion.scmdb;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.lang.NonNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that has methods for working with formatting messages.
 */
public final class StringPlaceholderUtils {

    private static final String PREFIX = "{";
    private static final String SUFFIX = "}";
    private static final String PLACEHOLDER_REGEXP = "\\" + PREFIX + "([\\w]((?!(\\" + PREFIX + ")|(\\\")).)*[\\w])\\" + SUFFIX;
    private static final Pattern PLACEHOLDER_REGEXP_PATTERN = Pattern.compile(PLACEHOLDER_REGEXP);

    private StringPlaceholderUtils() {
    }

    /**
     * The method replaces {placeholder} in the original message with the value from the Map by key equal to the placeholder name.
     * For example: in the source "Email originated from {link}" with Map(key = "link", value = "any message"),
     * the result is "Email originated from any message".
     * @param source original message
     * @param placeholders Map with values for placeholder.
     * @return Formatted message.
     */
    public static String replace(String source, Map<String, Object> placeholders) {
        if (MapUtils.isNotEmpty(placeholders) && StringUtils.isNotBlank(source)) {
            StringSubstitutor sub = new StringSubstitutor(placeholders, PREFIX, SUFFIX);
            return sub.replace(source);
        } else {
            return source;
        }
    }

    /**
     * The method replaces "{}" in the original message with arguments in order.
     * For example: in the source "a{}.parent_id = :p_id{}" with fieldId = 15 and fieldId = 21,
     * the result is "a.15.parent_id =: p_id21".
     * @param source original message
     * @param params arguments that will replace placeholders in order
     * @return Formatted message.
     */
    public static String replace(String source, Object... params) {
        return MessageFormatter.arrayFormat(source, params).getMessage();
    }

    /**
     * The method returns a Set of placeholders from "source" param.
     * For example: in the source "Invalid JSON at line: {lineNr}, column {columnNr}. {error}",
     * Set will contain {lineNr}, {columnNr}, {error}.
     * @param source original message
     * @return Set of placeholders.
     */
    public static Set<String> getPlaceholdersWithSuffixAndPrefix(String source) {
        if (StringUtils.isBlank(source)) {
            return Set.of();
        }
        Set<String> placeholders = new HashSet<>();
        Matcher m = PLACEHOLDER_REGEXP_PATTERN.matcher(source);
        while (m.find()) {
            placeholders.add(m.group());
        }
        return placeholders;
    }

    public static Set<String> getPlaceholders(String source) {
        if (StringUtils.isBlank(source)) {
            return Set.of();
        }
        Set<String> placeholders = new HashSet<>();
        Matcher m = PLACEHOLDER_REGEXP_PATTERN.matcher(source);
        while (m.find()) {
            placeholders.add(m.group(1));
        }
        return placeholders;
    }

    public static String buildPlaceholderWithName(@NonNull String name) {
        return PREFIX + Objects.requireNonNull(name) + SUFFIX;
    }
}
