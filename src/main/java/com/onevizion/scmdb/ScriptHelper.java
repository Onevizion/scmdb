package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbObject;
import com.onevizion.scmdb.vo.DbObjectType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ScriptHelper {

    public static String removeSpecialFromScriptText(String scriptText) {
        // Remove SQL single-line comments (-- ...)
        scriptText = scriptText.replaceAll("--.*\r*\n", "");
        // Remove SQL multi-line comments (/* ... */)
        scriptText = scriptText.replaceAll("/\\*([\\s\\S]*?)\\*/", "");
        // Replace newlines with single space
        scriptText = scriptText.replaceAll("\n+", " ");
        // Replace multiple whitespaces with single space
        scriptText = scriptText.replaceAll("\\s\\s+", " ");
        // Remove quotes
        scriptText = scriptText.replaceAll("\"", "");
        return scriptText.toLowerCase();
    }

    public static List<DbObject> findChangedDbObjectsInScriptText(String scriptText) {
        List<DbObject> dbObjects = new ArrayList<>();
        Matcher matcher;
        for (DbObjectType dbObjectType : DbObjectType.values()) {
            for (String keyword : dbObjectType.getChangeKeywords()) {

                String keywordRegexp = keyword + "\\s+\\w+";
                matcher = Pattern.compile(keywordRegexp).matcher(scriptText);
                while (matcher.find()) {
                    String objectName = matcher.group().replaceFirst(keyword + "\\s", "");
                    dbObjects.add(new DbObject(objectName, dbObjectType));
                }
                scriptText = scriptText.replaceAll(keywordRegexp, "");
            }
        }

        return dbObjects;
    }

    public static boolean isPackageScript(String scriptText) {
        if (scriptText == null || scriptText.isEmpty()) {
            return false;
        }

        String normalizedText = removeSpecialFromScriptText(scriptText);
        List<DbObject> dbObjects = findChangedDbObjectsInScriptText(normalizedText);

        return dbObjects.stream()
                       .anyMatch(obj -> obj.getType() == DbObjectType.PACKAGE_SPEC ||
                                       obj.getType() == DbObjectType.PACKAGE_BODY);
    }
}

