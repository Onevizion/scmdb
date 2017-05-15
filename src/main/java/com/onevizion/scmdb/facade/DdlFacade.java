package com.onevizion.scmdb.facade;

import com.onevizion.scmdb.DdlGenerator;
import com.onevizion.scmdb.vo.DbObject;
import com.onevizion.scmdb.vo.DbObjectType;
import com.onevizion.scmdb.vo.SqlScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class DdlFacade {
    @Resource
    private DdlGenerator ddlGenerator;

    public void generateDdl(List<SqlScript> scripts) {
        Set<DbObject> changedDbObjects = findChangedDbObjects(scripts);
        ddlGenerator.executeSettingTransformParams();
        ddlGenerator.createDdlsForChangedDbObjects(changedDbObjects);
    }

    private Set<DbObject> findChangedDbObjects(List<SqlScript> scripts) {
        Set<DbObject> updatedDbObjects;

        updatedDbObjects = scripts.stream()
                                  .map(script -> removeSpecialFromScriptText(script.getText()))
                                  .flatMap(scriptText -> findChangedDbObjectsInScriptText(scriptText).stream())
                                  .distinct()
                                  .collect(Collectors.toSet());
        return updatedDbObjects;
    }

    private List<DbObject> findChangedDbObjectsInScriptText(String scriptText) {
        List<DbObject> dbObjects = new ArrayList<>();
        Matcher matcher;
        for (DbObjectType dbObjectType : DbObjectType.values()) {
            for (String keyword : dbObjectType.getChangeKeywords()) {

                String keywordRegexp = keyword + "\\s+\\w+";
                matcher = Pattern.compile(keywordRegexp).matcher(scriptText);
                if (!matcher.find()) {
                    continue;
                }
                scriptText = scriptText.replaceAll(keywordRegexp, "");
                matcher.reset();
                while (matcher.find()) {
                    String objectName = matcher.group().replaceFirst(keyword + "\\s", "");
                    dbObjects.add(new DbObject(objectName, dbObjectType));
                }
            }
        }

        return dbObjects;
    }

    private String removeSpecialFromScriptText(String scriptText) {
        scriptText = scriptText.replaceAll("--.*\r*\n", "");
        scriptText = scriptText.replaceAll("/\\*([\\s\\S]*?)\\*/", "");
        scriptText = scriptText.replaceAll("\n+", " ");
        scriptText = scriptText.replaceAll("\\s\\s+", " ");
        scriptText = scriptText.replaceAll("\"", "");
        return scriptText.toLowerCase();
    }
}
