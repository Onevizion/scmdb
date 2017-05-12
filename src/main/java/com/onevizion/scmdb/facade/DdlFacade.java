package com.onevizion.scmdb.facade;

import com.onevizion.scmdb.AppArguments;
import com.onevizion.scmdb.ColorLogger;
import com.onevizion.scmdb.DdlGenerator;
import com.onevizion.scmdb.vo.DbObject;
import com.onevizion.scmdb.vo.DbObjectType;
import com.onevizion.scmdb.vo.SqlScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class DdlFacade {
    @Resource
    private DdlGenerator ddlGenerator;

    @Resource
    private AppArguments appArguments;

    @Resource
    private ColorLogger logger;


    public void generateDdl(List<SqlScript> scripts) {
        List<DbObject> changedDbObjects = findChangedDbObjects(scripts);
        ddlGenerator.executeSettingTransformParams();
        ddlGenerator.createDdlsForChangedDbObjects(changedDbObjects);
    }

    private List<DbObject> findChangedDbObjects(List<SqlScript> scripts) {
        List<DbObject> updatedDbObjects ;

        updatedDbObjects = scripts.stream()
                                  .map(script -> removeSpecialFromScriptText(script.getText()))
                                  .flatMap(scriptText -> findChangedDbObjectsInScriptText(scriptText).stream())
                                  .collect(Collectors.toList());

        System.out.println("!!!");
        String packageBody = "package_body";
        String packageSpec = "package_spec";
        String columnCommentKw = "comment on column";
        String tableCommentKw = "comment on table";

        /*for (String objType : objTypes) {
            String regexp = ".*?" + objType;
            for (int i = 0; i < dbObjectsTypes.size(); i++) {
                String dbObjectsType = dbObjectsTypes.get(i);
                if (dbObjectsType.matches(regexp)) {
                    dbObjectsType = dbObjectsType.replaceFirst(regexp, objType);
                } else if (columnCommentKw.equals(dbObjectsType) || tableCommentKw.equals(dbObjectsType)) {
                    dbObjectsType = "comment";
                } else {
                    continue;
                }

                if ("package body".equals(dbObjectsType)) {
                    dbObjectsType = packageBody;
                } else if ("package".equals(dbObjectsType)) {
                    dbObjectsType = packageSpec;
                }
                dbObjectsTypes.set(i, dbObjectsType);
            }

            for (int i = 0; i < delDbObjectsTypes.size(); i++) {
                String delDbObjectsType = delDbObjectsTypes.get(i);
                if (delDbObjectsType.matches(regexp)) {
                    delDbObjectsType = delDbObjectsType.replaceFirst(regexp, objType);
                } else {
                    continue;
                }

                if ("package body".equals(delDbObjectsType)) {
                    delDbObjectsType = packageBody;
                } else if ("package".equals(delDbObjectsType)) {
                    delDbObjectsType = packageSpec;
                }
                delDbObjectsTypes.set(i, delDbObjectsType);
            }
        }

        List<String> uniqueObjNames = new ArrayList<>();
        List<String> dbTypes = new ArrayList<>();
        boolean isUniqueObj;
        for (int i = 0; i < dbObjectsNames.size(); i++) {
            isUniqueObj = true;
            for (int j = i + 1; j < dbObjectsNames.size(); j++) {
                if (dbObjectsNames.get(i).equals(dbObjectsNames.get(j))
                        && dbObjectsTypes.get(i).equals(dbObjectsTypes.get(j))) {
                    isUniqueObj = false;
                    break;
                }
            }

            if (isUniqueObj) {
                uniqueObjNames.add(dbObjectsNames.get(i));
                dbTypes.add(dbObjectsTypes.get(i));
            }
        }

        List<String> extractDdlArr = new ArrayList<>();
        for (int i = 0; i < uniqueObjNames.size(); i++) {
            extractDdlArr.add(uniqueObjNames.get(i) + "|" + dbTypes.get(i));
        }

        for (int i = 0; i < delDbObjectsNames.size(); i++) {
            extractDdlArr.add(delDbObjectsNames.get(i) + "|" + delDbObjectsTypes.get(i));
        }*/

        //return extractDdlArr.toArray(new String[extractDdlArr.size()]);
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
