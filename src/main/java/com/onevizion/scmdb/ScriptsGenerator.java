package com.onevizion.scmdb;


import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.vo.DbObject;
import com.onevizion.scmdb.vo.DbObjectType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class ScriptsGenerator {
    @Resource
    private DdlDao dao;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String[] excludedSequences = {"SEQ_BPD_ITEMS_UNIT_ID"};
    private final String[] excludedPackages = {"PKGR_"};
    private final String[] excludedViews = {"VX_"};

    private final String packagesPathSuf = "/packages";
    private final String tablesPathSuf = "/tables";
    private final String viewsPathSuf = "/views";

    public void setConCacheProperties(String fileName) throws IOException {
        Properties props = new Properties();
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
            props.loadFromXML(inputStream);
            dao.setConCacheProperties(props);
        } catch (SQLException e) {
            logger.error(e.toString());
        }
    }

    public void executeSettingTransformParams() {
        dao.executeTransformParamStatements();
    }

    public void generatePackageSpecScripts(String outputDirectory, DbObject dbObject) throws IOException {
        String packagesDir = outputDirectory + packagesPathSuf;

        List<DbObject> packagesSpec;
        if (dbObject == null) {
            packagesSpec = dao.extractPackageSpecDdls();
        } else {
            packagesSpec = new ArrayList<DbObject>();
            packagesSpec.add(dbObject);
        }

        for (DbObject pkgSpec : packagesSpec) {
            if (!isExcludeObject(pkgSpec.getName(), excludedPackages)) {
                logger.info("Generating DDL for package spec [{}]", pkgSpec.getName());
                String ddl = removeSchemaNameInDdl(pkgSpec.getDdl());
                ddl = ddl.trim();
                ddl = ddl.replaceFirst("\\s+/$", "\n/");
                ddl = ddl.replaceAll("\\n", "\r\n");
                pkgSpec.setDdl(ddl);
                File file = new File(packagesDir + "/" + pkgSpec.getName().toLowerCase() + "_spec.sql");
                FileUtils.write(file, pkgSpec.getDdl(), false);
            }
        }
    }

    public void generatePackageBodyScripts(String outputDirectory, DbObject dbObject) throws IOException {
        String packagesDir = outputDirectory + packagesPathSuf;

        List<DbObject> packagesBodies;
        if (dbObject == null) {
            packagesBodies = dao.extractPackageBodiesDdls();
        } else {
            packagesBodies = new ArrayList<DbObject>();
            packagesBodies.add(dbObject);
        }

        for (DbObject pkgBody : packagesBodies) {
            if (!isExcludeObject(pkgBody.getName(), excludedPackages)) {
                logger.info("Generating DDL for package body [{}]", pkgBody.getName());
                String ddl = removeSchemaNameInDdl(pkgBody.getDdl());
                ddl = ddl.trim();
                ddl = ddl.replaceFirst("\\s+/$", "\n/");
                ddl = ddl.replaceAll("\\n", "\r\n");
                pkgBody.setDdl(ddl);
                File file = new File(packagesDir + "/" + pkgBody.getName().toLowerCase() + ".sql");
                FileUtils.write(file, pkgBody.getDdl(), false);
            }
        }
    }

    public void generateTableScripts(String outputDirectory, DbObject dbObject) throws IOException {
        String tablesDir = outputDirectory + tablesPathSuf;

        List<DbObject> tables;
        if (dbObject == null) {
            tables = dao.extractTablesDdls();
        } else {
            tables = new ArrayList<DbObject>();
            tables.add(dbObject);
        }

        for (DbObject table : tables) {
            logger.info("Generating DDL for table [{}]", table.getName());
            String ddl = removeSchemaNameInDdl(table.getDdl());
            ddl = ddl.trim();
            ddl = ddl.replaceAll("\\s+;", ";");
            ddl = ddl.replaceFirst("\\n\\s+\\(", "(\n");
            ddl = ddl.replaceFirst("\\s+\\)", "\n)");
            ddl = ddl.replaceAll("\\n", "\r\n");
            ddl = ddl.replaceAll("\\t", "    ");
            ddl = ddl.replaceAll("\\r\\n\\s+REFERENCES\\s", " REFERENCES ");
            table.setDdl(ddl);
            File file = new File(tablesDir + "/" + table.getName().toLowerCase() + ".sql");
            FileUtils.write(file, table.getDdl(), false);
        }
        generateTabCommentsScripts(tablesDir, dbObject);
        generateIndexScripts(tablesDir, dbObject);
        generateSequenceScripts(tablesDir, dbObject);
        generateTriggerScripts(tablesDir, dbObject);
    }

    private void generateTabCommentsScripts(String tablesDir, DbObject table) throws IOException {
        logger.info("Adding comments...");
        List<DbObject> comments;
        if (table == null) {
            comments = dao.extractTabCommentsDdls();
        } else {
            comments = dao.extractTableDependentObjectsDdl(table.getName(), DbObjectType.COMMENT.toString());
        }
        for (DbObject comment : comments) {
            String ddl = removeSchemaNameInDdl(comment.getDdl());
            ddl = ddl.trim();

            Pattern pattern = Pattern.compile("COMMENT ON TABLE.+", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(ddl);
            if (matcher.find()) {
                String commentStmt = matcher.group();
                ddl = ddl.replaceFirst("COMMENT ON TABLE.+", "");
                ddl = commentStmt + "\r\n" + ddl;
            }

            ddl = ddl.replaceAll("\\n", "");
            ddl = ddl.replaceAll("\\s+COMMENT", "COMMENT");
            ddl = ddl.replaceAll("COMMENT", "\r\nCOMMENT");
            ddl = ddl.trim();
            ddl = "\r\n\r\n" + ddl;
            comment.setDdl(ddl);
            File file = new File(tablesDir + "/" + comment.getName().toLowerCase() + ".sql");
            FileUtils.write(file, comment.getDdl(), true);
        }
    }

    private void generateIndexScripts(String tablesDir, DbObject table) throws IOException {
        logger.info("Adding indexes...");
        List<DbObject> indexes;
        if (table == null) {
            indexes = dao.extractIndexesDdls();
        } else {
            indexes = dao.extractTableDependentObjectsDdl(table.getName(), DbObjectType.INDEX.toString());
        }
        for (int i = 0; i < indexes.size(); i++) {
            DbObject index = indexes.get(i);
            String ddl = removeSchemaNameInDdl(index.getDdl());
            ddl = ddl.trim();
            if (i == 0) {
                ddl = "\r\n\r\n" + ddl;
            } else {
                ddl = "\r\n" + ddl;
            }
            ddl = ddl.replaceAll("\\s+;$", ";");
            index.setDdl(ddl);
            File file = new File(tablesDir + "/" + index.getName().toLowerCase() + ".sql");
            FileUtils.write(file, index.getDdl(), true);
        }
    }

    private void generateSequenceScripts(String tablesDir, DbObject table) throws IOException {
        logger.info("Adding sequences...");
        List<DbObject> sequences;
        if (table == null) {
            sequences = dao.extractSequencesDdls();
        } else {
            sequences = dao.extractTableDependentObjectsDdl(table.getName(), DbObjectType.SEQUENCE.toString());
        }
        for (DbObject seq : sequences) {
            String ddl = removeSchemaNameInDdl(seq.getDdl());
            boolean isExcludable = false;
            for (String exclSeq : excludedSequences) {
                if (ddl.contains(exclSeq)) {
                    isExcludable = true;
                    break;
                }
            }
            if (isExcludable) {
                continue;
            }

            ddl = ddl.trim();
            ddl = "\r\n" + ddl;
            int index = ddl.lastIndexOf("\"");
            if (index != -1) {
                ddl = ddl.substring(0, index + 1);
                ddl += ";";
            } else {
                ddl = ddl.replaceAll("\\s+;$", ";");
            }
            seq.setDdl(ddl);
            File file = new File(tablesDir + "/" + seq.getName().toLowerCase() + ".sql");
            FileUtils.write(file, seq.getDdl(), true);
        }
    }

    private void generateTriggerScripts(String tablesDir, DbObject table) throws IOException {
        logger.info("Adding triggers...");
        List<DbObject> triggers;
        if (table == null) {
            triggers = dao.extractTriggersDdls();
        } else {
            triggers = dao.extractTableDependentObjectsDdl(table.getName(), DbObjectType.TRIGGER.toString());
        }
        for (DbObject trigger : triggers) {
            String ddl = removeSchemaNameInDdl(trigger.getDdl());
            ddl = ddl.trim();
            ddl = ddl.replaceAll("\\n", "\r\n");
            ddl = "\r\n" + ddl;
            ddl = ddl.replaceAll("\\s+/", "\r\n/");
            ddl = ddl.replaceFirst("ALTER TRIGGER \"\\w+\" ENABLE;", "");
            trigger.setDdl(ddl);
            File file = new File(tablesDir + "/" + trigger.getName().toLowerCase() + ".sql");
            FileUtils.write(file, trigger.getDdl(), true);
        }
    }

    public void generateViewScripts(String outputDirectory, DbObject dbObject) throws IOException {
        String viewsDir = outputDirectory + viewsPathSuf;

        List<DbObject> views;
        if (dbObject == null) {
            views = dao.extractViewsDdls();
        } else {
            views = new ArrayList<DbObject>();
            views.add(dbObject);
        }

        for (DbObject view : views) {
            if (!isExcludeObject(view.getName(), excludedViews)) {
                logger.info("Generating DDL for view [{}]", view.getName());
                String ddl = removeSchemaNameInDdl(view.getDdl());
                ddl = ddl.trim();
                ddl = ddl.replaceAll("\\s+;", ";");
                ddl = ddl.replaceAll("\\n", "\r\n");
                view.setDdl(ddl);
                File file = new File(viewsDir + "/" + view.getName().toLowerCase() + ".sql");
                FileUtils.write(file, view.getDdl(), false);
            }
        }

        generateViewCommentsScripts(viewsDir, dbObject);
    }

    private void generateViewCommentsScripts(String viewsDir, DbObject view) throws IOException {
        logger.info("Adding views comments...");
        List<DbObject> comments;
        if (view == null) {
            comments = dao.extractViewCommentsDdls();
        } else {
            comments = dao.extractTableDependentObjectsDdl(view.getName(), DbObjectType.COMMENT.toString());
        }
        for (DbObject comment : comments) {
            String ddl = removeSchemaNameInDdl(comment.getDdl());
            ddl = ddl.replaceAll("\\s+COMMENT", "\r\nCOMMENT");
            ddl = ddl.replaceFirst("COMMENT", "\r\nCOMMENT");
            comment.setDdl(ddl);
            File file = new File(viewsDir + "/" + comment.getName().toLowerCase() + ".sql");
            FileUtils.write(file, comment.getDdl(), true);
        }
    }

    public void generateDbObjectScripts(String outputDirectory, String[] dbObjects) throws IOException {
        Pattern pattern = Pattern.compile("\\W");
        Map<String, String> tables = new HashMap<String, String>();
        for (String object : dbObjects) {
            object = object.toUpperCase();
            Matcher matcher = pattern.matcher(object);
            if (!matcher.find()) {
                logger.error("Can not find separator in this arg: {}" +
                        " Separator can be symbol by regexp: '\\W'", object);
                return;
            }
            String separator = matcher.group(0);
            int sepIndex = object.indexOf(separator);

            String name = object.substring(0, sepIndex);
            if (name.isEmpty()) {
                logger.error("Incorrect arg: {}. Name of db object can not be empty", object);
                return;
            }

            String type = object.substring(sepIndex + 1);
            if (type.isEmpty()) {
                logger.error("Incorrect db type in this arg: {}", object);
                return;
            }

            if(DbObjectType.COMMENT.toString().equals(type)){
                type = dao.getObjectTypeByName(name);
            }

            if (DbObjectType.INDEX.toString().equals(type)
                    || DbObjectType.TRIGGER.toString().equals(type)) {
                String tableName = getTableNameByDepObject(name);
                if (dao.isExist(tableName, DbObjectType.TABLE.toString())) {
                    tables.put(tableName, DbObjectType.TABLE.toString());
                } else {
                    logger.warn("Parent object not found for {} {}! Please, modify related DDL manually.", type, name);
                }
            } else if (DbObjectType.SEQUENCE.toString().equals(type)) {
                String tableName = dao.getTableNameByDepObject(name, type);
                if (StringUtils.isNotBlank(tableName)) {
                    tables.put(tableName, DbObjectType.TABLE.toString());
                } else {
                    logger.warn("Parent object not found for {} {}! Please, modify related DDL manually.", type, name);
                }
            } else if (DbObjectType.TABLE.toString().equals(type)) {
                if (!checkAndDeleteRedundantDdl(outputDirectory, name, type)) {
                    tables.put(name, DbObjectType.TABLE.toString());
                }
            } else {
                if (!checkAndDeleteRedundantDdl(outputDirectory, name, type)) {
                    String ddl = dao.extractDdlByNameAndType(name, type);
                    DbObject dbObject = new DbObject();
                    dbObject.setName(name);
                    dbObject.setDdl(ddl);
                    if (DbObjectType.PACKAGE_BODY.toString().equals(type)) {
                        generatePackageBodyScripts(outputDirectory, dbObject);
                    } else if (DbObjectType.PACKAGE_SPEC.toString().equals(type)) {
                        generatePackageSpecScripts(outputDirectory, dbObject);
                    } else if (DbObjectType.VIEW.toString().equals(type)) {
                        generateViewScripts(outputDirectory, dbObject);
                    }
                }
            }
        }
        DbObject dbObject = new DbObject();
        Set<String> keys = tables.keySet();
        for (String key : keys) {
            if (!key.isEmpty()) {
                String ddl = dao.extractDdlByNameAndType(key, tables.get(key));
                dbObject.setName(key);
                dbObject.setDdl(ddl);
                generateTableScripts(outputDirectory, dbObject);
            }
        }
    }

    private String getTableNameByDepObject(String name) {
        int posNumber = name.indexOf("_");
        String tableName = name.substring(posNumber + 1);
        return tableName;
    }

    public String removeSchemaNameInDdl(String ddl) {
        String schemaName = dao.getSchemaName();
        String regexp = String.format("\"%s\".", schemaName.toUpperCase());
        ddl = ddl.replaceAll(regexp, "");
        return ddl;
    }

    private boolean isExcludeObject(String objectName, String[] excludedObjects) {
        for (String exclPackageName : excludedObjects) {
            if (objectName.toUpperCase().startsWith(exclPackageName.toUpperCase())) {
                return true;
            }
        }
        return false;
    }

    private boolean checkAndDeleteRedundantDdl(String outputDir, String dbObjectName, String type) throws IOException {
        boolean isDeletePackSpecWithBody = false;
        File fileDir = null;
        String fileName = null;
        if (!dao.isExist(dbObjectName, type)) {
            if (DbObjectType.PACKAGE_BODY.toString().equals(type)) {
                fileDir = new File(outputDir + packagesPathSuf);
                fileName = dbObjectName + ".sql";
            } else if (DbObjectType.PACKAGE_SPEC.toString().equals(type)) {
                fileDir = new File(outputDir + packagesPathSuf);
                fileName = dbObjectName + "_spec.sql";
                isDeletePackSpecWithBody = true;
            } else if (DbObjectType.TABLE.toString().equals(type)) {
                fileDir = new File(outputDir + tablesPathSuf);
                fileName = dbObjectName + ".sql";
            } else if (DbObjectType.VIEW.toString().equals(type)) {
                fileDir = new File(outputDir + viewsPathSuf);
                fileName = dbObjectName + ".sql";
            }
        }

        if (fileName != null && fileDir != null) {
            deleteFilteredFiles(fileDir, fileName);
            if(isDeletePackSpecWithBody){
                String packBodyName = dbObjectName + ".sql";
                deleteFilteredFiles(fileDir, packBodyName);
            }
            return true;
        }
        return false;
    }

    private void deleteFilteredFiles(File fileDir, String fileName) throws IOException {
        FileFilter filter = new WildcardFileFilter(fileName, IOCase.INSENSITIVE);
        File[] filteredFiles = fileDir.listFiles(filter);
        if (filteredFiles != null && filteredFiles.length > 0) {
            for (File file : filteredFiles) {
                FileUtils.forceDeleteOnExit(file);
            }
        }
    }
}