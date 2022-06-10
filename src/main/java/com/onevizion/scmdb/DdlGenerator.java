package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.vo.DbObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.ColorLogger.Color.RED;
import static com.onevizion.scmdb.vo.DbObjectType.*;
import static com.onevizion.scmdb.vo.SchemaType.OWNER;

@Component
public class DdlGenerator {
    private static final String PACKAGE_SPECIFICATION_DDL_FILE_POSTFIX = "_spec";
    private static final String EDITIONABLE_MODIFIER = "EDITIONABLE ";
    private static final String NOEDITIONABLE_MODIFIER = "NONEDITIONABLE ";
    private static final String PK_CONSTRAINT_INDEX_POSTFIX = "\n  USING INDEX  ENABLE";
    private static final String CONSTRAINT_PATTERN = "^\\s*CONSTRAINT\\s.*\\r*\\n*";

    @Autowired
    private DdlDao ddlDao;

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private ColorLogger logger;

    private final String[] excludedSequences = {"SEQ_BPD_ITEMS_UNIT_ID"};
    private final String[] excludedPackages = {"PKGR_"};
    private final String[] excludedViews = {"VX_"};

    private static final String PACKAGES_DDL_DIRECTORY_NAME = "packages";
    private static final String TABLES_DDL_DIRECTORY_NAME = "tables";
    private static final String VIEWS_DDL_DIRECTORY_NAME = "views";

    public void executeSettingTransformParams() {
        ddlDao.executeTransformParamStatements();
    }

    private void generatePackageSpecScripts(DbObject pkgSpec) {
        if (!isExcludeObject(pkgSpec.getName(), excludedPackages)) {
            logger.info("Generating DDL for package spec [{}]", GREEN, pkgSpec.getName());
            String ddl = removeSchemaNameInDdl(pkgSpec.getDdl());
            ddl = ddl.trim();
            ddl = ddl.replaceFirst("\\s+/$", "\n/");
            ddl = ddl.replaceAll("\\n", "\r\n");
            pkgSpec.setDdl(ddl);
            prepareAndWriteDdlToFile(pkgSpec, PACKAGES_DDL_DIRECTORY_NAME);
        }
    }

    private void generatePackageBodyScripts(DbObject pkgBody) {
        if (!isExcludeObject(pkgBody.getName(), excludedPackages)) {
            logger.info("Generating DDL for package body [{}]", GREEN, pkgBody.getName());
            String ddl = removeSchemaNameInDdl(pkgBody.getDdl());
            ddl = ddl.trim();
            ddl = ddl.replaceFirst("\\s+/$", "\n/");
            ddl = ddl.replaceAll("\\n", "\r\n");
            pkgBody.setDdl(ddl);
            prepareAndWriteDdlToFile(pkgBody, PACKAGES_DDL_DIRECTORY_NAME);
        }
    }

    private void generateTableScripts(DbObject table) {
        logger.info("Generating DDL for table [{}]", GREEN, table.getName());
        String ddl = removeSchemaNameInDdl(table.getDdl());
        ddl = ddl.trim();
        ddl = ddl.replaceAll(PK_CONSTRAINT_INDEX_POSTFIX, "");
        ddl = ddl.replaceAll("\\s+;", ";");
        ddl = ddl.replaceFirst("\\n\\s+\\(", "(\n");
        ddl = ddl.replaceFirst("\\s+\\)", "\n)");
        ddl = ddl.replaceAll("\\n", "\r\n");
        ddl = ddl.replaceAll("\\t", "    ");
        ddl = ddl.replaceAll("\\r\\n\\s+REFERENCES\\s", " REFERENCES ");
        ddl = sortConstraintInScript(ddl);
        ddl += generateTableCommentsDdl(table);
        ddl += generateIndexScripts(table);
        ddl += generateSequenceScripts(table);
        ddl += generateTriggerScripts(table);
        table.setDdl(ddl);
        prepareAndWriteDdlToFile(table, TABLES_DDL_DIRECTORY_NAME);
    }

    private String applyCodeStyleFormattingToDdl(String ddl) {
        ddl = removeEditionableObjectsModifiers(ddl);
        return removeDoubleQuotesAroundObjectNames(ddl);
    }

    private String removeEditionableObjectsModifiers(String ddl) {
        ddl = ddl.replaceAll(NOEDITIONABLE_MODIFIER, "");
        return ddl.replaceAll(EDITIONABLE_MODIFIER, "");
    }

    private String removeDoubleQuotesAroundObjectNames(String ddl) {
        boolean isSingleQuoteOpened = false;
        boolean isSingleLineCommentStarted = false;
        boolean isMultiLineCommentStarted = false;
        char previousSymbol = '\n';
        StringBuilder result = new StringBuilder();
        for (char symbol : ddl.toCharArray()) {
            if (symbol == '-' && previousSymbol == '-' && !(isMultiLineCommentStarted || isSingleQuoteOpened)) {
                isSingleLineCommentStarted = true;
            } else if (symbol == '*' && previousSymbol == '/') {
                isMultiLineCommentStarted = true;
            } else if (symbol == '/' && previousSymbol == '*') {
                isMultiLineCommentStarted = false;
            } else if (symbol == '\n') {
                isSingleLineCommentStarted = false;
            } else if (symbol == '\'' && !(isSingleLineCommentStarted || isMultiLineCommentStarted)) {
                isSingleQuoteOpened = !isSingleQuoteOpened;
            }

            if (isSingleQuoteOpened || isSingleLineCommentStarted || isMultiLineCommentStarted || symbol != '"') {
                result.append(symbol);
            }

            previousSymbol = symbol;
        }

        String unquotedDdl = result.toString();

        return unquotedDdl.replaceAll("CASE,", "\"CASE\",").replaceAll("\\(CASE\\)","(\"CASE\")");
    }

    private void prepareAndWriteDdlToFile(DbObject dbObject, String ddlDirectoryName) {
        dbObject.setDdl(applyCodeStyleFormattingToDdl(dbObject.getDdl()));

        String directoryPath = appArguments.getDdlsDirectory().getAbsolutePath() + File.separator + ddlDirectoryName;

        String filePath = directoryPath + File.separator + dbObject.getName().toLowerCase();
        if (dbObject.getType() == PACKAGE_SPEC) {
            filePath += PACKAGE_SPECIFICATION_DDL_FILE_POSTFIX;
        }
        filePath += ".sql";
        File file = new File(filePath);
        try {
            FileUtils.write(file, dbObject.getDdl(), "UTF-8", false);
        } catch (IOException e) {
            throw new RuntimeException("Can't write ddl to file[" + file.getAbsolutePath() + "]", e);
        }
    }

    private String generateTableCommentsDdl(DbObject table) {
        logger.info("Adding comments...");
        List<DbObject> comments = ddlDao.extractTableDependentObjectsDdl(table.getName(), COMMENT);
        StringBuilder commentsDdl = new StringBuilder();
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
            ddl = ddl.replaceAll(";", ";\r\n");
            ddl = ddl.trim();
            ddl = "\r\n\r\n" + ddl;
            commentsDdl.append(ddl);
        }
        return commentsDdl.toString();
    }

    private String generateIndexScripts(DbObject table) {
        logger.info("Adding indexes...");
        List<DbObject> indexes = ddlDao.extractTableDependentObjectsDdl(table.getName(), INDEX);
        indexes.sort(Comparator.comparing(DbObject::getDdl));
        StringBuilder indexesDdl = new StringBuilder();
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
            indexesDdl.append(ddl);
        }
        indexesDdl.append("\r\n");
        return indexesDdl.toString();
    }

    private String generateSequenceScripts(DbObject table) {
        logger.info("Adding sequences...");
        List<DbObject> sequences = ddlDao.extractTableDependentObjectsDdl(table.getName(), SEQUENCE);
        StringBuilder sequencesDdl = new StringBuilder();
        for (DbObject sequence : sequences) {
            String ddl = removeSchemaNameInDdl(sequence.getDdl());
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
            sequencesDdl.append(ddl);
        }
        sequencesDdl.append("\r\n");
        return sequencesDdl.toString();
    }

    private String generateTriggerScripts(DbObject table) {
        logger.info("Adding triggers...");
        List<DbObject> triggers = ddlDao.extractTableDependentObjectsDdl(table.getName(), TRIGGER);
        StringBuilder triggersDdl = new StringBuilder();
        for (DbObject trigger : triggers) {
            String ddl = removeSchemaNameInDdl(trigger.getDdl());
            ddl = ddl.trim();
            ddl = ddl.replaceAll("\\n", "\r\n");
            ddl = "\r\n" + ddl;
            ddl = ddl.replaceAll("\\s+/", "\r\n/");
            ddl = ddl.replaceFirst("ALTER TRIGGER \"\\w+\" ENABLE;", "");
            triggersDdl.append(ddl);
        }
        return triggersDdl.toString();
    }

    private void generateViewScripts(DbObject view) {
        if (!isExcludeObject(view.getName(), excludedViews)) {
            logger.info("Generating DDL for view [{}]", GREEN, view.getName());
            String ddl = removeSchemaNameInDdl(view.getDdl());
            ddl = ddl.trim();
            ddl = ddl.replaceAll("\\s+;", ";");
            ddl = ddl.replaceAll("\\n", "\r\n");
            ddl += generateViewCommentsScripts(view);
            view.setDdl(ddl);
            prepareAndWriteDdlToFile(view, VIEWS_DDL_DIRECTORY_NAME);
        }
    }

    private String generateViewCommentsScripts(DbObject view) {
        logger.info("Adding views comments...");
        List<DbObject> comments = ddlDao.extractTableDependentObjectsDdl(view.getName(), COMMENT);
        StringBuilder commentsDdl = new StringBuilder();
        for (DbObject comment : comments) {
            String ddl = removeSchemaNameInDdl(comment.getDdl());
            ddl = ddl.replaceAll("\\s+COMMENT", "\r\nCOMMENT");
            ddl = ddl.replaceFirst("COMMENT", "\r\nCOMMENT");
            commentsDdl.append(ddl);
        }
        return commentsDdl.toString();
    }

    public void generateDdls(Collection<DbObject> dbObjects, boolean skipGenDdlForDepObject) {
        Set<DbObject> tables = new HashSet<>();
        for (DbObject dbObject : dbObjects) {
            if (dbObject.getType() == COMMENT && !skipGenDdlForDepObject) {
                dbObject.setType(ddlDao.getObjectTypeByName(dbObject.getName()));
            }

            if ((dbObject.getType() == INDEX || dbObject.getType() == TRIGGER) && !skipGenDdlForDepObject) {
                String tableName = ddlDao.getTableNameByDepObject(dbObject);
                if (ddlDao.isExist(tableName, TABLE)) {
                    tables.add(new DbObject(tableName, TABLE));
                } else {
                    logger.warn("Parent object not found for {} {}! Please, modify related DDL manually.", RED,
                            dbObject.getType(), dbObject.getName());
                }
            } else if (dbObject.getType() == SEQUENCE && !skipGenDdlForDepObject) {
                String tableName = ddlDao.getTableNameByDepObject(dbObject);
                if (StringUtils.isNotBlank(tableName)) {
                    tables.add(new DbObject(tableName, TABLE));
                } else {
                    logger.warn("Parent object not found for {} {}! Please, modify related DDL manually.", RED,
                            dbObject.getType(), dbObject.getName());
                }
            } else if (dbObject.getType() == TABLE) {
                if (!checkAndDeleteRedundantDdl(dbObject)) {
                    tables.add(dbObject);
                }
            } else {
                if (!checkAndDeleteRedundantDdl(dbObject)) {
                    dbObject.setDdl(ddlDao.extractDdl(dbObject));
                    if (dbObject.getType() == PACKAGE_BODY) {
                        generatePackageBodyScripts(dbObject);
                    } else if (dbObject.getType() == PACKAGE_SPEC) {
                        generatePackageSpecScripts(dbObject);
                    } else if (dbObject.getType() == VIEW) {
                        generateViewScripts(dbObject);
                    }
                }
            }
        }
        for (DbObject table : tables) {
            table.setDdl(ddlDao.extractDdl(table));
            generateTableScripts(table);
        }
    }

    private String removeSchemaNameInDdl(String ddl) {
        String regexp = String.format("\"%s\".", appArguments.getDbCredentials(OWNER).getSchemaName().toUpperCase());
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

    private boolean checkAndDeleteRedundantDdl(DbObject dbObject) {
        String ddlsDirectoryPath = appArguments.getDdlsDirectory().getAbsolutePath();
        boolean isDeletePackageSpecWithBody = false;
        File fileDir = null;
        String fileName = null;
        if (!ddlDao.isExist(dbObject.getName(), dbObject.getType())) {
            if (dbObject.getType() == PACKAGE_BODY) {
                fileDir = new File(appArguments.getDdlsDirectory() + PACKAGES_DDL_DIRECTORY_NAME);
                fileName = dbObject.getName() + ".sql";
            } else if (dbObject.getType() == PACKAGE_SPEC) {
                fileDir = new File(ddlsDirectoryPath + File.separator + PACKAGES_DDL_DIRECTORY_NAME);
                fileName = dbObject.getName() + "_spec.sql";
                isDeletePackageSpecWithBody = true;
            } else if (dbObject.getType() == TABLE) {
                fileDir = new File(ddlsDirectoryPath + File.separator + TABLES_DDL_DIRECTORY_NAME);
                fileName = dbObject.getName() + ".sql";
            } else if (dbObject.getType() == VIEW) {
                fileDir = new File(ddlsDirectoryPath + File.separator + VIEWS_DDL_DIRECTORY_NAME);
                fileName = dbObject.getName() + ".sql";
            }
        }

        if (fileName != null) {
            deleteFilteredFiles(fileDir, fileName);
            if (isDeletePackageSpecWithBody) {
                String packBodyName = dbObject.getName() + ".sql";
                deleteFilteredFiles(fileDir, packBodyName);
            }
            return true;
        }
        return false;
    }

    private void deleteFilteredFiles(File fileDir, String fileName) {
        FileFilter filter = new WildcardFileFilter(fileName, IOCase.INSENSITIVE);
        File[] filteredFiles = fileDir.listFiles(filter);
        if (filteredFiles != null && filteredFiles.length > 0) {
            for (File file : filteredFiles) {
                try {
                    FileUtils.forceDeleteOnExit(file);
                } catch (IOException e) {
                    throw new RuntimeException("Can't delete ddl file [" + file.getAbsolutePath() + "]", e);
                }
            }
        }
    }

    public void generateDllsForAllDbObjects() {
        generateDdls(ddlDao.extractAllDbObjectsWithoutDdl(), true);
    }

    private String sortConstraintInScript(String ddlScript) {
        Pattern pattern = Pattern.compile(CONSTRAINT_PATTERN, Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(ddlScript);
        StringBuilder sortedDdl = new StringBuilder();
        List<String> constraints = new ArrayList<>();
        while (matcher.find()) {
            constraints.add(matcher.group());
            matcher.appendReplacement(sortedDdl, "");
        }
        matcher.appendTail(sortedDdl);
        sortedDdl.setLength(sortedDdl.length() - 2);
        Collections.sort(constraints);
        constraints.forEach(sortedDdl::append);
        sortedDdl.append(");");
        return sortedDdl.toString();
    }
}