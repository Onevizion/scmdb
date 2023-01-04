package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.ScriptExecException;
import com.onevizion.scmdb.facade.DbScriptFacade;
import com.onevizion.scmdb.vo.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.ColorLogger.Color.CYAN;
import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.Scmdb.EXIT_CODE_SUCCESS;
import static com.onevizion.scmdb.vo.SchemaType.OWNER;
import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static com.onevizion.scmdb.vo.ScriptType.ROLLBACK;

public class DbManager {
    private static final String SCRIPT_EXECUTION_ERROR_MESSAGE = "Fix and execute manually script [{0}] and then run SCMDB again to execute other scripts.";
    private static final String FIRST_RUN_MESSAGE = "It's your first run of SCMDB. SCMDB was initialized.";
    private static final String NO_SCRIPTS_TO_EXEC_MSG = "No scripts to execute in [{}]:";
    private static final String SCRIPTS_TO_EXEC_MSG = "\nScripts to be executed in [{}]:";
    private static final String ROLLBACKS_TO_SKIP_MSG = "\nRollbacks skipped in [{}]:";
    private static final String SCRIPT_NUMBERING_IS_MORE_THAN_TWO_DIGITS_REGEX = "^\\d{3,}_.*";

    @Autowired
    private DbScriptFacade scriptsFacade;

    @Autowired
    private DdlGenerator ddlGenerator;

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private SqlScriptExecutor scriptExecutor;

    @Autowired
    private ColorLogger logger;

    public void updateDb() {
        logger.info("SCMDB {}", getClass().getPackage().getImplementationVersion());

        scriptsFacade.checkDbConnection();

        if (!scriptsFacade.isScriptTableExist()) {
            scriptExecutor.createDbScriptTable();
            scriptsFacade.createAllFromDirectory();
            logger.info(FIRST_RUN_MESSAGE);
        } else if (scriptsFacade.isFirstRun()) {
            scriptsFacade.createAllFromDirectory();
            logger.info(FIRST_RUN_MESSAGE);
        } else {
            scriptsFacade.cleanExecDir();
            checkUpdatedScripts();
            checkDeletedScripts();
            executeNewScripts();
        }
    }

    private void executeNewScripts() {
        List<SqlScript> newScripts = scriptsFacade.getNewScripts();
        if (newScripts.isEmpty()) {
            logger.info(NO_SCRIPTS_TO_EXEC_MSG, appArguments.getDbCredentials(OWNER).getSchemaWithUrlBeforeDot());
            return;
        }

        List<SqlScript> newCommitScripts = sortScriptsInExecutionOrder(newScripts, COMMIT);

        List<SqlScript> newRollbackScripts = sortScriptsInExecutionOrder(newScripts, ROLLBACK);
        scriptsFacade.batchCreate(newRollbackScripts);

        if (appArguments.isExecuteScripts()) {
            logger.info(SCRIPTS_TO_EXEC_MSG, appArguments.getDbCredentials(OWNER).getSchemaWithUrlBeforeDot());
            newCommitScripts.forEach(script -> logger.info(script.getName()));
            newCommitScripts.forEach(script -> {
                int exitCode = scriptExecutor.execute(script);
                script.setStatus(ScriptStatus.getByScriptExitCode(exitCode));
                scriptsFacade.create(script);

                if (script.getStatus() != ScriptStatus.EXECUTED && !appArguments.isIgnoreErrors()) {
                    throw new ScriptExecException(MessageFormat.format(SCRIPT_EXECUTION_ERROR_MESSAGE, script.getName()));
                }
            });
        } else {
            logger.info("You should execute following script files to update your database:");
            scriptsFacade.copyScriptsToExecDir(newCommitScripts);
            newCommitScripts.forEach(script -> logger.info(script.getFile().getAbsolutePath()));
            scriptsFacade.batchCreate(newCommitScripts);
        }
    }

    private void checkDeletedScripts() {
        Map<String, SqlScript> deletedScripts = scriptsFacade.getDeletedScriptsMap();
        List<SqlScript> rollbacksToExec = deletedScripts.values()
                                                        .stream()
                                                        .filter(script -> deletedScripts.containsKey(script.getCommitName()))
                                                        .collect(Collectors.toList());
        rollbacksToExec = sortScriptsInExecutionOrder(rollbacksToExec, ROLLBACK);
        if (rollbacksToExec.isEmpty()) {
            scriptsFacade.deleteAll(deletedScripts.values());
            return;
        }

        if (appArguments.isOmitChanged()) {
            logger.info(ROLLBACKS_TO_SKIP_MSG, appArguments.getDbCredentials(OWNER).getSchemaWithUrlBeforeDot());
            rollbacksToExec.forEach(s -> logger.info(s.getName()));
            logger.info("\n");

            scriptsFacade.deleteAll(deletedScripts.values());
            return;
        }

        boolean executeRollbacks = false;
        if (appArguments.isExecuteScripts()) {
            logger.info("Do you really want to execute {} rollbacks? \n", GREEN, rollbacksToExec.size());
            rollbacksToExec.forEach(r -> logger.info(r.getName(), GREEN));
            logger.info("\nType [no] and rollbacks will be copied to EXECUTE_ME directory and marked as executed. " +
                    "Execute them manually and run scmdb again to execute new scripts.", GREEN);
            logger.info("Type [yes] to continue and execute all rollbacks", GREEN);
            executeRollbacks = userGrantsPermission();
        }

        List <SqlScript> rollbacksToExecWithText = scriptsFacade.getDbScriptsWithText(rollbacksToExec);
        if (executeRollbacks) {
            executeRollbacks(deletedScripts, rollbacksToExecWithText);
        } else {
            logger.info("At first you should execute following rollbacks to revert changes of deleted scripts:");
            scriptsFacade.copyRollbacksToExecDir(rollbacksToExecWithText);
            rollbacksToExecWithText.forEach(script -> logger.info(script.getName(), GREEN));
            scriptsFacade.deleteAll(deletedScripts.values());

            System.exit(EXIT_CODE_SUCCESS);
        }
    }

    private void executeRollbacks(Map<String, SqlScript> deletedScripts, List<SqlScript> rollbacksToExec) {
        for (SqlScript rollback : rollbacksToExec) {
            if (deletedScripts.containsKey(rollback.getCommitName())) {
                scriptsFacade.copyRollbackToExecDir(rollback);

                SqlScript commit = deletedScripts.get(rollback.getCommitName());
                scriptsFacade.delete(rollback.getId());
                scriptsFacade.delete(commit.getId());

                int exitCode = scriptExecutor.execute(rollback);
                if (exitCode != 0) {
                    throw new ScriptExecException(MessageFormat.format(SCRIPT_EXECUTION_ERROR_MESSAGE, rollback.getName()));
                }

                deletedScripts.keySet().remove(rollback.getName());
                deletedScripts.keySet().remove(rollback.getCommitName());
            }
        }
    }

    private void checkUpdatedScripts() {
        if (appArguments.isOmitChanged()) {
            return;
        }
        List<SqlScript> updatedScripts = scriptsFacade.getUpdatedScripts();
        scriptsFacade.batchUpdate(updatedScripts);
        updatedScripts.forEach(script -> logger.info("Script file [{}] was changed", CYAN, script.getName()));
    }

    private boolean userGrantsPermission() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            String str = br.readLine();
            if ("yes".equalsIgnoreCase(str)) {
                return true;
            } else if ("no".equalsIgnoreCase(str)) {
                return false;
            } else {
                logger.info("Type yes or no");
                return userGrantsPermission();
            }

        } catch (IOException e) {
            throw new RuntimeException("Can't read user console input.", e);
        }
    }

    public void generateDdlForNewOrChangedScripts() {
        logger.info("Extracting DDL for new and updated scripts");

        scriptsFacade.checkDbConnection();

        List<SqlScript> scripts = scriptsFacade.getNewScripts();
        scripts.addAll(scriptsFacade.getUpdatedScripts());
        List<SqlScript> scriptsToGenDdl = scripts.stream()
                                                 .sorted()
                                                 .filter(script -> script.getType() == ScriptType.COMMIT)
                                                 .filter(script -> script.getSchemaType() == OWNER)
                                                 .collect(Collectors.toList());

        Set<DbObject> changedDbObjects = findChangedDbObjects(scriptsToGenDdl);
        ddlGenerator.executeSettingTransformParams();
        ddlGenerator.generateDdls(changedDbObjects, false);
    }

    private Set<DbObject> findChangedDbObjects(List<SqlScript> scripts) {
        Set<DbObject> updatedDbObjects;

        List <SqlScript> dbScriptsWithText = scriptsFacade.getDbScriptsWithText(scripts);

        updatedDbObjects = dbScriptsWithText.stream()
                                  .map(script -> removeSpecialFromScriptText(script.getText()))
                                  .flatMap(scriptText -> findChangedDbObjectsInScriptText(scriptText).stream())
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

    public void generateDdlForAllObjects() {
        logger.info("Extracting DDL for all db objects");

        scriptsFacade.checkDbConnection();

        ddlGenerator.executeSettingTransformParams();
        ddlGenerator.generateDllsForAllDbObjects();
    }

    /**
     * The method filters scripts by type and sorts them in the correct execution order.
     * Scripts will be sorted in ascending order of numbering, scripts in the range 1..99 will be added to the end of the list.
     * If the type is ROLLBACK, then the scripts will be sorted in descending order of numbering,
     * scripts in the range 99..1 will be added to the end of the list.
     *
     * Example 1 : Input  scripts = [1_script1.sql, 1_script1_rollback.sql, 8861_script2.sql, 8861_script2_rollback.sql]
     *                    scriptType = COMMIT
     *             Output List<SqlScript> = [8861_script2.sql, 1_script1.sql]
     *
     * Example 2 : Input  scripts = [1_script1.sql, 1_script1_rollback.sql, 8861_script2.sql, 8861_script2_rollback.sql]
     *                    scriptType = ROLLBACK
     *             Output List<SqlScript> = [1_script1_rollback.sql, 8861_script2_rollback.sql]
     *
     * @param scripts list of scripts to sort
     * @param scriptType script type
     * @return list of scripts with the correct execution order
     */
    private List<SqlScript> sortScriptsInExecutionOrder(List<SqlScript> scripts, ScriptType scriptType) {
        scripts = scripts.stream()
                         .filter(script -> script.getType() == scriptType)
                         .collect(Collectors.toList());

        List<SqlScript> commonScripts = new ArrayList<>();
        List<SqlScript> scriptsLessThanHundred = new ArrayList<>();
        for (SqlScript script : scripts) {
            if (!script.getName().matches(SCRIPT_NUMBERING_IS_MORE_THAN_TWO_DIGITS_REGEX)) {
                scriptsLessThanHundred.add(script);
            } else {
                commonScripts.add(script);
            }
        }

        if (scriptType == ROLLBACK) {
            commonScripts.sort(Comparator.reverseOrder());
            scriptsLessThanHundred.sort(Comparator.reverseOrder());
            scriptsLessThanHundred.addAll(commonScripts);
            return scriptsLessThanHundred;
        } else {
            commonScripts.sort(Comparator.naturalOrder());
            scriptsLessThanHundred.sort(Comparator.naturalOrder());
            commonScripts.addAll(scriptsLessThanHundred);
            return commonScripts;
        }
    }
}
