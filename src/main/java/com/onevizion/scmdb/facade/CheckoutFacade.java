package com.onevizion.scmdb.facade;

import com.onevizion.scmdb.dao.DbScriptDaoOra;
import com.onevizion.scmdb.vo.DbScriptType;
import com.onevizion.scmdb.vo.DbScriptVo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.*;

@Component
public class CheckoutFacade {
    @Resource
    private DbScriptDaoOra dbScriptDaoOra;

    @Resource
    private DdlFacade ddlFacade;

    private final String EXEC_FOLDER_NAME = "EXECUTE_ME";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public void createAllFromPath(File scriptDir) {
        Collection<File> files = FileUtils.listFiles(scriptDir, new String[]{"sql"}, false);
        dbScriptDaoOra.batchCreate(createVosFromScriptFiles(files));
    }

    @Transactional
    public Collection<DbScriptVo> getScriptsToExec(File scriptDir) {
        logger.debug("Searching new scripts in [{}]", scriptDir.getAbsolutePath());

        Map<String, DbScriptVo> dbScripts = dbScriptDaoOra.readAll();
        Collection<File> scriptFiles = FileUtils.listFiles(scriptDir, new RegexFileFilter(".+\\.sql"), null);
        List<DbScriptVo> scriptsInDir = createVosFromScriptFiles(scriptFiles);

        for (DbScriptVo script : scriptsInDir) {
            if (dbScripts.containsKey(script.getName())) {
                DbScriptVo savedScript = dbScripts.get(script.getName());
                if (!script.getFileHash().equals(savedScript.getFileHash())) {
                    logger.warn("Script file was changed [{}]", script.getName());
                    savedScript.setFileHash(script.getFileHash());
                    dbScriptDaoOra.update(savedScript);
                }
            }
        }

        logger.debug("Searching deleted scripts in [{}]", scriptDir.getAbsolutePath());
        Collection<DbScriptVo> deletedScripts = CollectionUtils.subtract(dbScripts.values(), scriptsInDir);
        List<DbScriptVo> rollbacksToExec = new ArrayList<DbScriptVo>();
        List<Long> deleteScriptIds = new ArrayList<Long>();
        for (DbScriptVo deletedScript : deletedScripts) {
            if (DbScriptType.ROLLBACK.getTypeId().equals(deletedScript.getType())) {
                deleteScriptIds.add(deletedScript.getDbScriptId());
            }else if (DbScriptType.COMMIT.getTypeId().equals(deletedScript.getType())
                    && dbScripts.containsKey(deletedScript.getRollbackName())) {
                rollbacksToExec.add(dbScripts.get(deletedScript.getRollbackName()));
                deleteScriptIds.add(deletedScript.getDbScriptId());
            }
        }
        if (!deleteScriptIds.isEmpty()) {
            logger.debug("Deleting missed scripts form db");
            dbScriptDaoOra.deleteByIds(deleteScriptIds);
        }

        Collection<DbScriptVo> newScripts = removeRollbacks(CollectionUtils.subtract(scriptsInDir, dbScripts.values()));
        return copyScriptsToExecDir(scriptDir, newScripts, rollbacksToExec);
    }

    private Collection<DbScriptVo> removeRollbacks(Collection<DbScriptVo> scripts) {
        Iterator<DbScriptVo> iterator = scripts.iterator();
        while (iterator.hasNext()) {
            DbScriptVo script = iterator.next();
            if (DbScriptType.ROLLBACK.getTypeId().equals(script.getType())) {
                iterator.remove();
            }
        }
        return scripts;
    }

    public Collection<DbScriptVo> copyScriptsToExecDir(File scriptDir, Collection<DbScriptVo> newScripts, Collection<DbScriptVo> rollbacks) {
        File execDir = createExecDir(scriptDir);

        for (DbScriptVo vo : rollbacks) {
            File f = new File(execDir.getAbsolutePath() + File.separator + vo.getName());
            try {
                logger.debug("Creating rollback script [{}]", f.getAbsolutePath());
                FileUtils.writeStringToFile(f, vo.getText());
                logger.info("Execute manually rollback script: [{}]", f.getAbsolutePath());
            } catch (IOException e) {
                logger.error("Can't create file [{}]", f.getAbsolutePath(), e);
                throw new RuntimeException(e);
            }
        }

        for (DbScriptVo script : newScripts) {
            if (DbScriptType.ROLLBACK.getTypeId().equals(script.getType())) {
                continue;
            }
            File srcFile = new File(scriptDir.getAbsolutePath() + File.separator + script.getName());
            File destFile = new File(execDir.getAbsolutePath() + File.separator + script.getName());
            try {
                logger.debug("Copying new script [{}]", destFile.getAbsolutePath());
                FileUtils.copyFile(srcFile, destFile);
                script.setFile(destFile);
            } catch (IOException e) {
                logger.error("Can't copy file [{}]", srcFile.getAbsolutePath(), e);
                throw new RuntimeException(e);
            }
        }

        return newScripts;
    }

    public void genDdl(File scriptDir, Collection<DbScriptVo> newScripts) {
        List<DbScriptVo> newCommitScripts = new ArrayList<DbScriptVo>();
        for (DbScriptVo vo : newScripts) {
            if (DbScriptType.COMMIT.getTypeId().equals(vo.getType())) {
                newCommitScripts.add(vo);
            }
        }
        ddlFacade.generateDdl(newCommitScripts, scriptDir);
    }

    public boolean isFirstRun() {
        return dbScriptDaoOra.readCount().equals(0L);
    }

    private File createExecDir(File scriptDir) {
        File execDir = new File(scriptDir.getAbsolutePath() + File.separator + EXEC_FOLDER_NAME);
        if (execDir.exists()) {
            try {
                FileUtils.deleteDirectory(execDir);
            } catch (IOException e) {
                logger.error("Can't delete directory by path {" + execDir.getAbsolutePath() + "}", e);
            }
        }
        return execDir;
    }

    private List<DbScriptVo> createVosFromScriptFiles(Collection<File> files) {
        List<DbScriptVo> dbScripts = new ArrayList<DbScriptVo>();
        for (File scriptFile : files) {
            dbScripts.add(DbScriptVo.create(scriptFile));
        }
        return dbScripts;
    }
}