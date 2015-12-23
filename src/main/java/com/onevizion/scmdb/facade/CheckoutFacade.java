package com.onevizion.scmdb.facade;

import com.onevizion.scmdb.dao.DbScriptDaoOra;
import com.onevizion.scmdb.vo.DbScriptStatus;
import com.onevizion.scmdb.vo.DbScriptType;
import com.onevizion.scmdb.vo.DbScriptVo;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;

@Component
public class CheckoutFacade {
    @Resource
    private DbScriptDaoOra dbScriptDaoOra;

    @Resource
    private DdlFacade ddlFacade;

    private final String ROLLBACK_SUFX = "_rollback";

    private final String EXEC_FOLDER_NAME = "EXECUTE_ME";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public void createAllFromPath(File scriptDir) {
        Collection<File> files = FileUtils.listFiles(scriptDir, new String[]{"sql"}, false);
        List<DbScriptVo> dbScripts = createAll(files);
        DbScriptVo[] dbScriptArr = dbScripts.toArray(new DbScriptVo[dbScripts.size()]);
        dbScriptDaoOra.batchCreate(dbScriptArr);
    }

    @Transactional
    public List<File> checkoutDbFromPath(File scriptDir, Map<String, DbScriptVo> dbScripts, boolean isGenDdl) {
        DbScriptVo dbScriptVo = dbScriptDaoOra.readNewest();
        logger.debug("Searching new scripts in [{}]", scriptDir.getAbsolutePath());
        Collection<File> files = FileUtils.listFiles(scriptDir, new AgeFileFilter(dbScriptVo.getTs(), false), null);
        List<DbScriptVo> newDbScripts = createAll(files);
        List<DbScriptVo> delDbScripts = new ArrayList<DbScriptVo>();
        delDbScripts.addAll(newDbScripts);
        Iterator<DbScriptVo> iter = newDbScripts.iterator();
        while (iter.hasNext()) {
            DbScriptVo vo1 = iter.next();
            if (dbScripts.containsKey(vo1.getName())) {
                DbScriptVo vo2 = dbScripts.get(vo1.getName());
                if (!vo1.equals(vo2)) {
                    logger.warn("Script file was changed [{}]", vo1.getName());
                    vo1.setDbScriptId(vo2.getDbScriptId());
                    dbScriptDaoOra.update(vo1);
                }
                iter.remove();
            }
        }

        logger.debug("Searching deleted scripts in [{}]", scriptDir.getAbsolutePath());
        files = FileUtils.listFiles(scriptDir, new AgeFileFilter(dbScriptVo.getTs(), true), null);
        delDbScripts.addAll(createAll(files));

        File file = new File(scriptDir.getAbsolutePath() + File.separator + dbScriptVo.getName());
        if (file.exists() && !delDbScripts.contains(dbScriptVo)) {
            delDbScripts.add(dbScriptVo);
        }

        List<DbScriptVo> rollbackDbScripts = new ArrayList<DbScriptVo>();
        List<Long> delIds = new ArrayList<Long>();
        iter = dbScripts.values().iterator();
        while (iter.hasNext()) {
            DbScriptVo vo = iter.next();
            if (!delDbScripts.contains(vo)) {
                if (DbScriptType.ROLLBACK.equals(vo.getType())) {
                    rollbackDbScripts.add(vo);
                    delIds.add(vo.getDbScriptId());
                } else {
                    String rollbackScript = vo.getName() + ROLLBACK_SUFX;
                    if (dbScripts.containsKey(rollbackScript)) {
                        delIds.add(vo.getDbScriptId());
                    }
                }
            }
        }

        File execDir = new File(scriptDir.getAbsolutePath() + File.separator + EXEC_FOLDER_NAME);
        if (execDir.exists()) {
            try {
                FileUtils.deleteDirectory(execDir);
            } catch (IOException e) {
                logger.warn("Can't delete directory by path {[]}", execDir.getAbsolutePath());
            }
        }
        
        List<File> result = new ArrayList<File>();
        if (!isGenDdl) {
            for (DbScriptVo vo : rollbackDbScripts) {
                File f = new File(execDir.getAbsolutePath() + File.separator + vo.getName());
                try {
                    logger.debug("Creating rollback script [{}]", f.getAbsolutePath());
                    FileUtils.writeStringToFile(f, vo.getText());
                    result.add(f);
                } catch (IOException e) {
                    logger.error("Can't create file [{}]", f.getAbsolutePath(), e);
                    throw new RuntimeException(e);
                }
            }

            for (DbScriptVo vo : newDbScripts) {
                if (DbScriptType.ROLLBACK.equals(vo.getType())) {
                    continue;
                }
                File srcFile = new File(scriptDir.getAbsolutePath() + File.separator + vo.getName());
                File destFile = new File(execDir.getAbsolutePath() + File.separator + vo.getName());
                try {
                    logger.debug("Copying new script [{}]", destFile.getAbsolutePath());
                    FileUtils.copyFile(srcFile, destFile);
                    result.add(destFile);
                } catch (IOException e) {
                    logger.error("Can't copy file [{}]", srcFile.getAbsolutePath(), e);
                    throw new RuntimeException(e);
                }
            }
        }

        if (!delIds.isEmpty()) {
            logger.debug("Deleting missed scripts form db");
            dbScriptDaoOra.deleteByIds(delIds);
        }

        if (!newDbScripts.isEmpty()) {
            logger.debug("Saving new scripts in db");
            DbScriptVo[] newDbScriptArr = newDbScripts.toArray(new DbScriptVo[newDbScripts.size()]);
            dbScriptDaoOra.batchCreate(newDbScriptArr);

            if (isGenDdl) {
                logger.info("Extract DDL for new scripts");
                ddlFacade.generateDdl(newDbScripts, scriptDir);
            }
        }

        return result;
    }

    private List<DbScriptVo> createAll(Collection<File> files) {
        Iterator<File> iter = files.iterator();
        List<DbScriptVo> dbScripts = new ArrayList<DbScriptVo>();
        while (iter.hasNext()) {
            File file = iter.next();
            DbScriptVo dbScriptVo = new DbScriptVo();
            dbScriptVo.setName(file.getName());
            try {
                String fileStr = FileUtils.readFileToString(file);
                fileStr = fileStr.replaceAll("\\r\\n", "\n");
                String sha1Hex = DigestUtils.sha1Hex(fileStr);
                dbScriptVo.setFileHash(sha1Hex);
            } catch (IOException e) {
                logger.warn("Can't generate hash for [{}]", file.getName(), e);
            }
            dbScriptVo.setTs(new Date(file.lastModified()));

            String fileName = FilenameUtils.getBaseName(file.getName());
            if (fileName.endsWith(ROLLBACK_SUFX)) {
                try {
                    String content = FileUtils.readFileToString(file);
                    dbScriptVo.setText(content);
                } catch (IOException e) {
                    logger.warn("Can't read file [{}]", file.getName(), e);
                }
                dbScriptVo.setType(DbScriptType.ROLLBACK);
            } else {
                dbScriptVo.setType(DbScriptType.COMMIT);
            }
            dbScriptVo.setStatus(DbScriptStatus.EXECUTED);
            dbScripts.add(dbScriptVo);
        }
        return dbScripts;
    }
}
