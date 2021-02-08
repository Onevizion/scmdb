package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.revwalk.RevCommit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

@Component
public class PackageGenerator {
    private static final String PACKAGE_SPECIFICATION_SUFFIX = "_spec";
    private static final String ROLLBACK_SUFFIX = "_rollback";
    private static final String PACKAGES_DDL_DIRECTORY_NAME = "packages";

    @Autowired
    private DdlDao ddlDao;

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private ColorLogger logger;


    public void packageScriptGenerate() {
        File repDirectory = getRepositoryDirectory();
        File packageDirectory = appArguments.getPackageDirectory();
        try(Git git = Git.open(repDirectory)) {
            List<DiffEntry> diffFiles = git.diff().call();
            String namePackage;
            String nameScript;
            String nameRollback;
            Path pathPKGSpec;
            Path pathPKG;
            String scriptBeforeRollback;
            for(DiffEntry d : diffFiles) {
                if (d.getChangeType() == DiffEntry.ChangeType.MODIFY || d.getChangeType() == DiffEntry.ChangeType.ADD) {
                    if (d.getNewPath().contains(PACKAGES_DDL_DIRECTORY_NAME)) {
                        logger.info("Create script for package");
                        namePackage = getNamePackage(d.getNewPath());
                        nameScript = getNameScript(getIssueName(git), namePackage);
                        nameRollback = nameScript + ROLLBACK_SUFFIX;

                        pathPKGSpec = Path.of(packageDirectory + File.separator + namePackage + PACKAGE_SPECIFICATION_SUFFIX + ".sql");
                        pathPKG = Path.of(packageDirectory + File.separator + namePackage + ".sql");

                        createCommitScript(nameScript, pathPKGSpec, pathPKG);

                        logger.info("Created new script " + nameScript);

                        File oldFile = new File(packageDirectory + File.separator + namePackage + ".sql");
                        scriptBeforeRollback = FileUtils.readFileToString(oldFile, "UTF-8");

                        git.checkout().addPath(d.getNewPath()).call();

                        createRollbackScript(nameRollback, pathPKGSpec, pathPKG);
                        FileUtils.write(oldFile, scriptBeforeRollback, "UTF-8");

                        logger.info("Created rollback for new script " + nameRollback);
                        git.add().addFilepattern("db/scripts/" + nameScript+".sql").call();
                        git.add().addFilepattern("db/scripts/" + nameRollback+".sql").call();
                    }
                }
            }
        } catch (IOException | GitAPIException e) {
            e.printStackTrace();
        }
    }

    private void createCommitScript(String nameScript, Path pathPKGSpec, Path pathPKG) throws IOException {
        Path newScriptPKG;
        newScriptPKG = Path.of(appArguments.getScriptsDirectory() + File.separator + nameScript + ".sql");

        Files.write(newScriptPKG, Files.readAllBytes(pathPKGSpec), StandardOpenOption.CREATE_NEW);
        Files.write(newScriptPKG, "\n\n".getBytes(), StandardOpenOption.APPEND);
        Files.write(newScriptPKG, Files.readAllBytes(pathPKG), StandardOpenOption.APPEND);
    }

    private void createRollbackScript(String nameRollback, Path pathPKGSpec, Path pathPKG) throws IOException {
        Path newRollbackPKG;
        newRollbackPKG = Path.of(appArguments.getScriptsDirectory() + File.separator + nameRollback + ".sql");

        Files.write(newRollbackPKG, Files.readAllBytes(pathPKGSpec), StandardOpenOption.CREATE_NEW);
        Files.write(newRollbackPKG, "\n\n".getBytes(), StandardOpenOption.APPEND);
        Files.write(newRollbackPKG, Files.readAllBytes(pathPKG), StandardOpenOption.APPEND);
    }

    private String getNameScript(String issueName, String packageName) {
        List<File> scripts = (List<File>) FileUtils.listFiles(appArguments.getScriptsDirectory(), new String[]{"sql"}, false);

        String lastNum = scripts.get(scripts.size() - 1).getName().split("_")[0];
        int numScript = Integer.parseInt(lastNum) + 1;

        return String.format("%d_%s_pkg_%s", numScript, issueName, packageName);
    }

    private File getRepositoryDirectory() {
        return new File(Path.of(appArguments.getScriptsDirectory().getParent()).toFile().getParent());
    }

    private String getNamePackage(String path) {
        String result = path.split("/")[3];
        return result.substring(0, result.length() - 4);
    }

    private String getIssueName(Git git) throws IOException {
        return git.getRepository().getBranch().split("_")[0];
    }
}