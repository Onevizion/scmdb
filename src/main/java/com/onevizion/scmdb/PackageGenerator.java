package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.*;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class PackageGenerator {
    private static final String PACKAGE_SPECIFICATION_SUFFIX = "_spec";
    private static final String ROLLBACK_SUFFIX = "_rollback";
    private static final String PACKAGES_DDL_DIRECTORY_NAME = "packages";

    private Git git;
    private Repository repository;

    @Autowired
    private DdlDao ddlDao;

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private ColorLogger logger;

    public void generaScript() {
        try {
            git = Git.open(getRepositoryDirectory());
            repository = git.getRepository();
            List<String> changedFilesList = getChangedFiles();
            if (CollectionUtils.isEmpty(changedFilesList)) {
                logger.info("Not found changes in packages");
                return;
            }

            logger.info("Create script for packages: " + changedFilesList, ColorLogger.Color.YELLOW);
            boolean wasSpec = false;
            String packageName = "";
            String scriptCommitName = null;
            String scriptRollbackName = null;
            for (String fileName : changedFilesList) {
                if (fileName.contains("_spec.sql")) {
                    packageName = getPackageName(fileName, true);
                    scriptCommitName = createCommitScript(packageName);
                    scriptRollbackName = createRollbackScript(packageName);
                    wasSpec = true;
                } else if (!wasSpec || !fileName.contains(packageName)) {
                    packageName = getPackageName(fileName, false);
                    scriptCommitName = createCommitScript(packageName);
                    scriptRollbackName = createRollbackScript(packageName);
                    wasSpec = false;
                } else {
                    wasSpec = false;
                    continue;
                }
                git.add().addFilepattern("db/scripts/" + scriptCommitName).call();
                git.add().addFilepattern("db/scripts/" + scriptRollbackName).call();
            }
            git.close();
            logger.info("Create packages done", ColorLogger.Color.GREEN);
        } catch (IOException | GitAPIException e) {
            e.printStackTrace();
        }
    }

    private List<String> getChangedFiles() {
        try {
            List<DiffEntry> diffFiles = git.diff().call();
            return diffFiles.stream()
                    .filter(d -> d.getChangeType() == DiffEntry.ChangeType.MODIFY || d.getChangeType() == DiffEntry.ChangeType.ADD)
                    .map(DiffEntry::getNewPath)
                    .filter(newPath -> newPath.contains(PACKAGES_DDL_DIRECTORY_NAME))
                    .map(newPath -> newPath.split("/")[3])
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
        } catch (GitAPIException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String createCommitScript(String packageName) throws IOException {
        String scriptName = generateScriptName(getIssueName(), packageName);
        Path script = Path.of(String.format("%s%s%s.sql", appArguments.getScriptsDirectory(), File.separator, scriptName));
        Path pathPKGSpec = Path.of(appArguments.getPackageDirectory() + File.separator + packageName
                + PACKAGE_SPECIFICATION_SUFFIX + ".sql");
        Path pathPKG = Path.of(appArguments.getPackageDirectory() + File.separator + packageName + ".sql");

        Files.write(script, Files.readAllBytes(pathPKGSpec), StandardOpenOption.CREATE_NEW);
        Files.write(script, "\n\n".getBytes(), StandardOpenOption.APPEND);
        Files.write(script, Files.readAllBytes(pathPKG), StandardOpenOption.APPEND);

        logger.info("Created new script " + scriptName);
        return scriptName + ".sql";
    }

    private String createRollbackScript(String packageName) throws IOException, GitAPIException {
        String scriptName = generateScriptName(getIssueName(), packageName) + ROLLBACK_SUFFIX;
        Path script = Path.of(String.format("%s%s%s.sql", appArguments.getScriptsDirectory(), File.separator, scriptName));
        Path pathPKGSpec = Path.of(appArguments.getPackageDirectory() + File.separator + packageName
                + PACKAGE_SPECIFICATION_SUFFIX + ".sql");
        Path pathPKG = Path.of(appArguments.getPackageDirectory() + File.separator + packageName + ".sql");

        //save new text from ddl
        String newDdlText = FileUtils.readFileToString(pathPKGSpec.toFile(), "UTF-8");
        //rollback changes ddl
        git.checkout().addPath("db/ddl/packages/" + packageName + PACKAGE_SPECIFICATION_SUFFIX + ".sql").call();
        //write old text ddl to script_rollback
        Files.write(script, Files.readAllBytes(pathPKGSpec), StandardOpenOption.CREATE_NEW);
        Files.write(script, "\n\n".getBytes(), StandardOpenOption.APPEND);
        //comeback new text in ddl
        FileUtils.write(pathPKGSpec.toFile(), newDdlText, "UTF-8");

        newDdlText = FileUtils.readFileToString(pathPKG.toFile(), "UTF-8");
        git.checkout().addPath("db/ddl/packages/" + packageName + ".sql").call();
        Files.write(script, Files.readAllBytes(pathPKG), StandardOpenOption.APPEND);
        FileUtils.write(pathPKG.toFile(), newDdlText, "UTF-8");

        logger.info("Created rollback for new script " + scriptName);
        return scriptName + ".sql";
    }

    private String generateScriptName(String issueName, String packageName) {
        List<File> scripts = (List<File>) FileUtils.listFiles(appArguments.getScriptsDirectory(), new String[]{"sql"}, false);

        String lastNum = scripts.get(scripts.size() - 1).getName().split("_")[0];
        int numScript = Integer.parseInt(lastNum) + 1;

        return String.format("%d_%s_%s", numScript, issueName, packageName);
    }

    private File getRepositoryDirectory() {
        return new File(Path.of(appArguments.getScriptsDirectory().getParent()).toFile().getParent());
    }

    private String getPackageName(String fileName, boolean spec) {
        return spec ? fileName.substring(0, fileName.length() - 9) : fileName.substring(0, fileName.length() - 4);
    }


    private String getIssueName() throws IOException {
        return git.getRepository().getBranch().split("_")[0];
    }
}