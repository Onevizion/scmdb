package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.*;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.ColorLogger.Color.*;

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

    public void generatePackages() {
        try {
            git = Git.open(getRepositoryDirectory());
            repository = git.getRepository();

            List<String> changedFilesList = getChangedPackages();
            if (CollectionUtils.isEmpty(changedFilesList)) {
                logger.info("Not found changes in packages for new scripts", YELLOW);
            } else {
                generateScripts(changedFilesList);
                return;
            }

            RevCommit mergeCommit = getMergeCommit();
            if (mergeCommit == null) {
                logger.info("Not found Merge commit `master` into " + repository.getBranch(), YELLOW);
            } else {
                updateScript(mergeCommit);
            }

        } catch (GitAPIException | IOException e) {
            e.printStackTrace();
        } finally {
            git.close();
        }
    }

    public void generateScripts(List<String> changedFilesList) {
        try {
            logger.info("Create script for packages: " + changedFilesList);
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
            logger.info("Create packages done", GREEN);
        } catch (IOException | GitAPIException e) {
            e.printStackTrace();
        }
    }

    public void updateScript(RevCommit mergeCommit) {
        try {
            List<String> scriptFiles = getScriptNames();

            if (CollectionUtils.isEmpty(scriptFiles)) {
                logger.info("Not found scripts for packages", YELLOW);
                return;
            }
            logger.info("Updating scripts: " + scriptFiles);

            List<String> changedPackages = getChangedPackages(mergeCommit);
            String packageName;
            for(String pack : changedPackages) {
                for(String script : scriptFiles) {
                    packageName = getPackageNameFromScriptName(script);
                    if (pack.contains(packageName)) {
                        if (script.endsWith(ROLLBACK_SUFFIX + ".sql")) {
                            updateRollbackScript(packageName, script);
                        } else {
                            updateCommitScript(packageName, script);
                        }
                        git.add().addFilepattern("db/scripts/" + script).call();
                    }
                }
            }
        } catch (GitAPIException e) {
            logger.info(e.getMessage(), RED);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //TODO: add refresh cache, after reset cache mey broke, index = 0, but diff > 0
    private List<String> getChangedPackages() throws GitAPIException {
        List<DiffEntry> diff = git.diff().call();
        return diff.stream()
                .filter(d -> d.getChangeType() == DiffEntry.ChangeType.MODIFY || d.getChangeType() == DiffEntry.ChangeType.ADD)
                .map(DiffEntry::getNewPath)
                .filter(newPath -> newPath.contains(PACKAGES_DDL_DIRECTORY_NAME))
                .map(newPath -> newPath.split("/")[3])
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());

    }

    private List<String> getChangedPackages(RevCommit mergeCommit) throws IOException, GitAPIException {
        RevCommit beforeMergeCommit = getBeforeMergeCommit(mergeCommit);

        AbstractTreeIterator newTreeIterator = getCanonicalTreeParser(mergeCommit);
        AbstractTreeIterator oldTreeIterator = getCanonicalTreeParser(beforeMergeCommit);

        List<DiffEntry> diff = git.diff().setNewTree(newTreeIterator).setOldTree(oldTreeIterator).call();

        return diff.stream()
                .filter(d -> d.getChangeType() == DiffEntry.ChangeType.MODIFY)
                .map(DiffEntry::getNewPath)
                .filter(newPath -> newPath.contains(PACKAGES_DDL_DIRECTORY_NAME))
                .map(newPath -> newPath.split("/")[3])
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }

    private List<String> getScriptNames() throws IOException {
        String issue = getIssueName();
        List<File> files = (List<File>) FileUtils.listFiles(appArguments.getScriptsDirectory(), new String[]{"sql"}, false);
        return files.stream()
                .map(File::getName)
                .filter(name -> name.contains(issue))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
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

        logger.info("Script file [{}] was created", GREEN, scriptName);
        return scriptName + ".sql";
    }

    private void updateCommitScript(String packageName, String scriptName) throws IOException {
        Path script = Path.of(appArguments.getScriptsDirectory() + File.separator + scriptName);
        Path pathPKGSpec = Path.of(appArguments.getPackageDirectory() + File.separator + packageName
                + PACKAGE_SPECIFICATION_SUFFIX + ".sql");
        Path pathPKG = Path.of(appArguments.getPackageDirectory() + File.separator + packageName + ".sql");

        Files.write(script, Files.readAllBytes(pathPKGSpec), StandardOpenOption.TRUNCATE_EXISTING);
        Files.write(script, "\n\n".getBytes(), StandardOpenOption.APPEND);
        Files.write(script, Files.readAllBytes(pathPKG), StandardOpenOption.APPEND);

        logger.info("Script file [{}] was changed", CYAN, scriptName);
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

        logger.info("Script file [{}] was created", GREEN, scriptName);
        return scriptName + ".sql";
    }

    private void updateRollbackScript(String packageName, String scriptName) throws GitAPIException, IOException {
        Path script = Path.of(appArguments.getScriptsDirectory() + File.separator + scriptName);
        Path pathPKGSpec = Path.of(appArguments.getPackageDirectory() + File.separator + packageName
                + PACKAGE_SPECIFICATION_SUFFIX + ".sql");
        Path pathPKG = Path.of(appArguments.getPackageDirectory() + File.separator + packageName + ".sql");

        String scriptSpecText;
        String scriptBodyText;
        String currentBranch = repository.getBranch();

        git.checkout().setName("master").call();

        scriptSpecText = FileUtils.readFileToString(pathPKGSpec.toFile(), "UTF-8");
        scriptBodyText = FileUtils.readFileToString(pathPKG.toFile(), "UTF-8");

        git.checkout().setName(currentBranch).call();

        Files.write(script, scriptSpecText.getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);
        Files.write(script, "\n\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        Files.write(script, scriptBodyText.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);

        logger.info("Script file [{}] was changed", CYAN, scriptName);
    }

    //TODO: change logic and num of script file
    private String generateScriptName(String issueName, String packageName) {
//        List<File> scripts = (List<File>) FileUtils.listFiles(appArguments.getScriptsDirectory(), new String[]{"sql"}, false);
//
//        String lastNum = scripts.get(scripts.size() - 1).getName().split("_")[0];
//        int numScript = Integer.parseInt(lastNum) + 1;

//        return String.format("%d_%s_%s", numScript, issueName, packageName);
        return String.format("%s_%s_%s", "XXXX", issueName, packageName);
    }

    private File getRepositoryDirectory() {
        return new File(Path.of(appArguments.getScriptsDirectory().getParent()).toFile().getParent());
    }

    private String getPackageName(String fileName, boolean spec) {
        return spec ? fileName.substring(0, fileName.length() - 9) : fileName.substring(0, fileName.length() - 4);
    }

    private String getPackageNameFromScriptName(String fileName) throws IOException {
        String startName = "XXXX_" + getIssueName() + "_";
        return fileName.endsWith(ROLLBACK_SUFFIX + ".sql") ?
                fileName.substring(startName.length(), fileName.length() - (ROLLBACK_SUFFIX.length() + 4)) :
                fileName.substring(startName.length(), fileName.length() - 4);
    }

    private String getIssueName() throws IOException {
        return git.getRepository().getBranch().split("_")[0];
    }

    private RevCommit getMergeCommit() {
        try {
            String mergeMessage = "Merge branch 'master' into " + repository.getBranch();
            Iterable<RevCommit> commits = git.log().setMaxCount(3).call();
            for (RevCommit commit : commits) {
                if (commit.getShortMessage().equals(mergeMessage)) {
                    return commit;
                }
            }
        } catch (IOException | GitAPIException e) {
            e.printStackTrace();
        }
        return null;
    }

    public RevCommit getBeforeMergeCommit(RevCommit mergeCommit) {
        try {
            Iterable<RevCommit> commits = git.log().setMaxCount(3).call();
            boolean beforeMerge = false;
            for (RevCommit commit : commits) {
                if (beforeMerge) {
                    return  commit;
                }
                if (commit.getName().equals(mergeCommit.getName())) {
                    beforeMerge = true;
                }
            }
        } catch (GitAPIException e) {
            e.printStackTrace();
        }
        return null;
    }

    private AbstractTreeIterator getCanonicalTreeParser(ObjectId commitId) throws IOException {
        try (RevWalk walk = new RevWalk(git.getRepository())) {
            RevCommit commit = walk.parseCommit(commitId);
            ObjectId treeId = commit.getTree().getId();
            try (ObjectReader reader = git.getRepository().newObjectReader()) {
                return new CanonicalTreeParser(null, reader, treeId);
            }
        }
    }
}