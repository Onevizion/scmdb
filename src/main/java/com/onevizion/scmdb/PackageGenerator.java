package com.onevizion.scmdb;

import com.onevizion.scmdb.facade.DbScriptFacade;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.*;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.dircache.DirCacheIterator;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.FileTreeIterator;
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
    private DbScriptFacade scriptFacade;

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
                logger.info("There are no changes in the packages for generating the script.", YELLOW);
            } else {
                generateScripts(changedFilesList);
                return;
            }

            RevCommit mergeCommit = getMergeCommit();
            if (mergeCommit == null) {
                logger.info("Merge commit `master` into {} not found", YELLOW, repository.getBranch());
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
            logger.info("Generating scripts for packages: " + changedFilesList);
            boolean wasSpec = false;
            String packageName = "";
            String scriptCommitName;
            String scriptRollbackName;

            createStash();

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
            deleteStash();
            git.close();
        } catch (GitAPIException e) {
            logger.info(e.getMessage(), RED);
            applyStash();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateScript(RevCommit mergeCommit) {
        try {
            List<String> scriptFiles = getScriptNames(getIssueName());

            if (CollectionUtils.isEmpty(scriptFiles)) {
                logger.info("Scripts for packages not found", YELLOW);
                return;
            }
            logger.info("Find script file {}", scriptFiles);

            List<String> changedPackages = getChangedPackages(mergeCommit);

            if (CollectionUtils.isEmpty(changedPackages)) {
                changedPackages = getPackagesWithConflicts(mergeCommit);
            }
            if (CollectionUtils.isEmpty(changedPackages)) {
                logger.info("Conflicts nod found", YELLOW);
                return;
            }

            String packageName;
            for (String pack : changedPackages) {
                for (String script : scriptFiles) {
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

    public void generateBackportPackage() {
        try {
            git = Git.open(getRepositoryDirectory());
            repository = git.getRepository();

            //TODO: пройтись и найти коммиты одной issue
            List<RevCommit> commits = new ArrayList<>();
            RevCommit lastCommit = git.log().setMaxCount(1).call().iterator().next();
            RevCommit beforeCommit = null;
            RevWalk walk = new RevWalk(repository);
//            RevCommit lastCommit = walk.parseCommit(repository.resolve("5183c11d"));
            walk.markStart(walk.parseCommit(lastCommit));
            RevCommit commit;
            String issueId = lastCommit.getShortMessage().split(" ")[0];
            for (Iterator<RevCommit> it = walk.iterator(); it.hasNext(); ) {
                commit = it.next();
                if (commit.getShortMessage().startsWith(issueId)) {
                    commits.add(commit);
                } else {
                    beforeCommit = commit;
                    break;
                }
            }
            commits.stream().map(RevCommit::getShortMessage).forEach(System.out::println);
            walk.dispose();

            //TODO: сравнить коммиты isuee с последним комитом до cherry-pik --> getChangedPackages
            List<String> changedPackages = getChangedPackages(commits.get(0), beforeCommit);

            if (CollectionUtils.isEmpty(changedPackages)) {
                logger.info("not found packages", RED);
                return;
            }

            logger.info("Found changed packages: " + changedPackages);
            List<String> localScripts = getScriptNames(issueId);
            logger.info("Found scripts: " + localScripts);

            int index = 0;
            Path scriptPath;
            List<SqlScript> lastCommitScripts;

            for(String localScript : localScripts) {
                for(String packageName : changedPackages) {
                    if (packageName.contains("_spec")) {
                        packageName = getPackageName(packageName, true);
                    } else {
                        packageName = getPackageName(packageName, false);
                    }
                    if (localScript.endsWith(ROLLBACK_SUFFIX + ".sql") && scriptNameContainsPackage(localScript, packageName, true)) {
                        scriptPath = Path.of(appArguments.getScriptsDirectory() + File.separator + localScript);
                        lastCommitScripts = scriptFacade.findByPackageName(packageName);
                        while (lastCommitScripts.get(index).getName().equals(localScript)) {
                            index++;
                        }
                        Files.write(scriptPath, lastCommitScripts.get(index).getText().getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);
                        logger.info("Script file [{}] was changed based [{}]", CYAN, localScript, lastCommitScripts.get(index).getName());
                        index = 0;
                        break;
                    } else if (scriptNameContainsPackage(localScript, packageName, false)) {
                        updateCommitScript(packageName, localScript);
                        break;
                    }
                }
            }
        } catch (GitAPIException | IOException e) {
            e.printStackTrace();
        } finally {
            git.close();
        }
    }

    private boolean scriptNameContainsPackage(String script, String packageName, boolean isRollback) {
        int indexEnd = script.length();
        indexEnd = isRollback ? indexEnd - 13 : indexEnd - 4;

        String scr = script.substring(0, indexEnd);
        return  scr.endsWith(packageName);
    }

    private List<String> getChangedPackages() throws GitAPIException, IOException {
        AbstractTreeIterator oldTree = new FileTreeIterator(git.getRepository());
        AbstractTreeIterator newTree = new DirCacheIterator(git.getRepository().readDirCache());
        List<DiffEntry> diff = git.diff().setNewTree(newTree).setOldTree(oldTree).call();
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
        return getChangedPackages(mergeCommit, beforeMergeCommit);
    }

    private List<String> getChangedPackages(RevCommit lastCommit, RevCommit beforeCommit) throws IOException, GitAPIException {
        AbstractTreeIterator newTreeIterator = getCanonicalTreeParser(lastCommit);
        AbstractTreeIterator oldTreeIterator = getCanonicalTreeParser(beforeCommit);
        List<DiffEntry> diff = git.diff().setNewTree(newTreeIterator).setOldTree(oldTreeIterator).call();
        return diff.stream()
                .filter(d -> d.getChangeType() == DiffEntry.ChangeType.MODIFY)
                .map(DiffEntry::getNewPath)
                .filter(newPath -> newPath.contains(PACKAGES_DDL_DIRECTORY_NAME))
                .map(newPath -> newPath.split("/")[3])
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }

    private List<String> getPackagesWithConflicts(RevCommit mergeCommit) {
        String[] conflicts = mergeCommit.getFullMessage().split("#");
        List<String> pakagesFile = new ArrayList<>();
        String[] sp;
        for (String str : conflicts) {
            if (str.contains(PACKAGES_DDL_DIRECTORY_NAME)) {
                sp = str.split("/");
                pakagesFile.add(sp[sp.length - 1].strip());
            }
        }
        return pakagesFile;
    }

    private List<String> getScriptNames(String issueId) {
        List<File> files = (List<File>) FileUtils.listFiles(appArguments.getScriptsDirectory(), new String[]{"sql"}, false);
        return files.stream()
                .map(File::getName)
                .filter(name -> name.contains(issueId))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }

    private String createCommitScript(String packageName) throws IOException {
        String scriptName = generateScriptName(getIssueName(), packageName);
        return createCommitScript(packageName, scriptName);
    }

    private String createCommitScript(String packageName, String scriptName) throws IOException {
        Path script = Path.of(String.format("%s%s%s", appArguments.getScriptsDirectory(), File.separator, scriptName));
        Path pathPKGSpec = Path.of(appArguments.getPackageDirectory() + File.separator + packageName
                + PACKAGE_SPECIFICATION_SUFFIX + ".sql");
        Path pathPKG = Path.of(appArguments.getPackageDirectory() + File.separator + packageName + ".sql");
        StandardOpenOption openOption = StandardOpenOption.CREATE_NEW;
        if (Files.exists(script)) {
            openOption = StandardOpenOption.TRUNCATE_EXISTING;
        }

        Files.write(script, Files.readAllBytes(pathPKGSpec), openOption);
        Files.write(script, "\n\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        Files.write(script, Files.readAllBytes(pathPKG), StandardOpenOption.APPEND);

        if (openOption == StandardOpenOption.CREATE_NEW) {
            logger.info("Generated script file [{}]", GREEN, scriptName);
        } else {
            logger.info("Script file [{}] was changed", CYAN, scriptName);
        }

        return scriptName + ".sql";
    }

    private void updateCommitScript(String packageName, String scriptName) throws IOException {
        Path script = Path.of(appArguments.getScriptsDirectory() + File.separator + scriptName);
        Path pathPKGSpec = Path.of(appArguments.getPackageDirectory() + File.separator + packageName
                + PACKAGE_SPECIFICATION_SUFFIX + ".sql");
        Path pathPKG = Path.of(appArguments.getPackageDirectory() + File.separator + packageName + ".sql");

        Files.write(script, Files.readAllBytes(pathPKGSpec), StandardOpenOption.TRUNCATE_EXISTING);
        Files.write(script, "\n\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        Files.write(script, Files.readAllBytes(pathPKG), StandardOpenOption.APPEND);

        logger.info("Script file [{}] was changed", CYAN, scriptName);
    }

    private String createRollbackScript(String packageName) throws IOException, GitAPIException {
        String scriptName = generateScriptName(getIssueName(), packageName) + ROLLBACK_SUFFIX;
        Path rollback = Path.of(String.format("%s%s%s.sql", appArguments.getScriptsDirectory(), File.separator, scriptName));
        Path pathPKGSpec = Path.of(appArguments.getPackageDirectory() + File.separator + packageName
                + PACKAGE_SPECIFICATION_SUFFIX + ".sql");
        Path pathPKG = Path.of(appArguments.getPackageDirectory() + File.separator + packageName + ".sql");
        StandardOpenOption openOption = StandardOpenOption.CREATE_NEW;
        if (Files.exists(rollback)) {
            openOption = StandardOpenOption.TRUNCATE_EXISTING;
        }
        //save new text from ddl
        String newDdlText = FileUtils.readFileToString(pathPKGSpec.toFile(), "UTF-8");
        //rollback changes ddl
        git.checkout().addPath("db/ddl/packages/" + packageName + PACKAGE_SPECIFICATION_SUFFIX + ".sql").call();
        //write old text ddl to script_rollback
        Files.write(rollback, Files.readAllBytes(pathPKGSpec), openOption);
        Files.write(rollback, "\n\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        //comeback new text in ddl
        FileUtils.write(pathPKGSpec.toFile(), newDdlText, "UTF-8");

        newDdlText = FileUtils.readFileToString(pathPKG.toFile(), "UTF-8");
        git.checkout().addPath("db/ddl/packages/" + packageName + ".sql").call();
        Files.write(rollback, Files.readAllBytes(pathPKG), StandardOpenOption.APPEND);
        FileUtils.write(pathPKG.toFile(), newDdlText, "UTF-8");

        if (openOption == StandardOpenOption.CREATE_NEW) {
            logger.info("Generating script file [{}]", GREEN, scriptName);
        } else {
            logger.info("Script file [{}] was changed", CYAN, scriptName);
        }

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

    private String generateScriptName(String issueName, String packageName) {
        List<File> files = FileUtils.listFiles(appArguments.getScriptsDirectory(), new String[]{"sql"}, false)
                .stream()
                .filter(file -> file.getName().startsWith("9"))
                .collect(Collectors.toList());
        int num = 9000;
        String script = String.format("_%s_%s", issueName, packageName);
        String fileName;
        for (File file : files) {
            fileName = file.getName();
            if (fileName.startsWith(String.valueOf(num))) {
                if (fileName.contains(script)) {
                    return fileName.substring(0, fileName.length() - 4);
                } else {
                    num++;
                }
            }
        }
        return num + script;
    }

    private File getRepositoryDirectory() {
        return new File(Path.of(appArguments.getScriptsDirectory().getParent()).toFile().getParent());
    }

    private String getPackageName(String fileName, boolean spec) {
        return spec ? fileName.substring(0, fileName.length() - 9) : fileName.substring(0, fileName.length() - 4);
    }

    private String getPackageNameFromScriptName(String fileName) throws IOException {
        //XXXX_issueName_
        int length = 6 + getIssueName().length();
        return fileName.endsWith(ROLLBACK_SUFFIX + ".sql") ?
                fileName.substring(length, fileName.length() - (ROLLBACK_SUFFIX.length() + 4)) :
                fileName.substring(length, fileName.length() - 4);
    }

    private String getIssueName() throws IOException {
        return git.getRepository().getBranch().split("_")[0];
    }

    private void createStash() {
        try {
            RevCommit stash = git.stashCreate().call();
            logger.info("Create Stash");
            git.stashApply().setStashRef(stash.getName()).call();
        } catch (GitAPIException e) {
            logger.info(e.getMessage(), RED);
        }
    }

    private void deleteStash() {
        try {
            logger.info("Delete auto created Stash");
            git.stashDrop().setStashRef(0).call();
        } catch (GitAPIException e) {
            logger.info(e.getMessage(), RED);
        }
    }

    private void applyStash() {
        try {
            logger.info("Apply auto created Stash");
            Collection<RevCommit> stashs = git.stashList().call();
            git.stashApply().setStashRef(stashs.iterator().next().getName()).call();
        } catch (GitAPIException e) {
            logger.info(e.getMessage(), RED);
        }
    }

    private RevCommit getMergeCommit() {
        try {
            String commitMessage;
            String mergeMessage = "Merge branch 'master' into " + repository.getBranch();
            Iterable<RevCommit> commits = git.log().setMaxCount(3).call();
            for (RevCommit commit : commits) {
                commitMessage = commit.getShortMessage();
                if (commitMessage.contains(mergeMessage)) {
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
                    return commit;
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