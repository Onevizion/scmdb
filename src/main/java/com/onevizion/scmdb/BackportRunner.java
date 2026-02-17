package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.BackportResult;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.nio.file.Files;

public class BackportRunner {

    private static final String PYTHON_SCRIPT_RESOURCE = "backport_pipeline_single_script.py";

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private ColorLogger logger;

    public BackportResult run() {
        int prNumber = promptForPrNumber();

        File tempScript = extractPythonScript();
        try {
            Process process = getProcess(tempScript, prNumber);

            StringBuilder stderrBuilder = new StringBuilder();
            Thread stderrThread = getThread(process, stderrBuilder);

            String stdout;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append(System.lineSeparator());
                }
                stdout = sb.toString();
            }

            int exitCode = process.waitFor();
            stderrThread.join(5000);

            if (stdout.isEmpty()) {
                String stderr = stderrBuilder.toString().trim();
                String detail = stderr.isEmpty() ? "" : "\nScript stderr:\n" + stderr;
                throw new RuntimeException("Backport script produced no output. Exit code: " + exitCode + detail);
            }

            return BackportResult.fromJson(stdout);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to execute backport script: " + e.getMessage(), e);
        } finally {
            tempScript.deleteOnExit();
        }
    }

    private @NonNull Process getProcess(File tempScript, int prNumber) throws IOException {
        File dbPath = appArguments.getScriptsDirectory().getParentFile();

        ProcessBuilder processBuilder = new ProcessBuilder("python3", tempScript.getAbsolutePath(),
                                                           String.valueOf(prNumber),
                                                           "--gh-token", appArguments.getGhToken(),
                                                           "--db-path", dbPath.getAbsolutePath());
        processBuilder.redirectErrorStream(false);
        return processBuilder.start();
    }

    private @NonNull Thread getThread(Process process, StringBuilder stderrBuilder) {
        Thread stderrThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logger.info(line, chooseColor(line));
                    stderrBuilder.append(line).append("\n");
                }
            } catch (IOException e) {
                logger.error("Error reading backport script stderr", e);
            }
        });
        stderrThread.start();
        return stderrThread;
    }

    private int promptForPrNumber() {
        System.out.print("Enter PR number: ");
        System.out.flush();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            String input = br.readLine();
            if (StringUtils.isBlank(input)) {
                throw new IllegalArgumentException("PR number is required.");
            }
            return Integer.parseInt(input.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid PR number. Must be an integer.");
        } catch (IOException e) {
            throw new RuntimeException("Can't read user console input.", e);
        }
    }

    private ColorLogger.Color chooseColor(String line) {
        if (line.startsWith("PR #") || line.startsWith("commits skipped")
                || line.startsWith("packages detected") || line.startsWith("changed")) {
            return ColorLogger.Color.CYAN;
        }
        if (line.startsWith("- ") && (line.contains("/scripts/") || line.contains("/ddl/"))) {
            return ColorLogger.Color.GREEN;
        }
        return ColorLogger.Color.WHITE;
    }

    private File extractPythonScript() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(PYTHON_SCRIPT_RESOURCE)) {
            if (is == null) {
                throw new RuntimeException("Backport Python script not found in classpath: " + PYTHON_SCRIPT_RESOURCE);
            }
            File tempFile = Files.createTempFile("backport_pipeline_", ".py").toFile();
            try (OutputStream os = new FileOutputStream(tempFile)) {
                is.transferTo(os);
            }
            return tempFile;
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract backport Python script: " + e.getMessage(), e);
        }
    }
}
