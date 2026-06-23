package org.jgroups.kubernetes.it.support;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Provisions a kind cluster, loads the JGroups member image, and exposes pod log helpers.
 */
public final class KindClusterSupport implements AutoCloseable {

    private static KindClusterSupport shared;
    private static int sharedUsers;

    private final String cluster;
    private final String namespace;
    private final String context;
    private final String memberImage;
    private final boolean reuseCluster;
    private final boolean skipTeardown;
    private final Path projectRoot;
    private final Path binDir;

    private boolean createdCluster;

    public KindClusterSupport() {
        this.cluster = System.getProperty("kubernetes.it.cluster", KubernetesItNames.CLUSTER);
        this.namespace = System.getProperty("kubernetes.it.namespace", KubernetesItNames.NAMESPACE);
        this.context = KubernetesItNames.contextForCluster(cluster);
        this.memberImage = System.getProperty("kubernetes.it.image", "kubernetes-it-jgroups-member:it");
        this.reuseCluster = Boolean.parseBoolean(System.getProperty("kubernetes.it.reuse.cluster", "false"));
        this.skipTeardown = Boolean.parseBoolean(System.getProperty("kubernetes.it.skip.teardown", "false"));
        this.projectRoot = Path.of(System.getProperty("kubernetes.it.project.root",
                Path.of("").toAbsolutePath().normalize().toString()));
        this.binDir = projectRoot.resolve(".bin");
    }

    public static synchronized KindClusterSupport acquireShared() throws Exception {
        if (shared == null) {
            shared = new KindClusterSupport();
            shared.start();
        }
        sharedUsers++;
        return shared;
    }

    public static synchronized void releaseShared() throws Exception {
        if (shared == null) {
            return;
        }
        sharedUsers--;
        if (sharedUsers <= 0) {
            shared.close();
            shared = null;
            sharedUsers = 0;
        }
    }

    public void start() throws Exception {
        ensureBinaries();
        if (!clusterExists()) {
            run("kind", "create", "cluster", "--name", cluster, "--wait", "5m");
            createdCluster = true;
        } else if (!reuseCluster) {
            run("kind", "delete", "cluster", "--name", cluster);
            run("kind", "create", "cluster", "--name", cluster, "--wait", "5m");
            createdCluster = true;
        }

        buildMemberImage();
        loadMemberImageIntoKind();
        applyManifests();
        assertNamespaceExists();
        waitForDeployment();
    }

    public String namespace() {
        return namespace;
    }

    public List<String> memberPodNames() throws IOException, InterruptedException {
        String output = kubectl("get", "pods", "-n", namespace, "-l", KubernetesItNames.LABEL_SELECTOR,
                "-o", "jsonpath={.items[*].metadata.name}");
        if (output.isBlank()) {
            return List.of();
        }
        return List.of(output.split(" "));
    }

    public String podLogs(String podName) throws IOException, InterruptedException {
        return kubectl("logs", "-n", namespace, podName);
    }

    public String allPodLogs() throws IOException, InterruptedException {
        StringBuilder logs = new StringBuilder();
        for (String pod : memberPodNames()) {
            logs.append("===== ").append(pod).append(" =====").append(System.lineSeparator());
            logs.append(podLogs(pod)).append(System.lineSeparator());
        }
        return logs.toString();
    }

    public Path podLogsDirectory() {
        return projectRoot.resolve("target/logs");
    }

    /**
     * Writes full {@code kubectl logs} output for each member pod to {@code target/logs/&lt;pod&gt;.log}.
     */
    public List<Path> capturePodLogsToFiles() throws IOException, InterruptedException {
        Path dir = podLogsDirectory();
        Files.createDirectories(dir);
        List<Path> written = new ArrayList<>();
        for (String pod : memberPodNames()) {
            Path file = dir.resolve(pod + ".log");
            Files.writeString(file, podLogs(pod), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            written.add(file);
        }
        System.out.printf("KUBERNETES-IT-LOGS: wrote %d pod log file(s) to %s%n",
                written.size(), dir.toAbsolutePath());
        return written;
    }

    private void capturePodLogsQuietly() {
        try {
            capturePodLogsToFiles();
        } catch (Exception e) {
            System.err.printf("KUBERNETES-IT-WARN: failed capturing pod logs: %s%n", e.getMessage());
        }
    }

    public void waitForMarkerInAllPods(String marker, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        List<String> pods = memberPodNames();
        if (pods.size() < 3) {
            throw new IllegalStateException("Expected at least 3 member pods, found: " + pods);
        }

        while (Instant.now().isBefore(deadline)) {
            boolean allMatched = true;
            for (String pod : pods) {
                if (!podLogs(pod).contains(marker)) {
                    allMatched = false;
                    break;
                }
            }
            if (allMatched) {
                System.out.printf("KUBERNETES-IT-ASSERT: all %d pods contain marker '%s'%n", pods.size(), marker);
                return;
            }
            TimeUnit.SECONDS.sleep(2);
        }

        System.err.println("KUBERNETES-IT-FAILURE: marker '" + marker + "' not found in all pod logs:");
        System.err.println(allPodLogs());
        capturePodLogsQuietly();
        throw new AssertionError("Timed out waiting for marker '" + marker + "' in all pod logs");
    }

    public void waitForRepeatedMarkerInAllPods(String marker, int minimumCount, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        List<String> pods = memberPodNames();

        while (Instant.now().isBefore(deadline)) {
            boolean allMatched = true;
            for (String pod : pods) {
                if (countOccurrences(podLogs(pod), marker) < minimumCount) {
                    allMatched = false;
                    break;
                }
            }
            if (allMatched) {
                System.out.printf("KUBERNETES-IT-ASSERT: all pods contain marker '%s' at least %d times%n",
                        marker, minimumCount);
                return;
            }
            TimeUnit.SECONDS.sleep(2);
        }

        System.err.println("KUBERNETES-IT-FAILURE: marker '" + marker + "' not repeated in all pod logs:");
        System.err.println(allPodLogs());
        capturePodLogsQuietly();
        throw new AssertionError("Timed out waiting for marker '" + marker + "' at least " + minimumCount + " times");
    }

    public void assertMarkerPresentInAllPods(String marker) throws Exception {
        for (String pod : memberPodNames()) {
            String logs = podLogs(pod);
            if (!logs.contains(marker)) {
                throw new AssertionError("Pod " + pod + " missing marker '" + marker + "':\n" + logs);
            }
        }
        System.out.printf("KUBERNETES-IT-ASSERT: all pods contain marker '%s'%n", marker);
    }

    public void assertAllPodsHaveMarkerAfterNthOccurrence(String afterMarker, int afterCount,
            String targetMarker, int minimumTargetCount) throws Exception {
        for (String pod : memberPodNames()) {
            String logs = podLogs(pod);
            int index = nthOccurrenceIndex(logs, afterMarker, afterCount);
            if (index < 0) {
                throw new AssertionError("Pod " + pod + " has fewer than " + afterCount
                        + " occurrences of '" + afterMarker + "':\n" + logs);
            }
            String tail = logs.substring(index);
            if (countOccurrences(tail, targetMarker) < minimumTargetCount) {
                throw new AssertionError("Pod " + pod + " missing '" + targetMarker + "' after "
                        + afterCount + "x '" + afterMarker + "':\n" + logs);
            }
        }
        System.out.printf("KUBERNETES-IT-ASSERT: all pods contain '%s' at least %d times after %dx '%s'%n",
                targetMarker, minimumTargetCount, afterCount, afterMarker);
    }

    private static int nthOccurrenceIndex(String haystack, String needle, int n) {
        int index = 0;
        int found = 0;
        while ((index = haystack.indexOf(needle, index)) != -1) {
            found++;
            if (found == n) {
                return index;
            }
            index += needle.length();
        }
        return -1;
    }

    @Override
    public void close() throws Exception {
        capturePodLogsQuietly();
        if (skipTeardown) {
            return;
        }
        if (createdCluster && !reuseCluster) {
            run("kind", "delete", "cluster", "--name", cluster);
        }
    }

    private void buildMemberImage() throws IOException, InterruptedException {
        Path jar = projectRoot.resolve("target/kubernetes-it-member.jar");
        if (!Files.isRegularFile(jar)) {
            runProcess(List.of("mvn", "-q", "-f", projectRoot.resolve("pom.xml").toString(), "package", "-DskipTests"),
                    false);
        }
        if (!Files.isRegularFile(jar)) {
            throw new IllegalStateException("Member jar not found at " + jar);
        }
        runProcess(List.of("docker", "build", "-t", memberImage,
                "-f", projectRoot.resolve("docker/Dockerfile").toString(),
                projectRoot.toString()), false);
    }

    private void loadMemberImageIntoKind() throws IOException, InterruptedException {
        run("kind", "load", "docker-image", memberImage, "--name", cluster);
    }

    private void assertNamespaceExists() throws IOException, InterruptedException {
        String name = kubectl("get", "namespace", namespace, "-o", "jsonpath={.metadata.name}");
        if (!namespace.equals(name)) {
            throw new IllegalStateException("Namespace '" + namespace + "' was not created; kubectl returned: " + name);
        }
        System.out.printf("KUBERNETES-IT-ASSERT: namespace '%s' exists%n", namespace);
    }

    private void ensureBinaries() throws IOException, InterruptedException {
        Files.createDirectories(binDir);
        Path kind = binDir.resolve("kind");
        if (!Files.isExecutable(kind)) {
            download("https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64", kind);
        }
        Path kubectl = binDir.resolve("kubectl");
        if (!Files.isExecutable(kubectl)) {
            String stable = curl("https://dl.k8s.io/release/stable.txt").trim();
            download("https://dl.k8s.io/release/" + stable + "/bin/linux/amd64/kubectl", kubectl);
        }
    }

    private void download(String url, Path target) throws IOException, InterruptedException {
        runProcess(List.of("curl", "-fsSL", "-o", target.toString(), url), false);
        target.toFile().setExecutable(true);
    }

    private String curl(String url) throws IOException, InterruptedException {
        return runProcess(List.of("curl", "-fsSL", url), true);
    }

    private boolean clusterExists() throws IOException, InterruptedException {
        String output = runProcess(List.of(resolve("kind"), "get", "clusters"), true);
        return output.lines().anyMatch(line -> line.trim().equals(cluster));
    }

    private void applyManifests() throws IOException, InterruptedException {
        Path manifests = projectRoot.resolve("src/test/resources/k8s");
        run("kubectl", "--context", context, "apply", "-f", manifests.toString());
    }

    private void waitForDeployment() throws IOException, InterruptedException {
        run("kubectl", "--context", context, "-n", namespace,
                "rollout", "status", "statefulset/" + KubernetesItNames.DEPLOYMENT, "--timeout=300s");
    }

    private String kubectl(String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(resolve("kubectl"));
        command.add("--context");
        command.add(context);
        command.addAll(List.of(args));
        return runProcess(command, true);
    }

    private void run(String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(resolve(args[0]));
        for (int i = 1; i < args.length; i++) {
            command.add(args[i]);
        }
        runProcess(command, false);
    }

    private String resolve(String binary) {
        Path local = binDir.resolve(binary);
        return Files.isExecutable(local) ? local.toString() : binary;
    }

    private String runProcess(List<String> command, boolean captureOutput) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(command);
        Map<String, String> env = builder.environment();
        env.put("PATH", binDir.toAbsolutePath() + ":" + env.getOrDefault("PATH", ""));

        if (captureOutput) {
            builder.redirectErrorStream(true);
        } else {
            builder.inheritIO();
        }

        Process process = builder.start();
        String output = captureOutput ? read(process) : "";
        if (!process.waitFor(15, TimeUnit.MINUTES)) {
            process.destroyForcibly();
            throw new IllegalStateException("Timed out running: " + command);
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException("Command failed (" + process.exitValue() + "): " + command
                    + (output.isEmpty() ? "" : "\n" + output));
        }
        return output;
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int index = 0;
        while ((index = haystack.indexOf(needle, index)) != -1) {
            count++;
            index += needle.length();
        }
        return count;
    }

    private static String read(Process process) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }
}
