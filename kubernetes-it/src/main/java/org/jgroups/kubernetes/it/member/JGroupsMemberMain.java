package org.jgroups.kubernetes.it.member;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.kubernetes.Client;
import org.jgroups.protocols.kubernetes.KUBE_PING;
import org.jgroups.protocols.kubernetes.stream.TokenStreamProvider;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runs inside each {@code kubernetes-it-member} pod: forms a JGroups cluster via {@link KUBE_PING}
 * and periodically exercises Kubernetes API token refresh.
 */
public final class JGroupsMemberMain {

    public static final String GMS_VIEW_MARKER = "KUBERNETES-IT-GMS-VIEW:";
    public static final String GMS_FORMED_MARKER = "KUBERNETES-IT-GMS-FORMED:";
    public static final String GMS_FORMED_3_MARKER = "KUBERNETES-IT-GMS-FORMED-3:";
    public static final String GMS_UNFORMED_MARKER = "KUBERNETES-IT-GMS-UNFORMED:";
    public static final String GMS_REFORM_START_MARKER = "KUBERNETES-IT-GMS-REFORM-START:";
    public static final String GMS_REFORMED_3_MARKER = "KUBERNETES-IT-GMS-REFORMED-3:";
    public static final String KUBE_DISCOVERY_AFTER_REFRESH_MARKER = "KUBERNETES-IT-KUBE-DISCOVERY-AFTER-REFRESH:";
    public static final String HELLO_SENT_MARKER = "KUBERNETES-IT-HELLO-SENT:";
    public static final String HELLO_RECV_MARKER = "KUBERNETES-IT-HELLO-RECV:";
    public static final String HELLO_PHASE_COMPLETE_MARKER = "KUBERNETES-IT-HELLO-PHASE-COMPLETE:";
    public static final String REFORM_PHASE_COMPLETE_MARKER = "KUBERNETES-IT-REFORM-PHASE-COMPLETE:";
    public static final String TOKEN_REFRESH_MARKER = "KUBERNETES-IT-TOKEN-REFRESH-OK";
    public static final String TOKEN_401_RECOVERY_MARKER = "KUBERNETES-IT-TOKEN-401-RECOVERY-OK";

    private static final AtomicBoolean SIMULATED_401 = new AtomicBoolean(false);
    private static final AtomicInteger TOKEN_REFRESH_PHASES = new AtomicInteger(0);
    private static final AtomicInteger HELLO_SENT_COUNT = new AtomicInteger(0);
    private static final AtomicInteger HELLO_RECV_COUNT = new AtomicInteger(0);
    private static final Logger log = Logger.getLogger(JGroupsMemberMain.class.getName());

    private JGroupsMemberMain() {
    }

    public static void main(String[] args) throws Exception {
        configureLogging();
        syncWritableServiceAccountToken();

        String namespace = requiredEnv("KUBERNETES_NAMESPACE");
        String labels = env("KUBERNETES_LABELS", "kubernetes-it=jgroups");
        String clusterName = env("JGROUPS_CLUSTER_NAME", "kubernetes-it-cluster");
        InetAddress podIp = InetAddress.getByName(requiredEnv("POD_IP"));
        int bindPort = Integer.parseInt(env("JGROUPS_BIND_PORT", "7800"));
        int refreshSeconds = Integer.parseInt(env("KUBERNETES_SA_TOKEN_REFRESH_INTERVAL_SECONDS", "10"));

        System.setProperty("KUBERNETES_SA_TOKEN_REFRESH_INTERVAL_SECONDS", Integer.toString(refreshSeconds));
        System.setProperty("KUBERNETES_SA_TOKEN_REFRESH_LOG_INFO",
                env("KUBERNETES_SA_TOKEN_REFRESH_LOG_INFO", "false"));
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");

        KUBE_PING kubePing = new KUBE_PING();
        kubePing.setValue("namespace", namespace);
        kubePing.setValue("labels", labels);
        kubePing.setValue("port_range", 0);
        kubePing.setValue("masterHost", env("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc"));
        kubePing.setValue("masterPort", Integer.parseInt(env("KUBERNETES_SERVICE_PORT", "443")));
        kubePing.setValue("saTokenFile", env("SA_TOKEN_FILE", "/var/run/kubernetes-it/token"));
        kubePing.setValue("dump_requests", true);

        kubePing.setValue("caCertFile", env("KUBERNETES_CA_CERTIFICATE_FILE",
                "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"));

        // Listen on all interfaces; advertise pod IP so peers can connect over the pod network.
        JChannel channel = new JChannel(
                new TCP()
                        .setValue("bind_addr", InetAddress.getByName("0.0.0.0"))
                        .setValue("external_addr", podIp)
                        .setValue("bind_port", bindPort),
                kubePing,
                new MERGE3(),
                new FD_ALL(),
                new NAKACK2(),
                new UNICAST3(),
                new GMS().setValue("join_timeout", 60000)
        );

        channel.setReceiver(new ReceiverAdapter() {
            @Override
            public void viewAccepted(View view) {
                logView(view);
            }

            @Override
            public void receive(Message msg) {
                if (msg.getLength() == 0) {
                    return;
                }
                String payload = new String(msg.getBuffer(), msg.getOffset(), msg.getLength(), StandardCharsets.UTF_8);
                if (!payload.startsWith("KUBERNETES-IT-HELLO")) {
                    return;
                }
                int received = HELLO_RECV_COUNT.incrementAndGet();
                System.out.printf("%s from=%s to=%s payload=%s totalReceived=%d%n",
                        HELLO_RECV_MARKER, msg.getSrc(), channel.getAddress(), payload, received);
            }
        });

        System.out.printf("KUBERNETES-IT-MEMBER-START: namespace=%s labels=%s cluster=%s podIp=%s tcpPort=%d refreshSeconds=%d%n",
                namespace, labels, clusterName, podIp.getHostAddress(), bindPort, refreshSeconds);

        channel.connect(clusterName);
        logView(channel.getView());

        Thread refreshProbe = new Thread(() -> refreshProbeLoop(kubePing, refreshSeconds), "kubernetes-it-token-probe");
        refreshProbe.setDaemon(true);
        refreshProbe.start();

        waitForThreeMemberView(channel);
        helloExchangeLoop(channel, "initial");

        int settleSeconds = Integer.parseInt(env("KUBERNETES_IT_REFORM_SETTLE_SECONDS", "20"));
        System.out.printf("KUBERNETES-IT-REFORM-SETTLE: waiting %ds before cluster unform%n", settleSeconds);
        TimeUnit.SECONDS.sleep(settleSeconds);

        waitForMinimumTokenRefreshes(2);
        waitFor401RecoveryIfEnabled();

        unformAndReformCluster(channel, clusterName, kubePing);

        Thread.currentThread().join();
    }

    private static void syncWritableServiceAccountToken() throws Exception {
        Path source = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token");
        Path target = Path.of("/var/run/kubernetes-it/token");
        Files.createDirectories(target.getParent());
        if (Files.isReadable(source)) {
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
        Thread copier = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (Files.isReadable(source)) {
                        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.log(Level.WARNING, "Failed syncing service account token", e);
                }
            }
        }, "kubernetes-it-token-sync");
        copier.setDaemon(true);
        copier.start();
    }

    private static void refreshProbeLoop(KUBE_PING kubePing, int refreshSeconds) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                String pods = kubePing.fetchFromKube();
                System.out.printf("KUBERNETES-IT-KUBE-FETCH: pods=%s%n", pods);

                probeTokenFileRefresh();
                maybeSimulate401Recovery();

                TimeUnit.SECONDS.sleep(refreshSeconds);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.out.printf("KUBERNETES-IT-KUBE-FETCH-ERROR: %s: %s%n",
                        e.getClass().getName(), e.getMessage());
                Util.sleep(refreshSeconds * 1000L);
            }
        }
    }

    private static void probeTokenFileRefresh() throws Exception {
        String tokenFile = env("SA_TOKEN_FILE", "/var/run/kubernetes-it/token");
        String caFile = env("KUBERNETES_CA_CERTIFICATE_FILE",
                "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
        String host = env("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc");
        int port = Integer.parseInt(env("KUBERNETES_SERVICE_PORT", "443"));
        String namespace = requiredEnv("KUBERNETES_NAMESPACE");

        Map<String, String> headers = new HashMap<>();
        TokenStreamProvider provider = new TokenStreamProvider(tokenFile, caFile);
        String masterUrl = String.format("https://%s:%d/api/v1", host, port);
        Client client = new Client(masterUrl, headers, 5000, 30000, 3, 500, provider,
                LogFactory.getLog(JGroupsMemberMain.class));

        int podCount = client.getPods(namespace, "kubernetes-it=jgroups", false).size();
        if (podCount >= 3) {
            int phase = TOKEN_REFRESH_PHASES.incrementAndGet();
            System.out.println(TOKEN_REFRESH_MARKER + " phase=" + phase + " podCount=" + podCount);
        }
    }

    private static void maybeSimulate401Recovery() {
        if (!"true".equalsIgnoreCase(env("KUBERNETES_IT_SIMULATE_401", "false"))
                || !SIMULATED_401.compareAndSet(false, true)) {
            return;
        }

        try {
            Path tokenFile = Path.of(env("SA_TOKEN_FILE", "/var/run/kubernetes-it/token"));
            String validToken = Files.readString(tokenFile, StandardCharsets.UTF_8).trim();
            Files.writeString(tokenFile, "invalid-token", StandardCharsets.UTF_8);

            Thread restorer = new Thread(() -> {
                Util.sleep(500);
                try {
                    Files.writeString(tokenFile, validToken, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    log.log(Level.SEVERE, "Failed restoring token during 401 simulation", e);
                }
            }, "kubernetes-it-token-restore");
            restorer.start();

            probeTokenFileRefresh();
            System.out.println(TOKEN_401_RECOVERY_MARKER);
        } catch (Exception e) {
            System.out.printf("KUBERNETES-IT-TOKEN-401-RECOVERY-FAILED: %s: %s%n",
                    e.getClass().getName(), e.getMessage());
        }
    }

    private static void waitForThreeMemberView(JChannel channel) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        while (channel.getView().size() < 3 && System.currentTimeMillis() < deadline) {
            TimeUnit.MILLISECONDS.sleep(500);
        }
        View view = channel.getView();
        if (view.size() < 3) {
            System.out.printf("KUBERNETES-IT-WARN: view size %d after wait; hello phase may be incomplete%n", view.size());
        }
        logView(view);
    }

    private static void waitForMinimumTokenRefreshes(int minimum) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
        while (TOKEN_REFRESH_PHASES.get() < minimum && System.currentTimeMillis() < deadline) {
            TimeUnit.MILLISECONDS.sleep(500);
        }
        if (TOKEN_REFRESH_PHASES.get() < minimum) {
            System.out.printf("KUBERNETES-IT-WARN: only %d token refresh phases before unform (wanted %d)%n",
                    TOKEN_REFRESH_PHASES.get(), minimum);
        }
    }

    private static void waitFor401RecoveryIfEnabled() throws InterruptedException {
        if (!"true".equalsIgnoreCase(env("KUBERNETES_IT_SIMULATE_401", "false"))) {
            return;
        }
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
        while (!SIMULATED_401.get() && System.currentTimeMillis() < deadline) {
            TimeUnit.MILLISECONDS.sleep(500);
        }
        TimeUnit.SECONDS.sleep(1);
    }

    private static void unformAndReformCluster(JChannel channel, String clusterName, KUBE_PING kubePing)
            throws Exception {
        View viewBeforeLeave = channel.getView();
        System.out.printf("%s cluster=%s view=%s refreshPhases=%d%n",
                GMS_UNFORMED_MARKER, clusterName, viewBeforeLeave, TOKEN_REFRESH_PHASES.get());

        channel.disconnect();
        System.out.println(GMS_VIEW_MARKER + " " + channel.getView());

        int pauseSeconds = Integer.parseInt(env("KUBERNETES_IT_REFORM_PAUSE_SECONDS", "5"));
        TimeUnit.SECONDS.sleep(pauseSeconds);

        String pods = kubePing.fetchFromKube();
        System.out.printf("%s refreshPhases=%d pods=%s%n",
                KUBE_DISCOVERY_AFTER_REFRESH_MARKER, TOKEN_REFRESH_PHASES.get(), pods);

        System.out.printf("%s cluster=%s refreshPhases=%d%n",
                GMS_REFORM_START_MARKER, clusterName, TOKEN_REFRESH_PHASES.get());

        channel.connect(clusterName);
        waitForThreeMemberViewAfterReform(channel);

        int reformHelloSeconds = Integer.parseInt(env("KUBERNETES_IT_REFORM_HELLO_DURATION_SECONDS", "15"));
        helloExchangeLoop(channel, "after-reform", reformHelloSeconds);

        System.out.printf("%s refreshPhases=%d sent=%d received=%d%n",
                REFORM_PHASE_COMPLETE_MARKER, TOKEN_REFRESH_PHASES.get(),
                HELLO_SENT_COUNT.get(), HELLO_RECV_COUNT.get());
    }

    private static void waitForThreeMemberViewAfterReform(JChannel channel) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        while (channel.getView().size() < 3 && System.currentTimeMillis() < deadline) {
            TimeUnit.MILLISECONDS.sleep(500);
        }
        View view = channel.getView();
        if (view.size() < 3) {
            System.out.printf("KUBERNETES-IT-WARN: reform view size %d after wait%n", view.size());
        }
        logReformedView(view);
    }

    private static void logReformedView(View view) {
        System.out.println(GMS_VIEW_MARKER + " " + view);
        if (view.size() >= 3) {
            String addresses = view.getMembers().stream().map(Object::toString).collect(Collectors.joining(", "));
            System.out.println(GMS_REFORMED_3_MARKER + " addresses=[" + addresses + "]");
        }
    }

    private static void helloExchangeLoop(JChannel channel, String phase) throws InterruptedException {
        int durationSeconds = Integer.parseInt(env("KUBERNETES_IT_HELLO_DURATION_SECONDS", "30"));
        helloExchangeLoop(channel, phase, durationSeconds);
    }

    private static void helloExchangeLoop(JChannel channel, String phase, int durationSeconds) throws InterruptedException {
        int intervalSeconds = Integer.parseInt(env("KUBERNETES_IT_HELLO_INTERVAL_SECONDS", "5"));
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(durationSeconds);

        System.out.printf("KUBERNETES-IT-HELLO-PHASE-START: phase=%s durationSeconds=%d intervalSeconds=%d%n",
                phase, durationSeconds, intervalSeconds);

        while (System.currentTimeMillis() < deadline) {
            View view = channel.getView();
            if (view.size() >= 3) {
                sendHelloToAllPeers(channel, view, phase);
            }
            TimeUnit.SECONDS.sleep(intervalSeconds);
        }

        System.out.printf("%s phase=%s sent=%d received=%d refreshPhases=%d%n",
                HELLO_PHASE_COMPLETE_MARKER, phase, HELLO_SENT_COUNT.get(), HELLO_RECV_COUNT.get(),
                TOKEN_REFRESH_PHASES.get());
    }

    private static void sendHelloToAllPeers(JChannel channel, View view, String helloPhase) {
        Address self = channel.getAddress();
        int refreshPhase = TOKEN_REFRESH_PHASES.get();
        String payload = "KUBERNETES-IT-HELLO from=" + self + " helloPhase=" + helloPhase
                + " refreshPhase=" + refreshPhase;

        for (Address member : view.getMembers()) {
            if (member.equals(self)) {
                continue;
            }
            try {
                channel.send(new Message(member, payload.getBytes(StandardCharsets.UTF_8)));
                int sent = HELLO_SENT_COUNT.incrementAndGet();
                System.out.printf("%s to=%s from=%s helloPhase=%s refreshPhase=%d totalSent=%d%n",
                        HELLO_SENT_MARKER, member, self, helloPhase, refreshPhase, sent);
            } catch (Exception e) {
                System.out.printf("KUBERNETES-IT-HELLO-SENT-FAILED: to=%s error=%s%n", member, e.getMessage());
            }
        }
    }

    private static void logView(View view) {
        System.out.println(GMS_VIEW_MARKER + " " + view);
        if (view.size() >= 3) {
            String addresses = view.getMembers().stream().map(Object::toString).collect(Collectors.joining(", "));
            System.out.println(GMS_FORMED_MARKER + " size=" + view.size() + " members=" + view.getMembers());
            System.out.println(GMS_FORMED_3_MARKER + " addresses=[" + addresses + "]");
        }
    }

    private static void configureLogging() {
        Logger.getLogger("org.jgroups").setLevel(Level.INFO);
        Logger.getLogger("org.jgroups.protocols.kubernetes").setLevel(Level.INFO);
        Logger.getLogger(TokenStreamProvider.class.getName()).setLevel(Level.INFO);
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return value;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
