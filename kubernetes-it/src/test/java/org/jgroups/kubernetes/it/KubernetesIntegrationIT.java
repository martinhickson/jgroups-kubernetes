package org.jgroups.kubernetes.it;

import org.jgroups.kubernetes.it.member.JGroupsMemberMain;
import org.jgroups.kubernetes.it.support.KubernetesItBase;

import java.time.Duration;
import java.util.List;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Runs three real JGroups members in kind and verifies 3-member GMS formation, hello exchange
 * over TCP, token refresh, intentional cluster unform/reform via KUBE_PING, and hello after reform.
 */
public class KubernetesIntegrationIT extends KubernetesItBase {

    private static final int REFRESH_SECONDS = Integer.getInteger("kubernetes.it.token.refresh.seconds", 10);
    private static final int HELLO_DURATION_SECONDS = Integer.getInteger("kubernetes.it.hello.duration.seconds", 30);
    private static final int REFORM_HELLO_SECONDS = Integer.getInteger("kubernetes.it.reform.hello.duration.seconds", 15);

    @org.junit.Test
    public void threeMemberClusterExchangesHelloDuringTokenRefreshAndReformsAfter() throws Exception {
        List<String> pods = cluster.memberPodNames();
        System.out.printf("KUBERNETES-IT-TEST: member pods=%s%n", pods);
        assertTrue("Expected at least 3 member pods, found " + pods, pods.size() >= 3);

        // 1. Initial 3-member GMS formation via KUBE_PING.
        cluster.waitForMarkerInAllPods(JGroupsMemberMain.GMS_FORMED_3_MARKER, Duration.ofMinutes(3));
        cluster.assertMarkerPresentInAllPods(JGroupsMemberMain.GMS_FORMED_MARKER);

        for (String pod : pods) {
            String logs = cluster.podLogs(pod);
            assertTrue("Pod " + pod + " should log a 3-member GMS formation with addresses",
                    logs.contains(JGroupsMemberMain.GMS_FORMED_3_MARKER + " addresses=[")
                            && logs.contains("kubernetes-it-member-"));
        }
        System.out.println("KUBERNETES-IT-TEST: initial 3-member GMS formation logged on all pods");

        // 2. Hello phase while token refresh runs (10s interval, 30s duration).
        cluster.waitForMarkerInAllPods(JGroupsMemberMain.HELLO_PHASE_COMPLETE_MARKER, Duration.ofMinutes(4));

        cluster.waitForRepeatedMarkerInAllPods(JGroupsMemberMain.TOKEN_REFRESH_MARKER, 2,
                Duration.ofSeconds(HELLO_DURATION_SECONDS + REFRESH_SECONDS * 2L + 30L));

        cluster.waitForRepeatedMarkerInAllPods(JGroupsMemberMain.HELLO_SENT_MARKER, 2, Duration.ofSeconds(30));
        cluster.waitForRepeatedMarkerInAllPods(JGroupsMemberMain.HELLO_RECV_MARKER, 2, Duration.ofSeconds(30));

        cluster.assertAllPodsHaveMarkerAfterNthOccurrence(
                JGroupsMemberMain.TOKEN_REFRESH_MARKER, 2,
                JGroupsMemberMain.HELLO_SENT_MARKER, 1);
        cluster.assertAllPodsHaveMarkerAfterNthOccurrence(
                JGroupsMemberMain.TOKEN_REFRESH_MARKER, 2,
                JGroupsMemberMain.HELLO_RECV_MARKER, 1);

        cluster.waitForMarkerInAllPods(JGroupsMemberMain.TOKEN_401_RECOVERY_MARKER, Duration.ofMinutes(2));
        System.out.println("KUBERNETES-IT-TEST: initial hello + token refresh verified");

        // 3. After token refresh, cluster unforms and reforms via KUBE_PING discovery.
        cluster.waitForMarkerInAllPods(JGroupsMemberMain.GMS_UNFORMED_MARKER, Duration.ofMinutes(3));
        cluster.waitForMarkerInAllPods(JGroupsMemberMain.KUBE_DISCOVERY_AFTER_REFRESH_MARKER, Duration.ofMinutes(3));
        cluster.waitForMarkerInAllPods(JGroupsMemberMain.GMS_REFORMED_3_MARKER, Duration.ofMinutes(3));
        cluster.waitForMarkerInAllPods(JGroupsMemberMain.REFORM_PHASE_COMPLETE_MARKER, Duration.ofMinutes(4));

        cluster.assertAllPodsHaveMarkerAfterNthOccurrence(
                JGroupsMemberMain.GMS_REFORMED_3_MARKER, 1,
                JGroupsMemberMain.HELLO_SENT_MARKER, 1);
        cluster.assertAllPodsHaveMarkerAfterNthOccurrence(
                JGroupsMemberMain.GMS_REFORMED_3_MARKER, 1,
                JGroupsMemberMain.HELLO_RECV_MARKER, 1);

        String logs = cluster.allPodLogs();
        if (!logs.contains("Refreshed service account token from file")) {
            fail("Expected TokenStreamProvider refresh INFO logs in pod output:\n" + logs);
        }
        if (!logs.contains("Received HTTP 401 Unauthorized from Kubernetes API server; token refreshed for retry")) {
            fail("Expected HTTP 401 recovery log line in pod output:\n" + logs);
        }
        if (!logs.contains("helloPhase=after-reform")) {
            fail("Expected hello exchange after cluster reform in pod output:\n" + logs);
        }

        System.out.println("KUBERNETES-IT-TEST: unform/reform via KUBE_PING after token refresh verified on all pods");

        List<Path> logFiles = cluster.capturePodLogsToFiles();
        System.out.printf("KUBERNETES-IT-TEST: pod logs available under %s (%d files)%n",
                cluster.podLogsDirectory().toAbsolutePath(), logFiles.size());
    }
}
