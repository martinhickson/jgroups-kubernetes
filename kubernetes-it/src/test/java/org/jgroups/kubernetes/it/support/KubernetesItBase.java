package org.jgroups.kubernetes.it.support;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class KubernetesItBase {

    protected static KindClusterSupport cluster;

    @BeforeClass
    public static void startCluster() throws Exception {
        cluster = KindClusterSupport.acquireShared();
    }

    @AfterClass
    public static void stopCluster() throws Exception {
        KindClusterSupport.releaseShared();
        cluster = null;
    }
}
