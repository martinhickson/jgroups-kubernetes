package org.jgroups.kubernetes.it.support;

/**
 * Shared naming for the kubernetes-it kind cluster, namespace, and workloads.
 * All resources use the {@code kubernetes-it} prefix so they are easy to find and tear down.
 */
public final class KubernetesItNames {

    public static final String CLUSTER = "kubernetes-it";
    public static final String NAMESPACE = "kubernetes-it";
    public static final String CONTEXT = "kind-kubernetes-it";
    public static final String DEPLOYMENT = "kubernetes-it-member";
    public static final String SERVICE_ACCOUNT = "kubernetes-it-sa";
    public static final String LABEL_SELECTOR = "kubernetes-it=jgroups";

    private KubernetesItNames() {
    }

    public static String contextForCluster(String cluster) {
        return "kind-" + cluster;
    }
}
