package io.failify.examples.hdfs;

import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.dsl.entities.PortType;
import io.failify.dsl.entities.ServiceType;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.ULimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.StringJoiner;

public class FailifyHelper {
    public static final Logger logger = LoggerFactory.getLogger(FailifyHelper.class);

    public static Deployment getDeployment(int numOfDNs) {
        String version = "3.1.2"; // this can be dynamically generated from maven metadata
        String dir = "hadoop-" + version;
        return Deployment.builder("example-hdfs")
            .withService("zk").dockerImgName("zookeeper:3.4.14").disableClockDrift().and().withNode("zk1", "zk").and()
            .withService("hadoop-base")
                .appPath("../hadoop-3.1.2-build/hadoop-dist/target/" + dir + ".tar.gz", "/hadoop", PathAttr.COMPRESSED)
                .appPath("etc", "/hadoop/" + dir + "/etc").workDir("/hadoop/" + dir)
                .env("HADOOP_HOME", "/hadoop/" + dir).env("HADOOP_HEAPSIZE_MAX", "1g")
                .dockerImgName("failify/hadoop:1.0").dockerFileAddr("docker/Dockerfile", true)
                .logDir("/hadoop/" + dir + "/logs").serviceType(ServiceType.JAVA).and()
            .withService("nn", "hadoop-base").initCmd("bin/hdfs namenode -bootstrapStandby")
                .startCmd("bin/hdfs --daemon start zkfc && /bin/hdfs --daemon start namenode")
                .stopCmd("bin/hdfs --daemon stop namenode").and().nodeInstances(3, "nn", "nn", true)
            .withService("dn", "hadoop-base")
                .startCmd("bin/hdfs --daemon start datanode").stopCmd("bin/hdfs --daemon stop datanode")
                .and().nodeInstances(numOfDNs, "dn", "dn", true)
            .withService("jn", "hadoop-base")
                .startCmd("bin/hdfs --daemon start journalnode").stopCmd("bin/hdfs --dae stop journalnode")
                .and().nodeInstances(3, "jn", "jn", false)
            .node("nn1").initCmd("bin/hdfs namenode -format && bin/hdfs zkfc -formatZK").and().build();
    }

    public static void startNodesInOrder(FailifyRunner runner) throws InterruptedException, RuntimeEngineException {
        Thread.sleep(10000);
        runner.runtime().startNode("nn1");
        Thread.sleep(10000);
        runner.runtime().startNode("nn2"); runner.runtime().startNode("nn3");
        for (String node: runner.runtime().nodeNames()) if (node.startsWith("dn")) runner.runtime().startNode(node);
        Thread.sleep(10000);
    }
}
