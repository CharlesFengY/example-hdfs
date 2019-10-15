package io.failify.examples.hdfs;

import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.dsl.entities.PortType;
import io.failify.dsl.entities.ServiceType;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.CommandResults;
import io.failify.execution.ULimit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.StringJoiner;

public class FailifyHelper {
    public static final Logger logger = LoggerFactory.getLogger(FailifyHelper.class);

    private static final String CLUSTER_NAME = "mycluster";
    private static final int NN_HTTP_PORT = 9870;
    private static final int NN_RPC_PORT = 8020;

    private int numOfDNs;
    private int numOfNNs;
    private FailifyRunner runner;
    private Deployment deployment;

    public FailifyHelper(int numOfNNs, int numOfDNs) {
        this.numOfDNs = numOfDNs;
        this.numOfNNs = numOfNNs;
    }

    public FailifyRunner start() throws RuntimeEngineException {
        String version = "3.1.2"; // this can be dynamically generated from maven metadata
        String dir = "hadoop-" + version;
        deployment = Deployment.builder("example-hdfs")
            .withService("zk").dockerImg("failify/zk:3.4.14").dockerFile("docker/zk", true)
                .disableClockDrift().and().withNode("zk1", "zk").and()
            .withService("hadoop-base")
                .appPath("../hadoop-3.1.2-build/hadoop-dist/target/" + dir + ".tar.gz", "/hadoop", PathAttr.COMPRESSED)
                .appPath("etc", "/hadoop/" + dir + "/etc").workDir("/hadoop/" + dir)
                .appPath("etc/hadoop/hdfs-site.xml", "/hadoop/" + dir + "/etc/hadoop/hdfs-site.xml",
                        new HashMap<String, String>() {{
                            put("NN_STRING", getNNString());
                            put("NN_ADDRESSES", getNNAddresses());
                        }})
                .env("HADOOP_HOME", "/hadoop/" + dir).env("HADOOP_HEAPSIZE_MAX", "1g")
                .dockerImg("failify/hadoop:1.0").dockerFile("docker/Dockerfile", true)
                .logDir("/hadoop/" + dir + "/logs").serviceType(ServiceType.JAVA).and()
            .withService("nn", "hadoop-base")
                .initCmd("bin/hdfs namenode -bootstrapStandby")
                .startCmd("bin/hdfs --daemon start zkfc && bin/hdfs --daemon start namenode").tcpPort(8020, 9870)
                .stopCmd("bin/hdfs --daemon stop namenode && bin/hdfs --daemon stop zkfc").and()
                .nodeInstances(numOfNNs, "nn", "nn", true)
            .withService("dn", "hadoop-base")
                .startCmd("bin/hdfs --daemon start datanode").stopCmd("bin/hdfs --daemon stop datanode")
                .and().nodeInstances(numOfDNs, "dn", "dn", true)
            .withService("jn", "hadoop-base")
                .startCmd("bin/hdfs --daemon start journalnode").stopCmd("bin/hdfs --daemon stop journalnode").and()
            .nodeInstances(3, "jn", "jn", false)
            .node("nn1").initCmd("bin/hdfs namenode -format && bin/hdfs zkfc -formatZK").and().build();
        runner = deployment.start();
        startNodesInOrder();
        return runner;
    }

    public void stop() {
        if (runner != null) {
            runner.stop();
        }
    }

    public void startNodesInOrder() throws RuntimeEngineException {
        try {
            Thread.sleep(10000);
            runner.runtime().startNode("nn1");
            Thread.sleep(15000);
            runner.runtime().startNode("nn2");
            runner.runtime().startNode("nn3");
            for (String node : runner.runtime().nodeNames())
                if (node.startsWith("dn")) runner.runtime().startNode(node);
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            logger.warn("startNodesInOrder sleep got interrupted");
        }
    }

    private String getNNString() {
        StringJoiner stringJoiner = new StringJoiner(",");
        for (int i=1; i<=numOfNNs; i++) {
            stringJoiner.add("nn" + i);
        }
        return stringJoiner.toString();
    }

    private String getNNAddresses() {
        String addrTemplate =
                "    <property>\n" +
                        "        <name>dfs.namenode.rpc-address.mycluster.{{NAME}}</name>\n" +
                        "        <value>{{NAME}}:8020</value>\n" +
                        "    </property>\n" +
                        "    <property>\n" +
                        "        <name>dfs.namenode.http-address.mycluster.{{NAME}}</name>\n" +
                        "        <value>{{NAME}}:9870</value>\n" +
                        "    </property>";

        String retStr = "";
        for (int i=1; i<=numOfNNs; i++) {
            retStr += addrTemplate.replace("{{NAME}}", "nn" + i);
        }
        return retStr;
    }

    public FailifyRunner runner() {
        return runner;
    }

    public Deployment deployment() {
        return deployment;
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(getConfiguration());
    }

    public Configuration getConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + CLUSTER_NAME);
        conf.set("dfs.client.failover.proxy.provider."+ CLUSTER_NAME,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.nameservices", CLUSTER_NAME);
        conf.set("dfs.ha.namenodes."+ CLUSTER_NAME, getNNString());

        for (int i=1; i<=numOfNNs; i++) {
            String nnIp = runner.runtime().ip("nn" + i);
            conf.set("dfs.namenode.rpc-address."+ CLUSTER_NAME +".nn" + i, nnIp + ":" +
                    runner.runtime().portMapping("nn" + i, NN_RPC_PORT, PortType.TCP));
            conf.set("dfs.namenode.http-address."+ CLUSTER_NAME +".nn" + i, nnIp + ":" +
                    runner.runtime().portMapping("nn" + i, NN_HTTP_PORT, PortType.TCP));
        }

        return conf;
    }

    private InetSocketAddress getNNRpcAddress(int index) {
        return new InetSocketAddress(runner.runtime().ip("nn" + index),
                runner.runtime().portMapping("nn" + index, NN_RPC_PORT, PortType.TCP));
    }

    private InetSocketAddress getNNHttpAddress(int index) {
        return new InetSocketAddress(runner.runtime().ip("nn" + index),
                runner.runtime().portMapping("nn" + index, NN_HTTP_PORT, PortType.TCP));
    }

    public void waitActive() throws RuntimeEngineException {

        for (int index=1; index<= numOfNNs; index++) {
            for (int retry=3; retry>0; retry--){
                logger.info("Checking if NN nn{} is UP (retries left {})", index, retry-1);
                if (this.assertNNisUpAndReceivingReport(index, numOfDNs))
                    continue;

                if (retry > 1) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        logger.warn("waitActive sleep got interrupted");
                    }
                }
            }
            throw new RuntimeException("NN nn" + index + " is not active or not receiving reports from DNs");
        }
        logger.info("The cluster is ACTIVE");
    }

    public boolean assertNNisUpAndReceivingReport(int index, int numOfDNs) throws RuntimeEngineException {
        if (runner.runtime().runCommandInNode("nn" + index, "bin/hdfs haadmin -ns " + CLUSTER_NAME + " -checkHealth nn" + index)
                .exitCode() != 0)
            return false;

        CommandResults res = runner.runtime().runCommandInNode("nn" + index, "bin/hdfs dfsadmin -report");
        if (res.exitCode() == 0 && res.stdOut().contains("Live datanodes (" + numOfDNs + ")"))
            return true;

        return false;
    }




}
