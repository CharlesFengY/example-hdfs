package io.failify.examples.hdfs;

import io.failify.FailifyRunner;
import io.failify.exceptions.RuntimeEngineException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);

    protected static final int NUM_OF_DNS = 3;

    private static final int BLOCK_SIZE = 4096;
    private static final int BLOCK_AND_A_HALF = BLOCK_SIZE * 3 / 2;

    private static final Path TEST_PATH =
            new Path("/test-file");

    protected static final Logger LOG = LoggerFactory.getLogger(
            SampleTest.class);


    /**
     * Test the scenario where the NN fails over after issuing a block
     * synchronization request, but before it is committed. The
     * DN running the recovery should then fail to commit the synchronization
     * and a later retry will succeed.
     */
    @Test
    public void testFailoverRightBeforeCommitSynchronization() throws IOException, RuntimeEngineException {
        final Configuration conf = new Configuration();
        // Disable permissions so that another user can recover the lease.
        conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
        conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

        FSDataOutputStream stm = null;

        FailifyHelper failifyHelper = new FailifyHelper(3, NUM_OF_DNS);
        FailifyRunner runner = failifyHelper.start();
        failifyHelper.waitActive();
        logger.info("The cluster is UP!");

        FileSystem fs = failifyHelper.getFileSystem();

        fs.create(new Path("/test.txt"));

        logger.info(" {} ", fs.listFiles(new Path("/"), false).next());
        runner.stop();

//        final MiniDFSCluster cluster = newMiniCluster(conf, 3);
//        try {
//            cluster.waitActive();
//            cluster.transitionToActive(0);
//            Thread.sleep(500);
//
//            LOG.info("Starting with NN 0 active");
//            FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
//            stm = fs.create(TEST_PATH);
//
//            // write a half block
//            AppendTestUtil.write(stm, 0, BLOCK_SIZE / 2);
//            stm.hflush();
//
//            // Look into the block manager on the active node for the block
//            // under construction.
//
//            NameNode nn0 = cluster.getNameNode(0);
//            ExtendedBlock blk = DFSTestUtil.getFirstBlock(fs, TEST_PATH);
//            DatanodeDescriptor expectedPrimary =
//                    DFSTestUtil.getExpectedPrimaryNode(nn0, blk);
//            LOG.info("Expecting block recovery to be triggered on DN " +
//                    expectedPrimary);
//
//            // Find the corresponding DN daemon, and spy on its connection to the
//            // active.
//            DataNode primaryDN = cluster.getDataNode(expectedPrimary.getIpcPort());
//            DatanodeProtocolClientSideTranslatorPB nnSpy =
//                    InternalDataNodeTestUtils.spyOnBposToNN(primaryDN, nn0);
//
//            // Delay the commitBlockSynchronization call
//            DelayAnswer delayer = new DelayAnswer(LOG);
//            Mockito.doAnswer(delayer).when(nnSpy).commitBlockSynchronization(
//                    Mockito.eq(blk),
//                    Mockito.anyLong(), // new genstamp
//                    Mockito.anyLong(), // new length
//                    Mockito.eq(true), // close file
//                    Mockito.eq(false), // delete block
//                    Mockito.any(),  // new targets
//                    Mockito.any()); // new target storages
//
//            DistributedFileSystem fsOtherUser = createFsAsOtherUser(cluster, conf);
//            assertFalse(fsOtherUser.recoverLease(TEST_PATH));
//
//            LOG.info("Waiting for commitBlockSynchronization call from primary");
//            delayer.waitForCall();
//
//            LOG.info("Failing over to NN 1");
//
//            cluster.transitionToStandby(0);
//            cluster.transitionToActive(1);
//
//            // Let the commitBlockSynchronization call go through, and check that
//            // it failed with the correct exception.
//            delayer.proceed();
//            delayer.waitForResult();
//            Throwable t = delayer.getThrown();
//            if (t == null) {
//                fail("commitBlockSynchronization call did not fail on standby");
//            }
//            GenericTestUtils.assertExceptionContains(
//                    "Operation category WRITE is not supported",
//                    t);
//
//            // Now, if we try again to recover the block, it should succeed on the new
//            // active.
//            loopRecoverLease(fsOtherUser, TEST_PATH);
//
//            AppendTestUtil.check(fs, TEST_PATH, BLOCK_SIZE/2);
//        } finally {
//            IOUtils.closeStream(stm);
//            cluster.shutdown();
//            runner.stop();
//        }
    }
}
