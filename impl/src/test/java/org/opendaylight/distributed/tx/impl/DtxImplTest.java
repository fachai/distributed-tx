package org.opendaylight.distributed.tx.impl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.CheckedFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.spi.TxException;
import org.opendaylight.distributed.tx.spi.TxProvider;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.Assert.fail;

public class DtxImplTest{
    InstanceIdentifier<NetconfNode1> netConfIid1;
    InstanceIdentifier<NetConfNode2> netConfIid2;
    InstanceIdentifier<DataStoreNode1> dataStoreIid1;
    InstanceIdentifier<DataStoreNode2> dataStoreIid2;
    InstanceIdentifier<TestIid1> n0;
    InstanceIdentifier<TestIid2> n1;
    InstanceIdentifier<TestIid3> n2;
    InstanceIdentifier<TestIid4> n3;

    DTXTestTransaction internalDtxNetconfTestTx1;
    DTXTestTransaction internalDtxNetconfTestTx2;
    DTXTestTransaction internalDtxDataStoreTestTx1;
    DTXTestTransaction internalDtxDataStoreTestTx2;

    Set<InstanceIdentifier<?>> netconfNodes;
    Set<InstanceIdentifier<?>> dataStoreNodes;
    Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap;
    List<InstanceIdentifier<? extends TestIid>> identifiers;
    DtxImpl netConfOnlyDTx;
    DtxImpl mixedDTx;
    ExecutorService threadPool;

    private class myNetconfTxProvider implements TxProvider{
        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            return nodeId == netConfIid1 ? internalDtxNetconfTestTx1 : internalDtxNetconfTestTx2;
        }

        @Override
        public boolean isDeviceLocked(InstanceIdentifier<?> device) {
            return false;
        }

        @Override
        public boolean lockTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {
            return true;
        }

        @Override
        public void releaseTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {

        }
    }

    private class myDataStoreTxProvider implements TxProvider{
        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            return nodeId == dataStoreIid1 ? internalDtxDataStoreTestTx1 : internalDtxDataStoreTestTx2;
        }

        @Override
        public boolean isDeviceLocked(InstanceIdentifier<?> device) {
            return false;
        }

        @Override
        public boolean lockTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {
            return true;
        }

        @Override
        public void releaseTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {

        }
    }

    private class TestIid implements DataObject{

        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }
    private class TestIid1 extends TestIid implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestIid2 extends TestIid implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestIid3 extends TestIid implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }
    private class TestIid4 extends TestIid implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class NetconfNode1 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class NetConfNode2 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class DataStoreNode1 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class DataStoreNode2 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }
    @Before
    @Test
    public void testInit(){
        netconfNodes = new HashSet<>();
        this.netConfIid1 = InstanceIdentifier.create(NetconfNode1.class);
        netconfNodes.add(netConfIid1);
        this.netConfIid2 = InstanceIdentifier.create(NetConfNode2.class);
        netconfNodes.add(netConfIid2);
        n0 = InstanceIdentifier.create(TestIid1.class);
        n1 = InstanceIdentifier.create(TestIid2.class);
        n2 = InstanceIdentifier.create(TestIid3.class);
        n3 = InstanceIdentifier.create(TestIid4.class);
        identifiers = Lists.newArrayList(n0, n1, n2, n3);

        internalDtxNetconfTestTx1 = new DTXTestTransaction();
        internalDtxNetconfTestTx1.addInstanceIdentifiers(n0, n1, n2, n3);
        internalDtxNetconfTestTx2 = new DTXTestTransaction();
        internalDtxNetconfTestTx2.addInstanceIdentifiers(n0, n1, n2, n3);
        Map<DTXLogicalTXProviderType, TxProvider> netconfTxProviderMap = new HashMap<>();
        TxProvider netconfTxProvider = new myNetconfTxProvider();
        netconfTxProviderMap.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, netconfTxProvider);
        netConfOnlyDTx = new DtxImpl(netconfTxProvider, netconfNodes, new DTxTransactionLockImpl(netconfTxProviderMap));

        dataStoreNodes = new HashSet<>();
        this.dataStoreIid1 = InstanceIdentifier.create(DataStoreNode1.class);
        dataStoreNodes.add(dataStoreIid1);
        this.dataStoreIid2 = InstanceIdentifier.create(DataStoreNode2.class);
        dataStoreNodes.add(dataStoreIid2);
        internalDtxDataStoreTestTx1 = new DTXTestTransaction();
        internalDtxDataStoreTestTx1.addInstanceIdentifiers(n0, n1, n2, n3);
        internalDtxDataStoreTestTx2 = new DTXTestTransaction();
        internalDtxDataStoreTestTx2.addInstanceIdentifiers(n0, n1, n2, n3);

        Set<DTXLogicalTXProviderType> providerTypes = Sets.newHashSet(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,DTXLogicalTXProviderType.NETCONF_TX_PROVIDER);
        //create two maps, transaction provider map and nodes map
        Map<DTXLogicalTXProviderType, TxProvider> txProviderMap = Maps.toMap(providerTypes, new Function<DTXLogicalTXProviderType, TxProvider>() {
            @Nullable
            @Override
            public TxProvider apply(@Nullable DTXLogicalTXProviderType dtxLogicalTXProviderType) {
                return dtxLogicalTXProviderType == DTXLogicalTXProviderType.NETCONF_TX_PROVIDER ? new myNetconfTxProvider() : new myDataStoreTxProvider();
            }
        });

        nodesMap = Maps.toMap(providerTypes, new Function<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>>() {
            @Nullable
            @Override
            public Set<InstanceIdentifier<?>> apply(@Nullable DTXLogicalTXProviderType dtxLogicalTXProviderType) {
                return dtxLogicalTXProviderType == DTXLogicalTXProviderType.NETCONF_TX_PROVIDER ?
                        (Set)Sets.newHashSet(netConfIid1, netConfIid2) : (Set)Sets.newHashSet(dataStoreIid1, dataStoreIid2);
            }
        });

        mixedDTx = new DtxImpl(txProviderMap, nodesMap, new DTxTransactionLockImpl(txProviderMap));
    }

    /**
     * test putAndRollbackOnFailure method for netconf only DTx
     * successfully put data in n0 of all the netconf nodes of DTx
     * ensure the size of data in the txs is correct
     */
    @Test
    public void testPutAndRollbackOnFailureInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception from the putAndRollbackOnFailure method");
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test putAndRollbackOnFailure method for mixed provider DTx
     * successfully put data in n0 of all the netConf and dataStore nodes of DTx
     * ensure the size of data in the txs is correct
     */
    @Test
    public void testPutAndRollbackOnFailureInMixedDTx(){
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception from the putAndRollbackOnFailure method");
        }

        int expectedDataSizeInNetconfTx1 = 1, expectedDataSizeInNetconfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expecteddataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expecteddataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test putAndRollbackOnFailure method for netConf only DTx, read fail and rollback succeed case
     * put data in n0 of netConf node1 and netConf node2
     * operation of netConf node2 failed for read exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testPutAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxNetconfTestTx2.setReadExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test putAndRollbackOnFailure method for netConf only DTx, put fail and rollback succeed case
     * put data in n0 of netConf node1 and netConf node2
     * operation of netConf node2 failed for put exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testPutAndRollbackOnFailurePutFailRollbackSucceedInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxNetconfTestTx2.setPutExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test putAndRollbackOnFailure method for mixed provider DTx, read fail and rollback succeed case
     * put data in n0 of all the nodes
     * operation of dataStore node2 failed for read exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testPutAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxDataStoreTestTx2.setReadExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f4.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetconfTx1 = 0, expectedDataSizeInNetconfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in netConfIid1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfIid2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test putAndRollbackOnFailure method for mixed provider DTx, put fail and rollback succeed case
     * put data in n0 of all the nodes
     * operation of dataStore node2 failed for put exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testPutAndRollbackOnFailurePutFailRollbackSucceedInMixedDTx() {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxDataStoreTestTx2.setPutExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f4.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetconfTx1 = 0, expectedDataSizeInNetconfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in netConfIid1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfIid2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test putAndRollbackOnFailure method for netConf only DTx, rollback fail case
     * put data in n0 of netConf node2
     * the operation failed for exception, perform rollback
     * rollback failed for submit exception, get DTxException.RollbackFailedException from DTx
     */
    @Test
    public void testPutAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.setPutExceptionByIid(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);

        try{
            f2.checkedGet();
            fail("Transaction2 can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected ReadFailException, the test failed", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * test putAndRollbackOnFailure method for mixed provider DTx, rollback fail case
     * put data in n0 of dataStore node2
     * the operation failed for exception, perform rollback
     * rollback failed for submit exception, get DTxException.RollbackFailedException from DTx
     */
    @Test
    public void testPutAndRollbackOnFailureRollbackFailInMixedDTx() {
        internalDtxDataStoreTestTx2.setPutExceptionByIid(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> f = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception from the transaction", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * test the thread safety of putAndRollbackOnFailure method for netConf only DTx
     * multi threads put data in different IIDs of netConf node1
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1 );
                    try{
                        f.checkedGet();
                    }catch (Exception e)
                    {
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of putAndRollbackOnFailure method for mixed provider DTx
     * multi threads put data in different IIDs of netConf node1
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureInMixedDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1 );
                    try{
                        f.checkedGet();
                    }catch (Exception e)
                    {
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of putAndRollbackOnFailure method for netConf only DTx, read fail and rollback succeed case
     * multi threads put data in different IIDs of netConf node1, one of the threads failed for read exception
     * rollback succeed
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(identifiers.get(finalI), true);
                    }
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1 );
                    try{
                        f.checkedGet();
                    }catch (Exception e)
                    {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of putAndRollbackOnFailure method for mixed provider DTx, read fail and rollback succeed case
     * multi threads put data in different IIDs of netConf node1 and dataStore node1, one of the threads failed for read exception
     * rollback succeed
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(identifiers.get(finalI), true);
                    }
                    CheckedFuture<Void, DTxException> netConfFuture = mixedDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1 );
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.putAndRollbackOnFailure(
                            DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), dataStoreIid1 );
                    try{
                        netConfFuture.checkedGet();
                        dataStoreFuture.checkedGet();
                    }catch (Exception e)
                    {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the netconf transaction is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the datastore transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of netconf identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastore identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether netConf only DTx will wait for all the operations finish to do the rollback
     * multi threads put data in the n0 of netConf node1
     * one of thread failed for read exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the tx and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentPutToSameNodeReadFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> writeFuture;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(n0, true);
                    }
                    writeFuture = netConfOnlyDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), netConfIid1);
                    try{
                        writeFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("The size of cached data in the tx is wrong", numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));

    }
    /**
     * test whether netConf only DTx will wait for all the operations finish to do the rollback
     * multi threads put data in the n0 of netConf node1
     * one of thread failed for put exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the tx and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentPutToSameNodePutFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> writeFuture;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setPutExceptionByIid(n0, true);
                    }
                    writeFuture = netConfOnlyDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), netConfIid1);
                    try{
                        writeFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("The size of cached data in tx is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether mixed provider DTx will wait for all the operations finish to do the rollback
     * multi threads put data in the n0 of netConf node1 and dataStore node1
     * one of thread failed for read exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the txs and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentPutToSameNodeReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int) (Math.random() * 3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int) (Math.random() * numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur) {
                        internalDtxNetconfTestTx1.setReadExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> netConfWriteFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), netConfIid1);;
                    CheckedFuture<Void, DTxException> dataStoreWriteFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), dataStoreIid1);;
                    try {
                        netConfWriteFuture.checkedGet();
                    } catch (DTxException e) {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                    try {
                        dataStoreWriteFuture.checkedGet();
                    } catch (DTxException e) {
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("The size of cached data in netConf node1 tx is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("The size of cached data in dataStore node1 tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1
        ));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in netConfTx is wrong",
                expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("The Size of data in dataStoreTx is wrong",
                expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether mixed provider DTx will wait for all the operations finish to do the rollback
     * multi threads put data in the n0 of netConf node1 and dataStore node1
     * one of thread failed for put exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the txs and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentPutToSameNodePutFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);

        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> netConfWriteFuture;
                    CheckedFuture<Void, DTxException> dataStoreWriteFuture;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setPutExceptionByIid(n0, true);
                    }
                    netConfWriteFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), netConfIid1);
                    dataStoreWriteFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), dataStoreIid1);
                    try{
                        netConfWriteFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                    try{
                        dataStoreWriteFuture.checkedGet();
                    }catch (DTxException e){
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("The size of cached data in netConf node1 tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("The size of cached data in dataStore node1 tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1
        ));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in netConfTx is wrong",
                expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("The Size of data in dataStoreTx is wrong",
                expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test mergeAndRollbackOnFailure method for netconf only DTx
     * successfully merge data in n0 of all the netconf nodes of DTx
     * ensure the size of data in the txs is correct
     */
    @Test
    public void testMergeAndRollbackOnFailureInNetConfOnlyDTx()  {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception");
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test mergeAndRollbackOnFailure method for mixed provider DTx
     * successfully merge data in n0 of all the netConf and dataStore nodes of DTx
     * ensure the size of data in the txs is correct
     */
    @Test
    public void testMergeAndRollbackOnFailureInMixedDTx()  {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);

        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception");
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test mergeAndRollbackOnFailure method for netConf only DTx, read fail and rollback succeed case
     * merge data in n0 of netConf node1 and netConf node2
     * operation of netConf node2 failed for read exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testMergeAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxNetconfTestTx2.setReadExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test mergeAndRollbackOnFailure method for netConf only DTx, merge fail and rollback succeed case
     * merge data in n0 of netConf node1 and netConf node2
     * operation of netConf node2 failed for merge exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testMergeAndRollbackOnFailureMergeFailRollbackSucceedInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxNetconfTestTx2.setMergeExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test mergeAndRollbackOnFailure method for mixed provider DTx, read fail and rollback succeed case
     * merge data in n0 of all the nodes
     * operation of dataStore node2 failed for read exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testMergeAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);

        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxDataStoreTestTx2.setReadExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try
        {
            f4.checkedGet();
            fail("get the unexpected exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test mergeAndRollbackOnFailure method for mixed provider DTx, merge fail and rollback succeed case
     * merge data in n0 of all the nodes
     * operation of dataStore node2 failed for merge exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testMergeAndRollbackOnFailureMergeFailRollbackSucceedInMixedDTx() {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);

        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxDataStoreTestTx2.setMergeExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try
        {
            f4.checkedGet();
            fail("get the unexpected exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test mergeAndRollbackOnFailure method for netConf only DTx, rollback fail case
     * merge data in n0 of netConf node2
     * the operation failed for exception, perform rollback
     * rollback failed for submit exception, get DTxException.RollbackFailedException from DTx
     */
    @Test
    public void testMergeAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.setMergeExceptionByIid(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f2.checkedGet();
            fail("Transaction2 can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected ReadFailException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * test mergeAndRollbackOnFailure method for mixed provider DTx, rollback fail case
     * merge data in n0 of dataStore node2
     * the operation failed for exception, perform rollback
     * rollback failed for submit exception, get DTxException.RollbackFailedException from DTx
     */
    @Test
    public void testMergeAndRollbackOnFailureRollbackFailInMixedDTx() {
        internalDtxDataStoreTestTx2.setMergeExceptionByIid(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> f = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * test the thread safety of mergeAndRollbackOnFailure method for netConf only DTx
     * multi threads merge data in different IIDs of netConf node1
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                    try{
                        f.checkedGet();
                    }catch (Exception e)
                    {
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of mergeAndRollbackOnFailure method for mixed provider DTx
     * multi threads merge data in different IIDs of netConf node1
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureInMixedDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), dataStoreIid1);
                    try{
                        dataStoreFuture.checkedGet();
                    }catch (Exception e)
                    {
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",
                numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of mergeAndRollbackOnFailure method for netConf only DTx, read fail and rollback succeed case
     * multi threads merge data in different IIDs of netConf node1, one of the threads failed for read exception
     * rollback succeed
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(identifiers.get(finalI), true);
                    }
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                    try{
                        f.checkedGet();
                    }catch (Exception e)
                    {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of mergeAndRollbackOnFailure method for mixed provider DTx, read fail and rollback succeed case
     * multi threads merge data in different IIDs of netConf node1 and dataStore node1, one of the threads failed for read exception
     * rollback succeed
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (errorOccur == finalI){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(identifiers.get(finalI), true);
                    }
                    CheckedFuture<Void, DTxException> netConfFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), dataStoreIid1);
                    try{
                        netConfFuture.checkedGet();
                        dataStoreFuture.checkedGet();
                    }catch (Exception e)
                    {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the transaction is wrong",
                numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether netConf only DTx will wait for all the operations finish to do the rollback
     * multi threads merge data in the n0 of netConf node1
     * one of thread failed for read exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the tx and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentMergeToSameNodeReadFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> writeFuture;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(n0, true);
                    }
                    writeFuture = netConfOnlyDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), netConfIid1);
                    try{
                        writeFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("The size of cached data in the tx is wrong", numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether netConf only DTx will wait for all the operations finish to do the rollback
     * multi threads merge data in the n0 of netConf node1
     * one of thread failed for merge exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the tx and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentMergeToSameNodeMergeFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> writeFuture;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setMergeExceptionByIid(n0, true);
                    }
                    writeFuture = netConfOnlyDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), netConfIid1);
                    try{
                        writeFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("The size of cached data in the tx is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether mixed provider DTx will wait for all the operations finish to do the rollback
     * multi threads merge data in the n0 of netConf node1 and dataStore node1
     * one of thread failed for read exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the txs and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentMergeToSameNodeReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> netConfFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), netConfIid1);
                    CheckedFuture<Void, DTxException> datastoreFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), dataStoreIid1);
                    try{
                        netConfFuture.checkedGet();
                        datastoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("The size of cached data in netConf node1 tx is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("The size of cached data in dataStore node1 tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1
        ));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in netconf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("The Size of data in datastore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether mixed provider DTx will wait for all the operations finish to do the rollback
     * multi threads merge data in the n0 of netConf node1 and dataStore node1
     * one of thread failed for merge exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the txs and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentMergeToSameNodeMergeFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setMergeExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> netConfFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), netConfIid1);
                    CheckedFuture<Void, DTxException> datastoreFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), dataStoreIid1);
                    try{
                        netConfFuture.checkedGet();
                        datastoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("The size of cached data in netConf node1 tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("The size of cached data in dataStore node1 tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1
        ));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in netconf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("The Size of data in datastore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test deleteAndRollbackOnFailure method for netconf only DTx
     * successfully delete data in n0 of all the netconf nodes of DTx
     * ensure the size of data in the txs is correct
     */
    @Test
    public void testDeleteAndRollbackOnFailureInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);
        CheckedFuture<Void, DTxException> deleteFuture1 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        CheckedFuture<Void, DTxException> deleteFuture2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        try
        {
            deleteFuture1.checkedGet();
            deleteFuture2.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        int expectedDataSizeInTx1 = 0,expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size in netConfIid1 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test deleteAndRollbackOnFailure method for mixed provider DTx
     * successfully delete data in n0 of all the netConf and dataStore nodes of DTx
     * ensure the size of data in the txs is correct
     */
    @Test
    public void testDeleteAndRollbackOnFailureInMixedDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);

        try {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test deleteAndRollbackOnFailure method for netConf only DTx, read fail and rollback succeed case
     * delete data in n0 of netConf node1 and netConf node2
     * operation of netConf node2 failed for read exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testDeleteAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setReadExceptionByIid(n0,true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception ");
        }

        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get the exception");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception ", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("Size in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test deleteAndRollbackOnFailure method for netConf only DTx, put fail and rollback succeed case
     * delete data in n0 of netConf node1 and netConf node2
     * operation of netConf node2 failed for delete exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testDeleteAndRollbackOnFailureDeleteFailRollbackSucceedInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setDeleteExceptionByIid(n0,true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception ");
        }

        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get the exception");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception ", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("Size in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Size in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test deleteAndRollbackOnFailure method for mixed provider DTx, read fail and rollback succeed case
     * delete data in n0 of all the nodes
     * operation of dataStore node2 failed for read exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testDeleteAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid1);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception");
        }

        internalDtxDataStoreTestTx2.setReadExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);
        try{
            f4.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test deleteAndRollbackOnFailure method for mixed provider DTx, delete fail and rollback succeed case
     * delete data in n0 of all the nodes
     * operation of dataStore node2 failed for delete exception, perform rollback
     * rollback succeed and get the DTxException.EditFailedException from DTx
     * ensure the size of data in txs is correct
     */
    @Test
    public void testDeleteAndRollbackOnFailureDeleteFailRollbackSucceedInMixedDTx(){
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid1);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception");
        }

        internalDtxDataStoreTestTx2.setDeleteExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);
        try{
            f4.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test deleteAndRollbackOnFailure method for netConf only DTx, rollback fail case
     * delete data in n0 of netConf node2
     * the operation failed for exception, perform rollback
     * rollback failed for submit exception, get DTxException.RollbackFailedException from DTx
     */
    @Test
    public void testDeleteAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setDeleteExceptionByIid(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception the test failed", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * test deleteAndRollbackOnFailure method for mixed provider DTx, rollback fail case
     * delete data in n0 of dataStore node2
     * the operation failed for exception, perform rollback
     * rollback failed for submit exception, get DTxException.RollbackFailedException from DTx
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackFailInMixedDTx(){
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.setDeleteExceptionByIid(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> f = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException);
        }
    }

    /**
     * test the thread safety of deleteAndRollbackOnFailure method for netConf only DTx
     * multi threads delete data in different IIDs of netConf node1
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
        }
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,identifiers.get(finalI), netConfIid1);
                    try{
                        f.checkedGet();
                    }catch (Exception e)
                    {
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of deleteAndRollbackOnFailure method for mixed provider DTx
     * multi threads delete data in different IIDs of netConf node1 and dataStore node1
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureInMixedDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
            internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
        }
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> netConfFuture = mixedDTx.deleteAndRollbackOnFailure(
                            LogicalDatastoreType.OPERATIONAL,identifiers.get(finalI), netConfIid1);
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(
                            DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,identifiers.get(finalI), dataStoreIid1);
                    try{
                        netConfFuture.checkedGet();
                        dataStoreFuture.checkedGet();
                    }catch (Exception e)
                    {
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the netconf transaction is wrong",numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the datastore transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of netconf identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastore identifiers's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of deleteAndRollbackOnFailure method for netConf only DTx, read fail and rollback succeed case
     * multi threads delete data in different IIDs of netConf node1, one of the threads failed for read exception
     * rollback succeed
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(identifiers.get(finalI), true);
                    }
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), netConfIid1 );
                    try{
                        f.checkedGet();
                    }catch (Exception e)
                    {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1,netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test the thread safety of deleteAndRollbackOnFailure method for mixed provider DTx, read fail and rollback succeed case
     * multi threads delete data in different IIDs of netConf node1 and dataStore node1, one of the threads failed for read exception
     * rollback succeed
     * ensure the size of cached data in the txs and the size of data in IID is correct
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
            internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(identifiers.get(finalI), true);
                    }
                    CheckedFuture<Void, DTxException> netConfFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION,identifiers.get(finalI), netConfIid1);
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, identifiers.get(finalI), dataStoreIid1);
                    try{
                        netConfFuture.checkedGet();
                        dataStoreFuture.checkedGet();
                    }catch (Exception e)
                    {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the netconf transaction is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the datastore transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of netconf identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastore identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether netConf only DTx will wait for all the operations finish to do the rollback
     * multi threads delete data in the n0 of netConf node1
     * one of thread failed for read exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the tx and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentDeleteToSameNodeReadFailRollbackSucceedInNetConfDTx(){
        int numOfThreads = (int) (Math.random() * 3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int) (Math.random() * numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur) {
                        internalDtxNetconfTestTx1.setReadExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> writeFuture = netConfOnlyDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, netConfIid1);
                    try {
                        writeFuture.checkedGet();
                    } catch (DTxException e) {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1,netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether mixed provider DTx will wait for all the operations finish to do the rollback
     * multi threads delete data in the n0 of netConf node1 and dataStore node1
     * one of thread failed for read exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the txs and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentDeleteToSameNodeReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> netConfFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, netConfIid1);
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, dataStoreIid1);
                    try{
                        netConfFuture.checkedGet();
                        dataStoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the erong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the netconf transaction is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the datastore transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        int expectedDataSizeInIdentifier = 1;
        Assert.assertEquals("The Size of data in netconf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("The Size of data in datastore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether netConf only DTx will wait for all the operations finish to do the rollback
     * multi threads delete data in the n0 of netConf node1
     * one of thread failed for delete exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the tx and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentDeleteToSameNodeDeleteFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int) (Math.random() * 3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int) (Math.random() * numOfThreads);
        internalDtxNetconfTestTx1.setDeleteExceptionByIid(n0, true);
        for (int i = 0; i < numOfThreads; i++) {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur) {
                        internalDtxNetconfTestTx1.setDeleteExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> writeFuture = netConfOnlyDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, netConfIid1);
                    try {
                        writeFuture.checkedGet();
                    } catch (DTxException e) {
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads,netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether mixed provider DTx will wait for all the operations finish to do the rollback
     * multi threads delete data in the n0 of netConf node1 and dataStore node1
     * one of thread failed for delete exception and perform rollback
     * rollback succeed
     * ensure the size of cached data in the txs and size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentDeleteToSameNodeWithObjExDeleteFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setDeleteExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> netConfFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, netConfIid1);
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, dataStoreIid1);
                    try{
                        netConfFuture.checkedGet();
                        dataStoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                        else
                            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the netconf transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the datastore transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        int expectedDataSizeInIdentifier = 1;
        Assert.assertEquals("The Size of data in netconf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("The Size of data in datastore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * test whether the submit action wait for all the put actions to finish in netConf only DTx
     * multi threads put data in different IIDs of all the netConf nodes
     * right after all the threads finish, invoking submit method
     * ensure the cached data in the txs and the size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentPutAndSubmitInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads * netconfNodes.size());
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            for (int i = 0; i < numOfThreads; i++) {
                final int finalI = i;
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        netConfOnlyDTx.putAndRollbackOnFailure(
                                LogicalDatastoreType.OPERATIONAL, (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), nodeIid);
                    }
                });
            }
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()){
            Thread.yield();
        }
        netConfOnlyDTx.submit();
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            Assert.assertEquals("Size of data in the transaction is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
            int expectedDataSizeInIdentifier = 1;
            for (int i = 0; i < numOfThreads; i++) {
                Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            }
        }
    }

    /**
     * test whether the submit action wait for all the put actions to finish in mixed provider DTx
     * multi threads put data in different IIDs of all the netConf nodes and dataStore nodes
     * right after all the threads finish, invoking submit method
     * ensure the cached data in the txs and the size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentPutAndSubmitInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads * 4);
        for (final DTXLogicalTXProviderType type : nodesMap.keySet()){
            for (final InstanceIdentifier<?> nodeIid : nodesMap.get(type)){
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            mixedDTx.putAndRollbackOnFailure(type, LogicalDatastoreType.OPERATIONAL,
                                    (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), nodeIid);
                        }
                    });
                }

            }
        }

        threadPool.shutdown();
        while (!threadPool.isTerminated()){
            Thread.yield();
        }
        mixedDTx.submit();
        for (InstanceIdentifier<?> nodeId : netconfNodes){
            Assert.assertEquals("Size of cached data in the netconf tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, nodeId
            ));
        }
        for (InstanceIdentifier<?> nodeId : dataStoreNodes){
            Assert.assertEquals("Size of cached data in the datastore tx is wrong", numOfThreads,mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, nodeId
            ));
        }
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("size of netconfNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of netconfNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether the submit action wait for all the merge actions to finish in netConf only DTx
     * multi threads merge data in different IIDs of all the netConf nodes
     * right after all the threads finish, invoking submit method
     * ensure the cached data in the txs and the size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentMergeAndSubmitInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads * netconfNodes.size());
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            for (int i = 0; i < numOfThreads; i++) {
                final int finalI = i;
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        netConfOnlyDTx.mergeAndRollbackOnFailure(
                                LogicalDatastoreType.OPERATIONAL, (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), nodeIid);
                    }
                });
            }
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()){
            Thread.yield();
        }
        netConfOnlyDTx.submit();
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            Assert.assertEquals("Size of data in the transaction is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
            int expectedDataSizeInIdentifier = 1;
            for (int i = 0; i < numOfThreads; i++) {
                Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            }
        }
    }

    /**
     * test whether the submit action wait for all the merge actions to finish in mixed provider DTx
     * multi threads merge data in different IIDs of all the netConf nodes and dataStore nodes
     * right after all the threads finish, invoking submit method
     * ensure the cached data in the txs and the size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentMergeAndSubmitInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads * 4);
        for (final DTXLogicalTXProviderType type : nodesMap.keySet()){
            for (final InstanceIdentifier<?> nodeIid : nodesMap.get(type)){
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            mixedDTx.mergeAndRollbackOnFailure(type, LogicalDatastoreType.OPERATIONAL,
                                    (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), nodeIid);
                        }
                    });
                }

            }
        }

        threadPool.shutdown();
        while (!threadPool.isTerminated()){
            Thread.yield();
        }
        mixedDTx.submit();
        for (InstanceIdentifier<?> nodeId : netconfNodes){
            Assert.assertEquals("Size of cached data in the netconf tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, nodeId
            ));
        }
        for (InstanceIdentifier<?> nodeId : dataStoreNodes){
            Assert.assertEquals("Size of cached data in the datastore tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, nodeId
            ));
        }
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("size of netconfNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of netconfNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether the submit action wait for all the delete actions to finish in netConf only DTx
     * multi threads delete data in different IIDs of all the netConf nodes
     * right after all the threads finish, invoking submit method
     * ensure the cached data in the txs and the size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentDeleteAndSubmitInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads * netconfNodes.size());
        for (int i = 0; i < numOfThreads; i++){
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
            internalDtxNetconfTestTx2.createObjForIdentifier(identifiers.get(i));
        }
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            for (int i = 0; i < numOfThreads; i++) {
                final int finalI = i;
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        netConfOnlyDTx.deleteAndRollbackOnFailure(
                                LogicalDatastoreType.OPERATIONAL, (InstanceIdentifier<TestIid>)identifiers.get(finalI), nodeIid);
                    }
                });
            }
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()){
            Thread.yield();
        }
        netConfOnlyDTx.submit();
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            Assert.assertEquals("Size of data in the transaction is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
        }
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether the submit action wait for all the delete actions to finish in mixed provider DTx
     * multi threads delete data in different IIDs of all the netConf nodes and dataStore nodes
     * right after all the threads finish, invoking submit method
     * ensure the cached data in the txs and the size of data in the IIDs is correct
     */
    @Test
    public void testConcurrentDeleteAndSubmitInMixedDTx(){
        int numOfThreads = (int) (Math.random() * 3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads * 4);
        for (int i = 0; i < numOfThreads; i++) {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
            internalDtxNetconfTestTx2.createObjForIdentifier(identifiers.get(i));
            internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
            internalDtxDataStoreTestTx2.createObjForIdentifier(identifiers.get(i));
        }
        for (final DTXLogicalTXProviderType type : nodesMap.keySet()) {
            for (final InstanceIdentifier<?> nodeIid : nodesMap.get(type)) {
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            mixedDTx.deleteAndRollbackOnFailure(type, LogicalDatastoreType.OPERATIONAL,
                                    (InstanceIdentifier<TestIid>) identifiers.get(finalI), nodeIid);
                        }
                    });
                }

            }
        }

        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        mixedDTx.submit();
        for (InstanceIdentifier<?> nodeId : netconfNodes){
            Assert.assertEquals("Size of cached data in the netconf tx is wrong", numOfThreads,mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, nodeId
            ));
        }
        for (InstanceIdentifier<?> nodeId : dataStoreNodes){
            Assert.assertEquals("Size of cached data in the datastore tx is wrong", numOfThreads,mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, nodeId
            ));
        }
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("size of netconfNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of netconfNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test best effort put method
     */
    @Test public void testPut()  {
        netConfOnlyDTx.put(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
    }

    /**
     * test best effort merge method
     */
    @Test public void testMerge() {
        netConfOnlyDTx.merge(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
    }

    /**
     * test best effort delete method
     */
    @Test public void testDelete() {
        netConfOnlyDTx.delete(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
    }

    /**
     * test rollback method for netConf Only DTx, rollback successfully case
     * put data in n0 of all the netConf nodes, invoke rollback
     * ensure the size of data in the IIDs is correct
     */
    @Test
    public void testRollbackInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);

        try
        {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception from the put");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> f = netConfOnlyDTx.rollback();
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        try{
            f.checkedGet();
            Assert.assertEquals("Size of n0'netconfNodes data in netConfIid1 is wrong after rollback", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
            Assert.assertEquals("Size of n0'netconfNodes data in netConfIid2 is wrong after rollback", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        }catch (Exception e)
        {
            fail("rollback get unexpected exception");
        }
    }

    /**
     * test rollback method for mixed provider DTx, rollback successfully case
     * put data in n0 of all the netConf nodes and dataStore nodes, invoke rollback
     * ensure the size of data in the IIDs is correct
     */
    @Test
    public void testRollbackInMixedDTx() {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);

        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFut = mixedDTx.rollback();
        try{
            rollbackFut.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test rollback method for netConf Only DTx, rollback fail case
     * put data in n0 of all the netConf nodes, invoke rollback
     * rollback fail for submit exception, get DTxException.RollbackException from DTx
     */
    @Test
    public void testRollbackFailInNetConfOnlyDTx()
    {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> f = netConfOnlyDTx.rollback();
        try
        {
            f.checkedGet();
            fail("no exception occur");
        }catch (Exception e)
        {
            Assert.assertTrue("type of exception is wrong", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * test rollback method for mixed provider DTx, rollback fail case
     * put data in n0 of all the netConf nodes and dataStore nodes, invoke rollback
     * rollback fail for submit exception, get DTxException.RollbackException from DTx
     */
    @Test
    public void testRollbackFailInMixedDTx()
    {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxDataStoreTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFut = mixedDTx.rollback();
        try{
            rollbackFut.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * test whether rollback operation wait for all the put action to finish in netConf only DTx
     * multi threads put data in different IIDs of all the netConf nodes
     * right after all the threads finish, invoking the rollback
     * ensure the size of data in the IID is correct
     */
    @Test
    public void testConcurrentPutRollbackInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = netConfOnlyDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("can't perform rollback, the test failed");
        }
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether rollback operation wait for all the put action to finish in mixed provider DTx
     * multi threads put data in different IIDs of all the netConf nodes and dataStore nodes
     * right after all the threads finish, invoking the rollback
     * ensure the size of data in the IID is correct
     */
    @Test
    public void testConcurrentPutRollbackInMixedDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                    mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), dataStoreIid1);
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = mixedDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Can't perform rollback, test failed");
        }
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether rollback operation wait for all the merge action to finish in netConf only DTx
     * multi threads merge data in different IIDs of all the netConf nodes
     * right after all the threads finish, invoking the rollback
     * ensure the size of data in the IID is correct
     */
    @Test
    public void testConcurrentMergeRollbackInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = netConfOnlyDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("can't perform rollback, the test failed");
        }
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether rollback operation wait for all the merge action to finish in mixed provider DTx
     * multi threads merge data in different IIDs of all the netConf nodes and dataStore nodes
     * right after all the threads finish, invoking the rollback
     * ensure the size of data in the IID is correct
     */
    @Test
    public void testConcurrentMergeRollbackInMixedDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                    mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), dataStoreIid1);
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = mixedDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Can't perform rollback, test failed");
        }
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether rollback operation wait for all the delete action to finish in netConf only DTx
     * multi threads delete data in different IIDs of all the netConf nodes
     * right after all the threads finish, invoking the rollback
     * ensure the size of data in the IID is correct
     */
    @Test
    public void testConcurrentDeleteRollbackInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
        }
        for (int i = 0; i < numOfThreads; i++) {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), netConfIid1);
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = netConfOnlyDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("can't perform rollback, the test failed");
        }
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test whether rollback operation wait for all the delete action to finish in mixed provider DTx
     * multi threads delete data in different IIDs of all the netConf nodes and dataStore nodes
     * right after all the threads finish, invoking the rollback
     * ensure the size of data in the IID is correct
     */
    @Test
    public void testConcurrentDeleteRollbackInMixedDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            internalDtxNetconfTestTx1.createObjForIdentifier((InstanceIdentifier<TestIid>)identifiers.get(i));
            internalDtxDataStoreTestTx1.createObjForIdentifier((InstanceIdentifier<TestIid>)identifiers.get(i));
        }
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), netConfIid1);
                    mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), dataStoreIid1);
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = mixedDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Can't perform rollback, test failed");
        }
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * test submit method for netConf only DTx, submit succeed case
     */
    @Test
    public void testSubmitInNetConfOnlyDTx() {
        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try
        {
            f.checkedGet();
        }catch (Exception e)
        {
            fail("Get the unexpected Exception");
        }
    }

    /**
     * test submit method for mixed provider DTx, submit succeed case
     */
    @Test
    public void testSubmitInMixedDTx() {
        CheckedFuture<Void, TransactionCommitFailedException> f = mixedDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
    }

    /**
     * test submit method for netConf only DTx, submit fail and rollback succeed case
     * put data in n0 of all the netConf nodes
     * submit failed and rollback succeed
     * get TransactionCommitFailedException from DTx
     * ensure the size of data in IIDs is correct
     */
    @Test
    public void testSubmitRollbackSucceedInNetConfOnlyDTx()  {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("netConfIid1 can't get rolledback", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("netConfIid2 can't get rolledback", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test submit method for mixed provider DTx, submit fail and rollback succeed case
     * put data in n0 of all the netConf nodes and dataStore nodes
     * submit failed and rollback succeed
     * get TransactionCommitFailedException from DTx
     * ensure the size of data in IIDs is correct
     */
    @Test
    public void testSubmitRollbackSucceedInMixedDTx()  {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxDataStoreTestTx2.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = mixedDTx.submit();
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("get the wrong kind of exception", e instanceof TransactionCommitFailedException);
        }
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * test submit method for netConf only DTx, submit fail and rollback fail case
     * put data in n0 of all the netConf nodes
     * submit failed and rollback fail
     * get TransactionCommitFailedException from DTx
     */
    @Test
    public void testSubmitRollbackFailInNetConfOnlyDTx()
    {
        internalDtxNetconfTestTx2.setSubmitException(true);
        internalDtxNetconfTestTx2.setDeleteExceptionByIid(n0,true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestIid1(), netConfIid2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException );
        }
    }

    /**
     * test submit method for mixed provider DTx, submit fail and rollback fail case
     * put data in n0 of all the netConf nodes and dataStore nodes
     * submit failed and rollback failed
     * get TransactionCommitFailedException from DTx
     */
    @Test
    public void testSubmitRollbackFailInMixedDTx()
    {
        internalDtxDataStoreTestTx2.setSubmitException(true);
        internalDtxDataStoreTestTx2.setDeleteExceptionByIid(n0, true);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception from the transaction");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f2 = mixedDTx.submit();
        try{
            f2.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("get the erong kind of exception", e instanceof TransactionCommitFailedException);
        }
    }
}
