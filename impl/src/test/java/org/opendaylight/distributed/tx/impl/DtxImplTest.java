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
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.DTxTransactionLockImpl;
import org.opendaylight.distributed.tx.impl.spi.DtxImpl;
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
    InstanceIdentifier<TestClassNode1> netConfNode1; //netconf nodeId1
    InstanceIdentifier<TestClassNode2> netConfNode2; //netconf nodeId2
    InstanceIdentifier<TestClassNode3> dataStoreNode1; //dataStore nodeId1
    InstanceIdentifier<TestClassNode4> dataStoreNode2; //dataStore nodeId2
    InstanceIdentifier<TestClassNode> n0, n1, n2, n3, n4, n5, n6, n7, n8, n9; //different data identifiers

    DTXTestTransaction internalDtxNetconfTestTx1; //transaction for node1 of the Netconf txProvider
    DTXTestTransaction internalDtxNetconfTestTx2; //transaction for node2  of the Netconf txProvider
    DTXTestTransaction internalDtxDataStoreTestTx1; //transaction for node1 of the Datastore txProvider
    DTXTestTransaction internalDtxDataStoreTestTx2; //transaction for node2 of the Datastore txProvider

    Set<InstanceIdentifier<?>> netconfNodes; //netconf nodeId set
    Set<InstanceIdentifier<?>> dataStoreNodes; //datastore nodeId set
    List<InstanceIdentifier<TestClassNode>> identifiers; //identifier list
    DtxImpl dtxImpl1; //dtx instance for netconf nodes
    DtxImpl dtxImpl2; //dtx instance for both netconf and datastore nodes
    ExecutorService threadPool; //use for multithread testing

    private class myNetconfTxProvider implements TxProvider{
        private boolean createTxException = false; //when it is true creating the transaction will throw an exception
        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            if(!createTxException)
            return nodeId == netConfNode1 ? internalDtxNetconfTestTx1 : internalDtxNetconfTestTx2;
            else
                throw new TxException.TxInitiatizationFailedException("create tx exception");
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

        public void setTxException(boolean createTxException)
        {
            this.createTxException = createTxException;
        }
    }

    private class myDataStoreTxProvider implements TxProvider{
        private boolean createTxException = false;
        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            if(!createTxException)
                return nodeId == dataStoreNode1? internalDtxDataStoreTestTx1 : internalDtxDataStoreTestTx2;
            else
                throw new TxException.TxInitiatizationFailedException("create tx exception");
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

        public void setTxException(boolean createTxException){this.createTxException = createTxException;}
    }

    private class TestClassNode implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestClassNode1 extends TestClassNode implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestClassNode2 extends TestClassNode implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestClassNode3 extends TestClassNode implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestClassNode4 extends TestClassNode implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    /**
     * test two different constructors of the DtxImpl
     * first one create a dtx instance only for netconf nodes
     * second one create a dtx instance for both netconf and datastore nodes
     */
    private void testInit(){
        netconfNodes = new HashSet<>();
        this.netConfNode1 = InstanceIdentifier.create(TestClassNode1.class);
        netconfNodes.add(netConfNode1);
        this.netConfNode2 = InstanceIdentifier.create(TestClassNode2.class);
        netconfNodes.add(netConfNode2);

        //initialize the identifier
        identifiers = Lists.newArrayList(n0, n1, n2, n3, n4, n5, n6, n7, n8, n9);
        for (InstanceIdentifier<TestClassNode> n: identifiers) {
            n = InstanceIdentifier.create(TestClassNode.class);
        }

        internalDtxNetconfTestTx1 = new DTXTestTransaction();
        internalDtxNetconfTestTx2 = new DTXTestTransaction();
        Map<DTXLogicalTXProviderType, TxProvider> netconfTxProviderMap = new HashMap<>();
        TxProvider netconfTxProvider = new myNetconfTxProvider();
        netconfTxProviderMap.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, netconfTxProvider);
        dtxImpl1 = new DtxImpl(netconfTxProvider, netconfNodes, new DTxTransactionLockImpl(netconfTxProviderMap));

        dataStoreNodes = new HashSet<>();
        this.dataStoreNode1 = InstanceIdentifier.create(TestClassNode3.class);
        dataStoreNodes.add(dataStoreNode1);
        this.dataStoreNode2 = InstanceIdentifier.create(TestClassNode4.class);
        dataStoreNodes.add(dataStoreNode2);
        internalDtxDataStoreTestTx1 = new DTXTestTransaction();
        internalDtxDataStoreTestTx2 = new DTXTestTransaction();
        Set<DTXLogicalTXProviderType> providerTypes = Sets.newHashSet(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,DTXLogicalTXProviderType.NETCONF_TX_PROVIDER);
        //create two maps, transaction provider map and nodes map
        Map<DTXLogicalTXProviderType, TxProvider> txProviderMap = Maps.toMap(providerTypes, new Function<DTXLogicalTXProviderType, TxProvider>() {
            @Nullable
            @Override
            public TxProvider apply(@Nullable DTXLogicalTXProviderType dtxLogicalTXProviderType) {
                return dtxLogicalTXProviderType == DTXLogicalTXProviderType.NETCONF_TX_PROVIDER ? new myNetconfTxProvider() : new myDataStoreTxProvider();
            }
        });

        Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap = Maps.toMap(providerTypes, new Function<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>>() {
            @Nullable
            @Override
            public Set<InstanceIdentifier<?>> apply(@Nullable DTXLogicalTXProviderType dtxLogicalTXProviderType) {
                return dtxLogicalTXProviderType == DTXLogicalTXProviderType.NETCONF_TX_PROVIDER ?
                        (Set)Sets.newHashSet(netConfNode1, netConfNode2) : (Set)Sets.newHashSet(dataStoreNode1, dataStoreNode2);
            }
        });

        dtxImpl2 = new DtxImpl(txProviderMap, nodesMap, new DTxTransactionLockImpl(txProviderMap));
    }

    @Before
    public void testConstructor() {
        testInit();
    }

    /**
     *simulate the case no data exist in all the nodes of the dtxImpl1, successfully put data into them
     */
    @Test
    public void testPutAndRollbackOnFailureInDtxImpl1() {
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs ; i++) {
            CheckedFuture<Void, DTxException> f1 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
            CheckedFuture<Void, DTxException> f2 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
            try
            {
                f1.checkedGet();
                f2.checkedGet();
            }catch (Exception e)
            {
                fail("get unexpected exception from the putAndRollbackOnFailure method");
            }
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * simulate the case no data exist in all the nodes in dtxImpl2, successfully put data in them
     */
    @Test
    public void testPutAndRollbackOnFailureInDtxImpl2(){
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for(int i = 0; i < numberOfObjs; i++)
        {
            CheckedFuture<Void, DTxException> f1 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
            CheckedFuture<Void, DTxException> f2 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
            CheckedFuture<Void, DTxException> f3 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);
            CheckedFuture<Void, DTxException> f4 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
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
        }

        int expectedDataSizeInNetconfTx1 = 1, expectedDataSizeInNetconfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expecteddataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expecteddataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case no data in both netConfNode1 and netConfNode2 of the dtxImpl1
     *then try to put data into the two nodes
     *after successfully put data in netConfNode1, netConfNode2 hit error the transaction fail but rollback succeed
     *at last no data in both netConfNode1 and netConfNode2
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackSucceedInDtxImpl1() {
        CheckedFuture<Void, DTxException> f1 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxNetconfTestTx2.setReadException(n0,true);
        CheckedFuture<Void, DTxException> f = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        try{
            f.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfNode1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfNode2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case no data in all the nodes of the dtxImpl2
     *then try to put data into all the nodes
     *after successfully put data into netConfNode1, netConfNode2 and dataStoreNode1, dataStoreNode2 hit error the transaction fail but rollback succeed
     *at last no data in all the nodes
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackSucceedInDtxImpl2() {
        CheckedFuture<Void, DTxException> f1 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        CheckedFuture<Void, DTxException> f3 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxDataStoreTestTx2.setReadException(n0,true);
        CheckedFuture<Void, DTxException> f4 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
        try{
            f4.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetconfTx1 = 0, expectedDataSizeInNetconfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in netConfNode1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfNode2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreNode1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreNode2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case data exist in netConfNode1, but no data in netConfNode2 of the dtxImpl1
     *try to put data in netConfNode2, but hit error
     *rollback succeed, at last the original data is back to the netConfNode1 and no data exist in netConfNode2
     */
    @Test
    public void testPutAndRollbackOnFailureWithObjExRollbackSucceedInDtxImpl1() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);

        internalDtxNetconfTestTx2.setReadException(n0,true);
        CheckedFuture<Void, DTxException> f = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        try{
            f.checkedGet();
            fail("Can't get the exception");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfNode1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfNode2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * simulate the case data exist in netConfNode1 and netConfNode2 of the dtxImpl2
     * try to put data into the dataStoreNode1 and dataStoreNode2
     * the operation on dataStoreNode2 hit error and rollback succeed
     */
    @Test
    public void testPutAndRollbackOnFailureWithObjExRollbackSucceedInDTxImpl2() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);
        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxDataStoreTestTx2.setPutException(n0, true);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
        try{
            f2.checkedGet();
            fail("can't get the exception the test failed");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception from the transaction", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetconfTx1 = 1, expectedDataSizeInNetconfTx2 = 1, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in netConfNode1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfNode2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreNode1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreNode2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case in which no data in netConfNode1 and netConfNode2 of dtxImpl1 at the beginning,
     *successfully put the data in netConfNode1
     *then try to put data into netConfNode2, but an exception occur and begin to rollback
     *when roll back in netConfNode2, submit exception occur and the rollback fail
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackFailInDtxImpl1() {
        internalDtxNetconfTestTx2.setPutException(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true); //submit fail the rollback will fail

        CheckedFuture<Void, DTxException> f1 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);

        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("Transaction1 get the unexpected exception");
        }

        try{
            f2.checkedGet();
            fail("Transaction2 can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected ReadFailException, the test failed", e instanceof DTxException);
        }
    }

    /**
     * simulate the case in which try to put data into dataStoreNode2, operation fail and rollback fail
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackFailInDtxImpl2() {
        internalDtxDataStoreTestTx2.setPutException(n0, true);

        CheckedFuture<Void, DTxException> f = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception from the transaction", e instanceof DTxException);
        }
    }

    /**
     * test the case multi threads try to put data into different identifiers in the same transaction
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureWithoutObjExInDtxImpl1(){
         int numOfThreads = (int)(Math.random()*9) + 1;
         threadPool = Executors.newFixedThreadPool(numOfThreads);
         for (int i = 0; i < numOfThreads; i++)
         {
             final int finalI = i;
             threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        CheckedFuture<Void, DTxException> f = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,identifiers.get(finalI), new TestClassNode(), netConfNode1);
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, dtxImpl1.getSizeofCacheByNodeId(netConfNode1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     *simulate the case no data exist in netConfNode1 and netConfNode2 at the beginning
     *we successfully merge data into netConfNode1 and netConfNode2
     */
    @Test
    public void testMergeAndRollbackOnFailureInDtxImpl1()  {
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs ; i++) {
            CheckedFuture<Void, DTxException> f1 = dtxImpl1.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
            CheckedFuture<Void, DTxException> f2 = dtxImpl1.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
            try
            {
                f1.checkedGet();
                f2.checkedGet();
            }catch (Exception e)
            {
                fail("get the unexpected exception");
            }
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * simulate the case no data in all the nodes of dtxImpl2
     * put data into all of the nodes
     */
    @Test
    public void testMergeAndRollbackOnFailureInDtxImpl2()  {
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs ; i++) {
            CheckedFuture<Void, DTxException> f1 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
            CheckedFuture<Void, DTxException> f2 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
            CheckedFuture<Void, DTxException> f3 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);
            CheckedFuture<Void, DTxException> f4 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);

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
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *test the case in which no data exist in netConfNode1 and netConfNode2
     *successfully merge data into netConfNode1
     *when try to merge data into netConfNode2 hit error
     *rollback succeed and at last no data exist in netConfNode1 and netConfNode2
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackSucceedInDtxImpl1() {
        CheckedFuture<Void, DTxException> f1 = dtxImpl1.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxNetconfTestTx2.setMergeException(n0,true);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        try{
            f2.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfNode1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfNode2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * test the case no data exist in all the nodes of the dtxImpl2
     * try to put data into them, when manipulate the data in dataStoreNode2, exception occur
     * rollback succeed
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackSucceedInDtxImpl2() {
        CheckedFuture<Void, DTxException> f1 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        CheckedFuture<Void, DTxException> f3 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);

        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxDataStoreTestTx2.setMergeException(n0, true);
        CheckedFuture<Void, DTxException> f4 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
        try
        {
            f4.checkedGet();
            fail("get the unexpected exception");
        }catch (Exception e)
        {
               Assert.assertTrue("can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *test the case in which data exist in netConfNode1, but no data in netConfNode2
     *when try to merge data into netConfNode2, it hit error
     *rollback succeed and at last original data is back to netConfNode1 and no data in netConfNode2
     */
    @Test
    public void testMergeAndRollbackOnFailureWithObjExRollbackSucceedInDtxImpl1() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setMergeException(n0,true);
        CheckedFuture<Void, DTxException> f = dtxImpl1.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        try{
            f.checkedGet();
            fail("Can't get the exception");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in netConfNode1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfNode2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * test the case in which data exist in netConfNode1, netConfNode2
     * try to put data into dataStoreNode1 and Node2, operation on Node2 fail
     * rollback succeed
     */
    @Test
    public void testMergeAndRollbackOnFailureWithObjExRollbackSucceedInDtxImpl2(){
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);
        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception");
        }
        internalDtxDataStoreTestTx2.setMergeException(n0, true);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
        try{
            f2.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case no data exist in both netConfNode1 and netConfNode2
     * successfully merge data into netConfNode1, but hit error when merge data in netConfNode2
     * rollback meet submit error in transaction2, rollback fail too
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackFailInDtxImpl1() {
        internalDtxNetconfTestTx2.setMergeException(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true); //submit fail the rollback will fail
        CheckedFuture<Void, DTxException> f1 = dtxImpl1.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("Transaction1 get the unexpected exception");
        }

        try{
            f2.checkedGet();
            fail("Transaction2 can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected ReadFailException", e instanceof DTxException);
        }
    }

    /**
     * simulate the case no data exist in  all the nodes
     * try to merge data into dataStoreNode2
     * error occur and rollback fail
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackFailInDtxImpl2() {
        internalDtxDataStoreTestTx2.setMergeException(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> f = dtxImpl2.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException);
        }
    }

    /**
     * simulate the case multithread try to merge data into different identifiers of the same transaction
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureInDtxImpl1() {
        int numOfThreads = (int)(Math.random()*9) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = dtxImpl1.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,identifiers.get(finalI), new TestClassNode(), netConfNode1);
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, dtxImpl1.getSizeofCacheByNodeId(netConfNode1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }

    /**
     *simulate the case in which data exist in netConfNode1
     *we successfully delete it
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExInDtxImpl1() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        CheckedFuture<Void, DTxException> deleteFuture = dtxImpl1.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfNode1);

        try
        {
            deleteFuture.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        int expectedDataSizeInTx1 = 0;
        Assert.assertEquals("Size in netConfNode1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
    }

    /**
     * simulate the case in which data exist in all the nodes
     * successfully delete all the data in the nodes
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExInDtxImpl2() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfNode2);
        CheckedFuture<Void, DTxException> f3 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreNode1);
        CheckedFuture<Void, DTxException> f4 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreNode2);

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
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case in which data exist in netConfNode1 and netConfNode2
     *successfully delete data in netConfNode1, but fail in netConfNode2
     *transaction fail but rollback succeed
     *at last original data is back to netConfNode1 and netConfNode2
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackSucceedInDtxImpl1() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setDeleteException(n0,true); //delete action will fail

        CheckedFuture<Void, DTxException> f1 = dtxImpl1.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfNode1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception ");
        }

        CheckedFuture<Void, DTxException> f2 = dtxImpl1.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfNode2);
        try{
            f2.checkedGet();
            fail("Can't get the exception");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception ", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("Size in netConfNode1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size in netConfNode2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * test the case in the dtxImpl2, data exist in all the nodes at the begginning
     * try to delete all the data, when manipulate the data in dataStoreNode2 delete exception occur
     * rollback succeed
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackSucceedInDtxImpl2(){
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfNode2);
        CheckedFuture<Void, DTxException> f3 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreNode1);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception");
        }

        internalDtxDataStoreTestTx2.setDeleteException(n0, true);
        CheckedFuture<Void, DTxException> f4 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreNode2);
        try{
            f4.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case in which data exist in netConfNode1 and netConfNode2
     *successfully delete data in netConfNode1 but fail to delete the data in netConfNode2, dtx begin to rollback
     *when rollback the netConfNode2 submit exception occur and the rollback fail
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackFailInDtxImpl1() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = dtxImpl1.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfNode1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxNetconfTestTx2.setDeleteException(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfNode2);
        try{
            f2.checkedGet();
            fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception the test failed", e instanceof DTxException);
        }

    }

    /**
     * simulate the case in dtxImpl2, try to delete the data in dataStoreNode2
     * delete exception occur, operation fail and rollback fail
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackFailInDtxImpl2(){
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.setDeleteException(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> f = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreNode2);
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can;t get the expected exception", e instanceof DTxException);
        }
    }

    /**
     * simulate the case multi threads try to delete the data in different identifiers of the same transaction
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureWithObjExInDtxImpl1(){
        int numOfThreads = (int)(Math.random()*9) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);

        //create the original data for each node
        for (int i = 0; i < numOfThreads; i++)
        {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
        }
        //multi threads try to delete the data
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = dtxImpl1.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,identifiers.get(finalI), netConfNode1);
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, dtxImpl1.getSizeofCacheByNodeId(netConfNode1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }

    /**
     * test best effort put method
     */
    @Test public void testPut()  {
        dtxImpl1.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
    }

    /**
     * test best effort merge method
     */
    @Test public void testMerge() {
        dtxImpl1.merge(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
    }

    /**
     * test best effort delete method
     */
    @Test public void testDelete() {
        dtxImpl1.delete(LogicalDatastoreType.OPERATIONAL, n0, netConfNode1);
    }

    /**
     *put data in both netConfNode1 and netConfNode2
     *rollback successfully no data will exist in netConfNode1 and netConfNode2
     */
    @Test
    public void testRollbackInDtxImpl1() {
        CheckedFuture<Void, DTxException> f1 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);

        try
        {
            f1.checkedGet();
            f2.checkedGet();
            Assert.assertEquals(1, internalDtxNetconfTestTx1.getTxDataSize(n0));
            Assert.assertEquals(1, internalDtxNetconfTestTx2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("get unexpected exception from the put");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> f = dtxImpl1.rollback();
        try{
            f.checkedGet();
            int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
            Assert.assertEquals("Size of n0'netconfNodes data in netConfNode1 is wrong after rollback", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
            Assert.assertEquals("Size of n0'netconfNodes data in netConfNode2 is wrong after rollback", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("rollback get unexpected exception");
        }
    }

    /**
     * this test case is used to test after the concurrent operations on the transaction, we can successfully rollback
     */
    @Test
    public void testConcurrentPutAndRollbackInDtxImpl1() {
        int numOfThreads = (int)(Math.random()*9) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,identifiers.get(finalI), new TestClassNode(), netConfNode1);
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
            //waiting for all the thread terminate
        }
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> f = dtxImpl1.rollback();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception");
        }
        expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }

    /**
     * put data into all the nodes in dtxImpl2
     * rollback successfully
     * no data exist in all the nodes
     */
    @Test
    public void testRollbackInDtxImpl2() {
        CheckedFuture<Void, DTxException> f1 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        CheckedFuture<Void, DTxException> f3 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);
        CheckedFuture<Void, DTxException> f4 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);

        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFut = dtxImpl2.rollback();
        try{
            rollbackFut.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *put data into netConfNode1 and netConfNode2
     *invoke rollback
     *when rollback netConfNode2 transaction, submit exception occur
     *rollback fail
     */
    @Test
    public void testRollbackFailInDtxImpl1()
    {
        CheckedFuture<Void, DTxException> f1 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        try{
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> f = dtxImpl1.rollback();
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
     * put data into all the nodes in dtxImpl2
     * invoke rollback
     * when rollback dataStore2 transaction, submit exception ocur
     * rollback fail
     */
    @Test
    public void testRollbackFailInDtxImpl2()
    {
        CheckedFuture<Void, DTxException> f1 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        CheckedFuture<Void, DTxException> f3 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);
        CheckedFuture<Void, DTxException> f4 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
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
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFut = dtxImpl2.rollback();
        try{
            rollbackFut.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     *submit succeed case in dtxImpl1
     */
    @Test
    public void testSubmitInDtxImpl1() {
        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl1.submit();
        try
        {
            f.checkedGet();
        }catch (Exception e)
        {
            fail("Get the unexpected Exception");
        }
    }

    /**
     * submit succeed case in dtxImpl2
     */
    @Test
    public void testSubmitInDtxImpl2() {
        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl2.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
    }

    /**
     *put data in netConfNode1 and netConfNode2, and submit
     *transaction2 submit fail
     *rollback succeed
     */
    @Test
    public void testSubmitWithoutObjExRollbackSucceedInDtxImpl1()  {
        CheckedFuture<Void, DTxException> f1 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            Assert.assertEquals(1, internalDtxNetconfTestTx1.getTxDataSize(n0));
            Assert.assertEquals(1, internalDtxNetconfTestTx2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl1.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("netConfNode1 can't get rolledback", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("netConfNode2 can't get rolledback", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * put data into all the nodes of dtxImpl2 and submit
     * dataStoreNode2 submit error rollback succeed
     */
    @Test
    public void testSubmitWithoutObjExRollbackSucceedInDtxImpl2()  {
        CheckedFuture<Void, DTxException> f1 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), netConfNode2);
        CheckedFuture<Void, DTxException> f3 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode1);
        CheckedFuture<Void, DTxException> f4 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
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
       CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl2.submit();
       try{
           f.checkedGet();
           fail("can't get the exception");
       }catch (Exception e)
       {
           Assert.assertTrue("get the wrong kind of exception", e instanceof TransactionCommitFailedException);
       }
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }
    /**
     *data exist in netConfNode1 and netConfNode2
     * successfully delete data in netConfNode1 and netConfNode2
     * invoke submit, netConfNode2 submit fail
     * rollback succeed
     */
    @Test
    public void testSubmitWithObjExRollbackSucceedInDtxImpl1() {
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> f1 = dtxImpl1.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfNode2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            Assert.assertEquals(0, internalDtxNetconfTestTx1.getTxDataSize(n0));
            Assert.assertEquals(0, internalDtxNetconfTestTx2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl1.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            //RollbackSucceed and Fail should throw two different exception??
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the TransactionCommitFailedException",cause.getClass().equals(DTxException.SubmitFailedException.class) );
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("netConfNode1 can't get rolledback", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("netConfNode2 can't get rolledback", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * simulate the case data exist in netConfNode1, netConfNode2 and dataStoreNode1, no data exist in dataStoreNode2
     * successfully delete all the data in netConfNode1, netCOnfNode2 and dataStoreNode1, and put data into dataStoreNode2
     * when try to submit the change, exception occur but rollback succeed
     */
    @Test
    public void testSubmitWithObjExRollbackSucceedInDtxImpl2(){
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> f1 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0,  netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfNode2);
        CheckedFuture<Void, DTxException> f3 = dtxImpl2.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreNode1);
        CheckedFuture<Void, DTxException> f4 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);

        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception from the transactions");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl2.submit();
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("get the unexpected exception", e instanceof TransactionCommitFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfNode1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfNode2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreNode2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *put data into netConfNode1 and netConfNode2
     *netConfNode2 submit fail and rollback but rollback hit delete exception and rollback fail
     */
    @Test
    public void testSubmitRollbackFailInDtxImpl1()
    {
        internalDtxNetconfTestTx2.setSubmitException(true);
        internalDtxNetconfTestTx2.setDeleteException(n0,true);

        CheckedFuture<Void, DTxException> f1 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestClassNode(), netConfNode1);
        CheckedFuture<Void, DTxException> f2 = dtxImpl1.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestClassNode(), netConfNode2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            Assert.assertEquals(1, internalDtxNetconfTestTx1.getTxDataSize(n0));
            Assert.assertEquals(1, internalDtxNetconfTestTx2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl1.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException );
        }
    }

    /**
     * put data into dataStoreNode2 of dtxImpl2, submit fail and rollback fail
     */
    @Test
    public void testSubmitRollbackFailInDtxImpl2()
    {
        internalDtxDataStoreTestTx2.setSubmitException(true);
        internalDtxDataStoreTestTx2.setDeleteException(n0, true);

        CheckedFuture<Void, DTxException> f1 = dtxImpl2.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), dataStoreNode2);
        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception from the transaction");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f2 = dtxImpl2.submit();
        try{
            f2.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("get the erong kind of exception", e instanceof TransactionCommitFailedException);
        }
    }

    /**
     *NetConfProvider can't create the new rollback transaction
     *submit will fail
     *distributedSubmitedFuture will be set a SubmitFailedException
     *no rollback
     */
    @Test
    public void testSubmitCreateRollbackTxFailInDtxImpl1() {
        myNetconfTxProvider txProvider = new myNetconfTxProvider();
        Map<DTXLogicalTXProviderType, TxProvider> netconfTxProviderMap = new HashMap<>();
        netconfTxProviderMap.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, txProvider);
        DtxImpl dtxImpl1 = new DtxImpl(txProvider, netconfNodes, new DTxTransactionLockImpl(netconfTxProviderMap));

        internalDtxNetconfTestTx1.setSubmitException(true);
        txProvider.setTxException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl1.submit();

        try{
            f.checkedGet();
            fail("Can't get the exception the test fail");
        } catch (Exception e) {
            Assert.assertTrue("get the wrong kind of exception", e instanceof TransactionCommitFailedException);
        }
    }

    /**
     * DataStoreProvider can't create the new rollback transaction
     * submit fail
     * no rollback
     */
    @Test
    public void testSubmitCrateRollbackTxFailInDtxImpl2() {
        final myNetconfTxProvider netconfTxProvider = new myNetconfTxProvider();
        final myDataStoreTxProvider dataStoreTxProvider = new myDataStoreTxProvider();

        Map<DTXLogicalTXProviderType, TxProvider> txProviderMap = Maps.toMap(Sets.newHashSet(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER),
                new Function<DTXLogicalTXProviderType, TxProvider>() {
                    @Nullable
                    @Override
                    public TxProvider apply(@Nullable DTXLogicalTXProviderType dtxLogicalTXProviderType) {
                        return dtxLogicalTXProviderType == DTXLogicalTXProviderType.NETCONF_TX_PROVIDER ? netconfTxProvider : dataStoreTxProvider;
                    }
                });
        Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap = Maps.toMap(Sets.newHashSet(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER),
                new Function<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>>() {
                    @Nullable
                    @Override
                    public Set<InstanceIdentifier<?>> apply(@Nullable DTXLogicalTXProviderType dtxLogicalTXProviderType) {
                        return dtxLogicalTXProviderType == DTXLogicalTXProviderType.NETCONF_TX_PROVIDER ? netconfNodes : dataStoreNodes  ;
                    }
                });
        DtxImpl dtxImpl2 = new DtxImpl(txProviderMap, nodesMap, new DTxTransactionLockImpl(txProviderMap));

        dataStoreTxProvider.setTxException(true);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl2.submit();

        try{
            f.checkedGet();
            fail("Can't get the exception the test fail");
        } catch (Exception e) {
            Assert.assertTrue("get the wrong kind of exception", e instanceof TransactionCommitFailedException);
        }
    }
}
