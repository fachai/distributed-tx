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

    DTXTestTransaction internalDtxNetconfTestTx1; //transaction for node1 of the Netconf txProvider
    DTXTestTransaction internalDtxNetconfTestTx2; //transaction for node2  of the Netconf txProvider
    DTXTestTransaction internalDtxDataStoreTestTx1; //transaction for node1 of the Datastore txProvider
    DTXTestTransaction internalDtxDataStoreTestTx2; //transaction for node2 of the Datastore txProvider

    Set<InstanceIdentifier<?>> netconfNodes; //netconf nodeId set
    Set<InstanceIdentifier<?>> dataStoreNodes; //datastore nodeId set
    Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap;
    List<InstanceIdentifier<? extends TestIid>> identifiers; //identifier list
    DtxImpl netConfOnlyDTx; //dtx instance for only netconf nodes
    DtxImpl mixedDTx; //dtx instance for both netconf and datastore nodes
    ExecutorService threadPool; //use for multithread testing

    private class myNetconfTxProvider implements TxProvider{
        private boolean createTxException = false; //when it is true creating the transaction will throw an exception
        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            if(!createTxException)
                return nodeId == netConfIid1 ? internalDtxNetconfTestTx1 : internalDtxNetconfTestTx2;
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
                return nodeId == dataStoreIid1 ? internalDtxDataStoreTestTx1 : internalDtxDataStoreTestTx2;
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

    /**
     * test two different constructors of the DtxImpl
     * first one create a dtx instance only for netconf nodes
     * second one create a dtx instance for both netconf and datastore nodes
     */
    private void testInit(){
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
        internalDtxNetconfTestTx1.addInstanceIdentifier(n0, n1, n2, n3);
        internalDtxNetconfTestTx2 = new DTXTestTransaction();
        internalDtxNetconfTestTx2.addInstanceIdentifier(n0, n1, n2, n3);
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
        internalDtxDataStoreTestTx1.addInstanceIdentifier(n0, n1, n2, n3);
        internalDtxDataStoreTestTx2 = new DTXTestTransaction();
        internalDtxDataStoreTestTx2.addInstanceIdentifier(n0, n1, n2, n3);

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

    @Before
    public void testConstructor() {
        testInit();
    }

    /**
     *simulate the case no data exist in all the nodes of the netConfOnlyDTx, successfully put data into them
     */
    @Test
    public void testPutAndRollbackOnFailureInNetConfOnlyDTx() {
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs ; i++) {
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
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * simulate the case no data exist in all the nodes in mixedDTx, successfully put data in them
     */
    @Test
    public void testPutAndRollbackOnFailureInMixedDTx(){
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for(int i = 0; i < numberOfObjs; i++)
        {
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
        }

        int expectedDataSizeInNetconfTx1 = 1, expectedDataSizeInNetconfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expecteddataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expecteddataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case no data in both netConfIid1 and netConfIid2 of the netConfOnlyDTx
     *then try to put data into the two Iids
     *after successfully put data in netConfIid1, netConfIid2 hit read error the transaction fail but rollback succeed
     *at last no data in both netConfIid1 and netConfIid2
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExReadFailRollbackSucceedInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxNetconfTestTx2.setReadException(n0,true);
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
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }
    /**
     *simulate the case no data in both netConfIid1 and netConfIid2 of the netConfOnlyDTx
     *then try to put data into the two Iids
     *after successfully put data in netConfIid1, netConfIid2 hit put error the transaction fail but rollback succeed
     *at last no data in both netConfIid1 and netConfIid2
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExPutFailRollbackSucceedInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxNetconfTestTx2.setPutException(n0,true);
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
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }


    /**
     *simulate the case no data in all the nodes of the mixedDTx
     *then try to put data into all the nodes
     *after successfully put data into netConfIid1, netConfIid2 and dataStoreIid1, dataStoreIid2 hit read error the transaction fail but rollback succeed
     *at last no data in all the nodes
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExReadFailRollbackSucceedInMixedDTx() {
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

        internalDtxDataStoreTestTx2.setReadException(n0,true);
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
        Assert.assertEquals("Size of the identifier n0's data in netConfIid1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfIid2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }
    /**
     *simulate the case no data in all the nodes of the mixedDTx
     *then try to put data into all the nodes
     *after successfully put data into netConfIid1, netConfIid2 and dataStoreIid1, dataStoreIid2 hit put error the transaction fail but rollback succeed
     *at last no data in all the nodes
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExPutFailRollbackSucceedInMixedDTx() {
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

        internalDtxDataStoreTestTx2.setPutException(n0,true);
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
        Assert.assertEquals("Size of the identifier n0's data in netConfIid1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfIid2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case data exist in netConfIid1, but no data in netConfIid2 of the netConfOnlyDTx
     *try to put data in netConfIid2, but hit read error
     *rollback succeed, at last the original data is back to the netConfIid1 and no data exist in netConfIid2
     */
    @Test
    public void testPutAndRollbackOnFailureWithObjExReadFailRollbackSucceedInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);

        internalDtxNetconfTestTx2.setReadException(n0,true);
        CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f.checkedGet();
            fail("Can't get the exception");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }
    /**
     *simulate the case data exist in netConfIid1, but no data in netConfIid2 of the netConfOnlyDTx
     *try to put data in netConfIid2, but hit put error
     *rollback succeed, at last the original data is back to the netConfIid1 and no data exist in netConfIid2
     */
    @Test
    public void testPutAndRollbackOnFailureWithObjExPutFailRollbackSucceedInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);

        internalDtxNetconfTestTx2.setPutException(n0,true);
        CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f.checkedGet();
            fail("Can't get the exception");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }


    /**
     * simulate the case data exist in netConfIid1 and netConfIid2 of the mixedDTx
     * try to put data into the dataStoreIid1 and dataStoreIid2
     * the operation on dataStoreIid2 hit read error and rollback succeed
     */
    @Test
    public void testPutAndRollbackOnFailureWithObjExReadFailRollbackSucceedInMixedDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxDataStoreTestTx2.setReadException(n0, true);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f2.checkedGet();
            fail("can't get the exception the test failed");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception from the transaction", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetconfTx1 = 1, expectedDataSizeInNetconfTx2 = 1, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in netConfIid1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfIid2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     * simulate the case data exist in netConfIid1 and netConfIid2 of the mixedDTx
     * try to put data into the dataStoreIid1 and dataStoreIid2
     * the operation on dataStoreIid2 hit put error and rollback succeed
     */
    @Test
    public void testPutAndRollbackOnFailureWithObjExPutFailRollbackSucceedInMixedDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxDataStoreTestTx2.setPutException(n0, true);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f2.checkedGet();
            fail("can't get the exception the test failed");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception from the transaction", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetconfTx1 = 1, expectedDataSizeInNetconfTx2 = 1, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in netConfIid1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfIid2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in dataStoreIid2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }


    /**
     *simulate the case in which no data in netConfIid1 and netConfIid2 of netConfOnlyDTx at the beginning,
     *successfully put the data in netConfIid1
     *then try to put data into netConfIid2, but an exception occur and begin to rollback
     *when roll back in netConfIid2, submit exception occur and the rollback fail
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.setPutException(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true); //submit fail the rollback will fail

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);

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
     * simulate the case in which try to put data into dataStoreIid2, operation fail and rollback fail
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackFailInMixedDTx() {
        internalDtxDataStoreTestTx2.setPutException(n0, true);

        CheckedFuture<Void, DTxException> f = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
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
    public void testConcurrentPutAndRollbackOnFailureWithoutObjExInNetConfOnlyDTx(){
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * test the case multi threads try to put data into different identifiers of netconfnode in NetConfOnlyDTx
     * one of the the transaction failed rollback succeed
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureWithoutObjExReadFailRollbackSucceedInNetConfOnlyDTx(){
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
                        internalDtxNetconfTestTx1.setReadException(identifiers.get(finalI), true);
                    }
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1 );
                    try{
                        f.checkedGet();
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * test the case multi threads try to put data into different identifiers of the two nodes in MixedDTx
     * one of the the transaction failed rollback succeed
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureWithoutObjExReadFailRollbackSucceedInMixedDTx(){
//        int numOfThreads = (int)(Math.random()*4) + 1;
        int numOfThreads = 4;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadException(identifiers.get(finalI), true);
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
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated())
        {
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the netconf transaction is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the datastore transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of netconf identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of datastore identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * test the case multi threads try to put data into different identifiers in the same transaction
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureWithoutObjExInMixedDTx(){
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case in which no data exist in n0 of netconf Node1
     * use multithreads to write data to n0
     * one of the operations put fail, rollback succeed and no data exist in n0
     * the test DTx is NetConfOnlyDTx(a dtx only contains netconf nodes)
     */
    @Test
    public void testConcurrentPutToSameNodeWithoutObjExPutFailRollbackSucceedInNetConfOnlyDTx(){
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
                        internalDtxNetconfTestTx1.setPutException(n0, true);
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

        }
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
    }
    /**
     * simulate the case in which no data exist in n0 of netconf Node1
     * use multithreads to write data to n0
     * one of the operations read fail, rollback succeed and no data exist in n0
     */
    @Test
    public void testConcurrentPutToSameNodeWithoutObjExReadExceptionRollbackSucceedInNetConfOnlyDTx(){
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
                        internalDtxNetconfTestTx1.setReadException(n0, true);
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

        }
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));

    }
    /**
     * simulate the case in which no data exist in n0 of netconf Node1 and datastore Node1
     * use multithreads to write data to n0 of both nodes
     * one of the operations read fail, rollback succeed and no data exist in n0 in bothNodes
     */
    @Test
    public void testConcurrentPutToSameNodeWithoutObjExReadExceptionRollbackSucceedInMixedDTx(){
            int numOfThreads = (int) (Math.random() * 3) + 1;
            threadPool = Executors.newFixedThreadPool(numOfThreads);

            final int errorOccur = (int) (Math.random() * numOfThreads);
            for (int i = 0; i < numOfThreads; i++) {
                final int finalI = i;
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        CheckedFuture<Void, DTxException> netConfWriteFuture;
                        CheckedFuture<Void, DTxException> dataStoreWriteFuture;
                        if (finalI == errorOccur) {
                            internalDtxNetconfTestTx1.setReadException(n0, true);
                        }
                        netConfWriteFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, new TestIid1(), netConfIid1);
                        dataStoreWriteFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, new TestIid1(), dataStoreIid1);
                        try {
                            netConfWriteFuture.checkedGet();
                        } catch (DTxException e) {
                            if (finalI != errorOccur)
                                fail("get the unexpected exception");
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

            }
            int expectedDataSizeInIdentifier = 0;
            Assert.assertEquals("The Size of data in netConfTx is wrong",
                    expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
            Assert.assertEquals("The Size of data in dataStoreTx is wrong",
                    expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(n0));
    }
    /**
     * simulate the case in which no data exist in n0 of netconf Node1 and datastore Node1
     * use multithreads to write data to n0 of both nodes
     * one of the operations put fail, rollback succeed and no data exist in n0 in bothNodes
     */
    @Test
    public void testConcurrentPutToSameNodeWithoutObjExPutExceptionRollbackSucceedInMixedDTx(){
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
                        internalDtxNetconfTestTx1.setPutException(n0, true);
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

        }
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in netConfTx is wrong",
                expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("The Size of data in dataStoreTx is wrong",
                expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(n0));
    }
    /**
     *simulate the case no data exist in netConfIid1 and netConfIid2 at the beginning
     *we successfully merge data into netConfIid1 and netConfIid2
     */
    @Test
    public void testMergeAndRollbackOnFailureInNetConfOnlyDTx()  {
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs ; i++) {
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
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * simulate the case no data in all the nodes of mixedDTx
     * put data into all of the nodes
     */
    @Test
    public void testMergeAndRollbackOnFailureInMixedDTx()  {
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs ; i++) {
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
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *test the case in which no data exist in netConfIid1 and netConfIid2
     *successfully merge data into netConfIid1
     *when try to merge data into netConfIid2 hit error
     *rollback succeed and at last no data exist in netConfIid1 and netConfIid2
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackSucceedInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxNetconfTestTx2.setMergeException(n0,true);
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
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0'netconfNodes data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * test the case no data exist in all the nodes of the mixedDTx
     * try to put data into them, when manipulate the data in dataStoreIid2, exception occur
     * rollback succeed
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackSucceedInMixedDTx() {
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

        internalDtxDataStoreTestTx2.setMergeException(n0, true);
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
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *test the case in which data exist in netConfIid1, but no data in netConfIid2
     *when try to merge data into netConfIid2, it hit error
     *rollback succeed and at last original data is back to netConfIid1 and no data in netConfIid2
     */
    @Test
    public void testMergeAndRollbackOnFailureWithObjExRollbackSucceedInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setMergeException(n0,true);
        CheckedFuture<Void, DTxException> f = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f.checkedGet();
            fail("Can't get the exception");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * test the case in which data exist in netConfIid1, netConfIid2
     * try to put data into dataStoreIid1 and Node2, operation on Node2 fail
     * rollback succeed
     */
    @Test
    public void testMergeAndRollbackOnFailureWithObjExRollbackSucceedInMixedDTx(){
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception");
        }
        internalDtxDataStoreTestTx2.setMergeException(n0, true);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f2.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case no data exist in both netConfIid1 and netConfIid2
     * successfully merge data into netConfIid1, but hit error when merge data in netConfIid2
     * rollback meet submit error in transaction2, rollback fail too
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.setMergeException(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true); //submit fail the rollback will fail
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
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
     * try to merge data into dataStoreIid2
     * error occur and rollback fail
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackFailInMixedDTx() {
        internalDtxDataStoreTestTx2.setMergeException(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> f = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f.checkedGet();
            fail("can't get the exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException);
        }
    }

    /**
     * simulate the case multithread try to merge data into different identifiers of the same netconf node
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureWithoutObjExInNetConfOnlyDTx() {
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case multithread try to merge data into different identifiers of two nodes in MixedDTx
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureWithoutObjExInMixedDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
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
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the transaction is wrong",
                numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case multithread try to merge data into different identifiers of the same netconf node
     * one of the operation hit error and rollback succeed
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureWithoutObjExReadErrorRollbackSucceedInNetConfOnlyDTx() {
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
                        internalDtxNetconfTestTx1.setReadException(identifiers.get(finalI), true);
                    }
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                    try{
                        f.checkedGet();
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case multithread try to merge data into different identifiers of two nodes in mixedDTx
     * one of the operation hit error and rollback succeed
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureWithoutObjExReadFailRollbackSucceedInMixedDTx() {
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
                        internalDtxNetconfTestTx1.setReadException(identifiers.get(finalI), true);
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the transaction is wrong",
                numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case no data exist in n0 of NetConfOnlyDTx netconf Node1
     * use multithreads to write data to n0
     * one of the operations read fail, rollback succeed
     */
    @Test
    public void testConcurrentMergeToSameNodeWithoutObjExReadExceptionRollbackSucceedInNetconfOnlyDTx(){
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
                        internalDtxNetconfTestTx1.setReadException(n0, true);
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

        }
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
    }
    /**
     * simulate the case no data exist in n0 of NetConfOnlyDTx netconf Node1 and datstore Node1
     * use multithreads to write data to n0
     * one of the operations read fail, rollback succeed
     */
    @Test
    public void testConcurrentMergeToSameNodeWithoutObjExReadExceptionRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadException(n0, true);
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

        }
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in netconf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("The Size of data in datastore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(n0));
    }
    /**
     * simulate the case no data exist in n0 of NetConfOnlyDTx netconf Node1
     * use multithreads to merge data to n0
     * one of the operations merge failed, rollback succeed
     */
    @Test
    public void testConcurrentMergeToSameNodeWithoutObjExMergeExceptionRollbackSucceedInNetConfOnlyDTx(){
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
                        internalDtxNetconfTestTx1.setMergeException(n0, true);
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

        }
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
    }
    /**
     * simulate the case no data exist in n0 of NetConfOnlyDTx netconf Node1 and datstore Node1
     * use multithreads to write data to n0
     * one of the operations merge fail, rollback succeed
     */
    @Test
    public void testConcurrentMergeToSameNodeWithoutObjExMergeExceptionRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setMergeException(n0, true);
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

        }
        int expectedDataSizeInIdentifier = 0;
        Assert.assertEquals("The Size of data in netconf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("The Size of data in datastore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(n0));
    }
    /**
     *simulate the case in which data exist in netConfIid1
     *we successfully delete it
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        CheckedFuture<Void, DTxException> deleteFuture = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);

        try
        {
            deleteFuture.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        int expectedDataSizeInTx1 = 0;
        Assert.assertEquals("Size in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
    }

    /**
     * simulate the case in which data exist in all the nodes
     * successfully delete all the data in the nodes
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExInMixedDTx() {
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
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case in which data exist in netConfIid1 and netConfIid2
     *successfully delete data in netConfIid1, but fail in netConfIid2
     *transaction fail but rollback succeed
     *at last original data is back to netConfIid1 and netConfIid2
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackSucceedInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setDeleteException(n0,true); //delete action will fail

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
        Assert.assertEquals("Size in netConfIid1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("Size in netConfIid2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * test the case in the mixedDTx, data exist in all the nodes at the beggining
     * try to delete all the data, when manipulate the data in dataStoreIid2 delete exception occur
     * rollback succeed
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackSucceedInMixedDTx(){
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

        internalDtxDataStoreTestTx2.setDeleteException(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);
        try{
            f4.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *simulate the case in which data exist in netConfIid1 and netConfIid2
     *successfully delete data in netConfIid1 but fail to delete the data in netConfIid2, dtx begin to rollback
     *when rollback the netConfIid2 submit exception occur and the rollback fail
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxNetconfTestTx2.setDeleteException(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception the test failed", e instanceof DTxException);
        }

    }

    /**
     * simulate the case in mixedDTx, try to delete the data in dataStoreIid2
     * delete exception occur, operation fail and rollback fail
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackFailInMixedDTx(){
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.setDeleteException(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> f = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);
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
    public void testConcurrentDeleteAndRollbackOnFailureWithObjExInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case multi threads try to delete the data in different identifiers of two nodes in MixedDTx
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureWithObjExInMixedDTx(){
        int numOfThreads = (int)(Math.random()*4) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);

        //create the original data for each node
        for (int i = 0; i < numOfThreads; i++)
        {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
            internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
        }
        //multi threads try to delete the data
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the netconf transaction is wrong",numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the datastore transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of netconf identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of datastore identifiers's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * test the case multi threads try to delete data in different identifiers of netconfnode in NetConfOnlyDTx
     * one of the the transaction failed rollback succeed
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureWithObjExReadFailRollbackSucceedInNetConfOnlyDTx(){
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
                    CheckedFuture<Void, DTxException> f = null;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadException(identifiers.get(finalI), true);
                        f = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                                (InstanceIdentifier<TestIid>)identifiers.get(finalI), netConfIid1 );
                    }
                    else{
                        f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                                (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1 );
                    }
                    try{
                        f.checkedGet();
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1,netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }

    /**
     * test the case multi threads try to delete data in different identifiers of the MixedDTx two nodes
     * one of the the transaction failed rollback succeed
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureWithoutObjExReadFailRollbackSucceedInMixedDTx(){
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
                    CheckedFuture<Void, DTxException> netConfFuture;
                    CheckedFuture<Void, DTxException> dataStoreFuture;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadException(identifiers.get(finalI), true);
                        netConfFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                                LogicalDatastoreType.CONFIGURATION,identifiers.get(finalI), netConfIid1);
                        dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                LogicalDatastoreType.CONFIGURATION, identifiers.get(finalI), dataStoreIid1);
                    }else {
                        netConfFuture = mixedDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                                (InstanceIdentifier<TestIid>) identifiers.get(finalI), new TestIid(), netConfIid1);
                        dataStoreFuture = mixedDTx.putAndRollbackOnFailure(
                                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                LogicalDatastoreType.OPERATIONAL,
                                (InstanceIdentifier<TestIid>) identifiers.get(finalI), new TestIid(), dataStoreIid1);
                    }
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
            //waiting for all the thread terminate
        }
        Assert.assertEquals("Size of data in the netconf transaction is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Size of data in the datastore transaction is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of netconf identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of datastore identifier's data is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case data exist in n0 of netconf Node1
     * use multithreads to put data to n0 and delete the data
     * the delete operation read failed, rollback succeed, original data back to n0
     */
    @Test
    public void testConcurrentDeleteToSameNodeWithoutObjExReadExceptionRollbackSucceedInNetConfDTx(){
            int numOfThreads = (int) (Math.random() * 3) + 1;
            threadPool = Executors.newFixedThreadPool(numOfThreads);
            final int errorOccur = (int) (Math.random() * numOfThreads);
            for (int i = 0; i < numOfThreads; i++) {
                final int finalI = i;
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        CheckedFuture<Void, DTxException> writeFuture;
                        if (finalI == errorOccur) {
                            internalDtxNetconfTestTx1.setReadException(n0, true);
                            writeFuture = netConfOnlyDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                    n0, netConfIid1);
                        } else {
                            writeFuture = netConfOnlyDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                    n0, new TestIid1(), netConfIid1);
                        }
                        try {
                            writeFuture.checkedGet();
                        } catch (DTxException e) {
                            if (finalI != errorOccur)
                                fail("get the unexpected exception");
                        }
                    }
                });
            }
            threadPool.shutdown();
            while (!threadPool.isTerminated()) {

            }
            int expectedDataSizeInIdentifier = 0;
            Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
    }
    /**
     * simulate the case data exist in n0 of netconf Node1 and datastore node1
     * use multithreads to put data to n0 and delete the data
     * the delete operation read failed, rollback succeed, original data back to n0 of both nodes in mixedDTx
     */
    @Test
    public void testConcurrentDeleteToSameNodeWithObjExReadExceptionRollbackSucceedInMixedDTx(){
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
                    CheckedFuture<Void, DTxException> netConfFuture;
                    CheckedFuture<Void, DTxException> dataStoreFuture;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setReadException(n0, true);
                        netConfFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, netConfIid1);
                        dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, dataStoreIid1);
                    }else {
                        netConfFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, new TestIid1(), netConfIid1);
                        dataStoreFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, new TestIid1(), dataStoreIid1);
                    }

                    try{
                        netConfFuture.checkedGet();
                        dataStoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){

        }
        int expectedDataSizeInIdentifier = 1;
        Assert.assertEquals("The Size of data in netconf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("The Size of data in datastore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(n0));
    }
    /**
     * simulate the case no data exist in n0 of netconf Node1
     * use multithreads to put data to n0 and delete the data
     * the delete operation delete failed, rollback succeed, no data in the node at last
     */
    @Test
    public void testConcurrentDeleteToSameNodeWithoutObjExDeleteExceptionRollbackSucceedInNetConfOnlyDTx(){
            int numOfThreads = (int) (Math.random() * 3) + 1;
            threadPool = Executors.newFixedThreadPool(numOfThreads);
            final int errorOccur = (int) (Math.random() * numOfThreads);
            internalDtxNetconfTestTx1.setDeleteException(n0, true);
            for (int i = 0; i < numOfThreads; i++) {
                final int finalI = i;
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        CheckedFuture<Void, DTxException> writeFuture;
                        if (finalI == errorOccur) {
                            writeFuture = netConfOnlyDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                    n0, netConfIid1);
                        } else {
                            writeFuture = netConfOnlyDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                    n0, new TestIid1(), netConfIid1);
                        }
                        try {
                            writeFuture.checkedGet();
                        } catch (DTxException e) {
                            if (finalI != errorOccur)
                                fail("get the unexpected exception");
                        }
                    }
                });
            }
            threadPool.shutdown();
            while (!threadPool.isTerminated()) {

            }
            int expectedDataSizeInIdentifier = 0;
            Assert.assertEquals("The Size of data in tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
    }
    /**
     * simulate the case data exist in n0 of netconf Node1 and datastore Node1
     * use multithreads to put data to n0 and delete the data in both nodes
     * the delete operation delete failed, rollback succeed, original data back to n0 in both nodes
     */
    @Test
    public void testConcurrentDeleteToSameNodeWithObjExDeleteExceptionRollbackSucceedInMixedDTx(){
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
                    CheckedFuture<Void, DTxException> netConfFuture;
                    CheckedFuture<Void, DTxException> dataStoreFuture;
                    if (finalI == errorOccur){
                        internalDtxNetconfTestTx1.setDeleteException(n0, true);
                        netConfFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, netConfIid1);
                        dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, dataStoreIid1);
                    }else {
                        netConfFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, new TestIid1(), netConfIid1);
                        dataStoreFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                                n0, new TestIid1(), dataStoreIid1);
                    }

                    try{
                        netConfFuture.checkedGet();
                        dataStoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){

        }
        int expectedDataSizeInIdentifier = 1;
        Assert.assertEquals("The Size of data in netconf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("The Size of data in datastore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(n0));
    }

    /**
     * simulate the case multi threads try to put the data in several identifiers of netconfNode1 and netconfNode2
     * this case is used to check whether all the transacions have been finished before submit executes
     */
    @Test
    public void testConcurrentPutWithoutObjExAndSubmitInNetConfOnlyDTx(){
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

        }
        netConfOnlyDTx.submit();
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            Assert.assertEquals("Size of data in the transaction is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
            int expectedDataSizeInIdentifier = 1;
            for (int i = 0; i < numOfThreads; i++) {
                Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            }
        }
    }
    /**
     * simulate the case multi threads try to put the data in several identifiers of netconfNode1, netconfNode2, dataStoreNode1 and datastoreNode2
     * this case is used to check whether all the transacions have been finished before submit executes
     */
    @Test
    public void testConcurrentPutWithoutObjExAndSubmitInMixedDTx(){
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

        }
        mixedDTx.submit();
        int expectedDataSizeInIdentifier = 1;

        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("size of netconfNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of netconfNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case multi threads try to merge the data in several identifiers of netconfNode1 and netconfNode2
     * this case is used to check whether all the transacions have been finished before submit executes
     */
    @Test
    public void testConcurrentMergeWithoutObjExAndSubmitInNetConfOnlyDTx(){
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

        }
        netConfOnlyDTx.submit();
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            Assert.assertEquals("Size of data in the transaction is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
            int expectedDataSizeInIdentifier = 1;
            for (int i = 0; i < numOfThreads; i++) {
                Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            }
        }
    }
    /**
     * simulate the case multi threads try to merge the data in several identifiers of netconfNode1, netconfNode2, dataStoreNode1 and datastoreNode2
     * this case is used to check whether all the transacions have been finished before submit executes
     */
    @Test
    public void testConcurrentMergeWithoutObjExAndSubmitInMixedDTx(){
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

        }
        mixedDTx.submit();
        int expectedDataSizeInIdentifier = 1;

        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("size of netconfNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of netconfNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of datastoreNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case multi threads try to delete the data in several identifiers of netconfNode1 and netconfNode2
     * this case is used to check whether all the transacions have been finished before submit executes
     */
    @Test
    public void testConcurrentDeleteWithObjExAndSubmitInNetConfOnlyDTx(){
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

        }
        netConfOnlyDTx.submit();
        for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
            Assert.assertEquals("Size of data in the transaction is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
        }
        int expectedDataSizeInIdentifier = 0;
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * simulate the case multi threads try to delete the data in several identifiers of netconfNode1, netconfNode2, dataStoreNode1 and datastoreNode2
     * this case is used to check whether all the transacions have been finished before submit executes
     */
    @Test
    public void testConcurrentDeleteWithObjExAndSubmitInMixedDTx(){
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

            }
            mixedDTx.submit();
            int expectedDataSizeInIdentifier = 0;

            for (int i = 0; i < numOfThreads; i++) {
                Assert.assertEquals("size of netconfNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
                Assert.assertEquals("size of netconfNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSize(identifiers.get(i)));
                Assert.assertEquals("size of datastoreNode1 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
                Assert.assertEquals("size of datastoreNode2 identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSize(identifiers.get(i)));
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
     *put data in both netConfIid1 and netConfIid2
     *rollback successfully no data will exist in netConfIid1 and netConfIid2
     */
    @Test
    public void testRollbackInNetConfOnlyDTx() {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);

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

        CheckedFuture<Void, DTxException.RollbackFailedException> f = netConfOnlyDTx.rollback();
        try{
            f.checkedGet();
            int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
            Assert.assertEquals("Size of n0'netconfNodes data in netConfIid1 is wrong after rollback", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
            Assert.assertEquals("Size of n0'netconfNodes data in netConfIid2 is wrong after rollback", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("rollback get unexpected exception");
        }
    }

    /**
     * multithreads put data in the netconf device, right after the operations invoke rollback immediately
     * check whether the rollback action wait for all the action finish in netconf only DTx
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
            //waiting for all the thread terminate
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
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * multithreads put data in the netconf device and datastore, right after the operations invoke rollback immediately
     * check whether the rollback action wait for all the action finish in mixed provider DTx
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
            //waiting for all the thread terminate
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
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * multithreads merge data in the netconf device, right after the operations invoke rollback immediately
     * check whether the rollback action wait for all the action finish
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
            //waiting for all the thread terminate
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
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * multithreads merge data in the netconf device and datastore, right after the operations invoke rollback immediately
     * check whether the rollback action wait for all the action finish in mixed provider DTx
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
            //waiting for all the thread terminate
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
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * multithreads delete data in the netconf device, right after the operations invoke rollback immediately
     * check whether the rollback action wait for all the action finish
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
            //waiting for all the thread terminate
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
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * multithreads delete data in the netconf device and datastore, right after the operations invoke rollback immediately
     * check whether the rollback action wait for all the action finish in mixed provider DTx
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
            //waiting for all the thread terminate
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
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSize(identifiers.get(i)));
        }
    }
    /**
     * this test case is used to test after the concurrent operations on the transaction, we can successfully rollback
     */
    @Test
    public void testConcurrentPutAndRollbackInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random()*3) + 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++)
        {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
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
            //waiting for all the thread terminate
        }
        int expectedDataSizeInIdentifier = 1;
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSize(identifiers.get(i)));
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> f = netConfOnlyDTx.rollback();
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
     * put data into all the nodes in mixedDTx
     * rollback successfully
     * no data exist in all the nodes
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
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *put data into netConfIid1 and netConfIid2
     *invoke rollback
     *when rollback netConfIid2 transaction, submit exception occur
     *rollback fail
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
     * put data into all the nodes in mixedDTx
     * invoke rollback
     * when rollback dataStore2 transaction, submit exception occur
     * rollback fail
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
     *submit succeed case in netConfOnlyDTx
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
     * submit succeed case in mixedDTx
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
     *put data in netConfIid1 and netConfIid2, and submit
     *transaction2 submit fail
     *rollback succeed
     */
    @Test
    public void testSubmitWithoutObjExRollbackSucceedInNetConfOnlyDTx()  {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
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
        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("netConfIid1 can't get rolledback", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("netConfIid2 can't get rolledback", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * put data into all the nodes of mixedDTx and submit
     * dataStoreIid2 submit error rollback succeed
     */
    @Test
    public void testSubmitWithoutObjExRollbackSucceedInMixedDTx()  {
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
        Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
        Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }
    /**
     *data exist in netConfIid1 and netConfIid2
     * successfully delete data in netConfIid1 and netConfIid2
     * invoke submit, netConfIid2 submit fail
     * rollback succeed
     */
    @Test
    public void testSubmitWithObjExRollbackSucceedInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
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

        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            //RollbackSucceed and Fail should throw two different exception??
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the TransactionCommitFailedException",cause.getClass().equals(DTxException.SubmitFailedException.class) );
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("netConfIid1 can't get rolledback", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
        Assert.assertEquals("netConfIid2 can't get rolledback", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
    }

    /**
     * simulate the case data exist in netConfIid1, netConfIid2 and dataStoreIid1, no data exist in dataStoreIid2
     * successfully delete all the data in netConfIid1, netCOnfNode2 and dataStoreIid1, and put data into dataStoreIid2
     * when try to submit the change, exception occur but rollback succeed
     */
    @Test
    public void testSubmitWithObjExRollbackSucceedInMixedDTx(){
            internalDtxNetconfTestTx1.createObjForIdentifier(n0);
            internalDtxNetconfTestTx2.createObjForIdentifier(n0);
            internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
            internalDtxDataStoreTestTx2.setSubmitException(true);

            CheckedFuture<Void, DTxException> f1 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
            CheckedFuture<Void, DTxException> f2 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
            CheckedFuture<Void, DTxException> f3 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid1);
            CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);

            try {
                f1.checkedGet();
                f2.checkedGet();
                f3.checkedGet();
                f4.checkedGet();
            } catch (Exception e) {
                fail("get unexpected exception from the transactions");
            }

            CheckedFuture<Void, TransactionCommitFailedException> f = mixedDTx.submit();
            try {
                f.checkedGet();
                fail("can't get the exception");
            } catch (Exception e) {
                Assert.assertTrue("get the unexpected exception", e instanceof TransactionCommitFailedException);
            }

            int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 0;
            Assert.assertEquals("size of netConfIid1 data is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSize(n0));
            Assert.assertEquals("size of netConfIid2 data is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSize(n0));
            Assert.assertEquals("size of dataStoreIid1 data is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSize(n0));
            Assert.assertEquals("size of dataStoreIid2 data is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSize(n0));
    }

    /**
     *put data into netConfIid1 and netConfIid2
     *netConfIid2 submit fail and rollback but rollback hit delete exception and rollback fail
     */
    @Test
    public void testSubmitRollbackFailInNetConfOnlyDTx()
    {
        internalDtxNetconfTestTx2.setSubmitException(true);
        internalDtxNetconfTestTx2.setDeleteException(n0,true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestIid1(), netConfIid2);
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

        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException );
        }
    }

    /**
     * put data into dataStoreIid2 of mixedDTx, submit fail and rollback fail
     */
    @Test
    public void testSubmitRollbackFailInMixedDTx()
    {
        internalDtxDataStoreTestTx2.setSubmitException(true);
        internalDtxDataStoreTestTx2.setDeleteException(n0, true);

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

    /**
     *NetConfProvider can't create the new rollback transaction
     *submit will fail
     *distributedSubmitedFuture will be set a SubmitFailedException
     *no rollback
     */
    @Test
    public void testSubmitCreateRollbackTxFailInNetConfOnlyDTx() {
        myNetconfTxProvider txProvider = new myNetconfTxProvider();
        Map<DTXLogicalTXProviderType, TxProvider> netconfTxProviderMap = new HashMap<>();
        netconfTxProviderMap.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, txProvider);
        DtxImpl NetConfOnlyDTx = new DtxImpl(txProvider, netconfNodes, new DTxTransactionLockImpl(netconfTxProviderMap));

        internalDtxNetconfTestTx1.setSubmitException(true);
        txProvider.setTxException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = NetConfOnlyDTx.submit();

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
    public void testSubmitCrateRollbackTxFailInMixedDTx() {
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
        DtxImpl MixedDTx = new DtxImpl(txProviderMap, nodesMap, new DTxTransactionLockImpl(txProviderMap));

        dataStoreTxProvider.setTxException(true);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, TransactionCommitFailedException> f = MixedDTx.submit();

        try{
            f.checkedGet();
            fail("Can't get the exception the test fail");
        } catch (Exception e) {
            Assert.assertTrue("get the wrong kind of exception", e instanceof TransactionCommitFailedException);
        }
    }
}
