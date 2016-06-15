/*
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
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
        //Create two maps, transaction provider map and nodes map
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
     * Test putAndRollbackOnFailure() with successful put
     */
    @Test
    public void testPutAndRollbackOnFailureInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("Get exception");
        }

        Assert.assertEquals("Data size in tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size is tx2 wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test putAndRollbackOnFailure() with successful put
     */
    @Test
    public void testPutAndRollbackOnFailureInMixedDTx(){
        int expectedDataSizeInNetconfTx1 = 1, expectedDataSizeInNetconfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expecteddataSizeInDataStoreTx2 = 1;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expecteddataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test putAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testPutAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try {
            f1.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxNetconfTestTx2.setReadExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f.checkedGet();
            fail("Can't get exception");
        } catch (Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test putAndRollbackOnFailure() with failing put and successful rollback
     */
    @Test
    public void testPutAndRollbackOnFailurePutFailRollbackSucceedInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try {
            f1.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxNetconfTestTx2.setPutExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f.checkedGet();
            fail("Can't get exception");
        } catch (Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test putAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testPutAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        int expectedDataSizeInNetconfTx1 = 0, expectedDataSizeInNetconfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        try {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxDataStoreTestTx2.setReadExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f4.checkedGet();
            fail("Can't get exception");
        } catch (Exception e) {
            Assert.assertTrue("Can't get EditfailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test putAndRollbackOnFailure() with failing put and successful rollback
     */
    @Test
    public void testPutAndRollbackOnFailurePutFailRollbackSucceedInMixedDTx() {
        int expectedDataSizeInNetconfTx1 = 0, expectedDataSizeInNetconfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        try {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxDataStoreTestTx2.setPutExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f4.checkedGet();
            fail("Can't get exception");
        } catch (Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetconfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetconfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test putAndRollbackOnFailure() with failing rollback
     */
    @Test
    public void testPutAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.setPutExceptionByIid(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> putFuture = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);

        try{
            putFuture.checkedGet();
            fail("Can't get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can't get RollbackFailedException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test putAndRollbackOnFailure() with failing rollback
     */
    @Test
    public void testPutAndRollbackOnFailureRollbackFailInMixedDTx() {
        internalDtxDataStoreTestTx2.setPutExceptionByIid(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> putFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            putFuture.checkedGet();
            fail("can't get exception");
        }catch (Exception e)
        {
            Assert.assertTrue("can't get RollbackFailedException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test thread safety of putAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
                        fail("Get exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size in netConf tx1 is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of putAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
                        fail("Get exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size in netConf tx1 is wrong",numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of putAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
                    }catch (Exception e) {
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFaiedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong",numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of putAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size in netConf tx1 is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Cache size in dataStore tx1 is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test putAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully
     */
    @Test
    public void testConcurrentPutToSameNodeReadFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
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
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }
    /**
     * Test putAndRollbackOnFailure(). One of threads fail to put and DTx rollback successfully
     */
    @Test
    public void testConcurrentPutToSameNodePutFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
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
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test putAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully
     */
    @Test
    public void testConcurrentPutToSameNodeReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int) (Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int) (Math.random() * numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur) {
                        internalDtxDataStoreTestTx1.setReadExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> dataStoreWriteFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), dataStoreIid1);;
                    try {
                        dataStoreWriteFuture.checkedGet();
                    } catch (DTxException e) {
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        Assert.assertEquals("Data size in datStore tx1 is wrong",
                expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test putAndRollbackOnFailure(). One of threads fail to put and DTx rollback successfully
     */
    @Test
    public void testConcurrentPutToSameNodePutFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxDataStoreTestTx1.setPutExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> dataStoreWriteFuture = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), dataStoreIid1);
                    try{
                        dataStoreWriteFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType
                (DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        Assert.assertEquals("Data size is wrong",
                expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure() with successful merge
     */
    @Test
    public void testMergeAndRollbackOnFailureInNetConfOnlyDTx()  {
        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure() with successful merge
     */
    @Test
    public void testMergeAndRollbackOnFailureInMixedDTx()  {
        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);

        try {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try {
            f1.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }
        internalDtxNetconfTestTx2.setReadExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get exception");
        }
        catch (Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure() with failing merge and successful rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureMergeFailRollbackSucceedInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        try {
            f1.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }
        internalDtxNetconfTestTx2.setMergeExceptionByIid(n0,true);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get exception");
        } catch (Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);

        try {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxDataStoreTestTx2.setReadExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try {
            f4.checkedGet();
            fail("Get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure() with failing merge and successful rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureMergeFailRollbackSucceedInMixedDTx() {
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
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
            fail("Get exception");
        }

        internalDtxDataStoreTestTx2.setMergeExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try
        {
            f4.checkedGet();
            fail("Get exception");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure() with failing rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.setMergeExceptionByIid(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> mergeFuture = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            mergeFuture.checkedGet();
            fail("Can't get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can't get RollbackException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test mergeAndRollbackOnFailure() with failing rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureRollbackFailInMixedDTx() {
        internalDtxDataStoreTestTx2.setMergeExceptionByIid(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> f = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f.checkedGet();
            fail("Can't get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can't get RollbackfailedException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test thread safety of mergeAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), netConfIid1);
                    try{
                        f.checkedGet();
                    }catch (Exception e) {
                        fail("Get exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of mergeAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureInMixedDTx() {
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.OPERATIONAL,
                            (InstanceIdentifier<TestIid>)identifiers.get(finalI), new TestIid(), dataStoreIid1);
                    try{
                        dataStoreFuture.checkedGet();
                    }catch (Exception e) {
                        fail("Get exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong",
                numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of mergeAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random()*4) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
                    }catch (Exception e) {
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Size of data in the transaction is wrong",numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("size of identifier's data is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of mergeAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
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
                            fail("Get exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size in netConf tx1 is wrong",numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Cache size in dataStore tx1 is wrong",
                numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test mergeAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully
     */
    @Test
    public void testConcurrentMergeToSameNodeReadFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
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
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure(). One of threads fail to merge and DTx rollback successfully
     */
    @Test
    public void testConcurrentMergeToSameNodeMergeFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
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
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure(). One of threads fail to read and DTx successfully rollback
     */
    @Test
    public void testConcurrentMergeToSameNodeReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxDataStoreTestTx1.setReadExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> datastoreFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION,
                            n0, new TestIid1(), dataStoreIid1);
                    try{
                        datastoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test mergeAndRollbackOnFailure(). One of threads fail to merge and DTx rollback successfully
     */
    @Test
    public void testConcurrentMergeToSameNodeMergeFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxDataStoreTestTx1.setMergeExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> datastoreFuture = mixedDTx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, n0, new TestIid1(), dataStoreIid1);
                    try{
                        datastoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure() with succcessful delete
     */
    @Test
    public void testDeleteAndRollbackOnFailureInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 0,expectedDataSizeInTx2 = 0;
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);
        CheckedFuture<Void, DTxException> deleteFuture1 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        CheckedFuture<Void, DTxException> deleteFuture2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        try {
            deleteFuture1.checkedGet();
            deleteFuture2.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure() with successful delete
     */
    @Test
    public void testDeleteAndRollbackOnFailureInMixedDTx() {
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
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
        }catch (Exception e) {
            fail("Get exception");
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setReadExceptionByIid(n0,true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        try {
            f1.checkedGet();
        }catch (Exception e) {
            fail("Get exception ");
        }

        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get exception");
        }catch(Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }
        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure () with failing delete and successful rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureDeleteFailRollbackSucceedInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setDeleteExceptionByIid(n0,true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        try {
            f1.checkedGet();
        }catch (Exception e) {
            fail("Get exception ");
        }

        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        try{
            f2.checkedGet();
            fail("Can't get exception");
        }catch(Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure() with failing read and successflu rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid1);
        try {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxDataStoreTestTx2.setReadExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);
        try{
            f4.checkedGet();
        }catch (Exception e) {
            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure () failing delete and successful rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureDeleteFailRollbackSucceedInMixedDTx(){
        int expectedDataSizeInNetConfTx1 = 1, expectedDataSizeInNetConfTx2 = 1, expectedDataSizeInDataStoreTx1 = 1, expectedDataSizeInDataStoreTx2 = 1;
        internalDtxNetconfTestTx1.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f1 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid1);
        try {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxDataStoreTestTx2.setDeleteExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);
        try{
            f4.checkedGet();
        }catch (Exception e) {
            Assert.assertTrue("can't get the expected exception", e instanceof DTxException.EditFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure() with failing rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.createObjForIdentifier(n0);
        internalDtxNetconfTestTx2.setDeleteExceptionByIid(n0,true);
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException> deleteFuture = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, netConfIid2);
        try{
            deleteFuture.checkedGet();
            fail("Can't get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can't get TRollbackFailedException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test deleteAndRollbackOnFailure() with failing rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackFailInMixedDTx(){
        internalDtxDataStoreTestTx2.createObjForIdentifier(n0);
        internalDtxDataStoreTestTx2.setDeleteExceptionByIid(n0, true);
        internalDtxDataStoreTestTx2.setSubmitException(true);

        CheckedFuture<Void, DTxException> deleteFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, dataStoreIid2);
        try{
            deleteFuture.checkedGet();
            fail("Can't get exception");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get RollbackFailedException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test thread safety of deleteAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
        }
        for (int i = 0; i < numOfThreads; i++) {
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    CheckedFuture<Void, DTxException> f = netConfOnlyDTx.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL,identifiers.get(finalI), netConfIid1);
                    try{
                        f.checkedGet();
                    }catch (Exception e)
                    {
                        fail("Get exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of deleteAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
            internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
        }
        for (int i = 0; i < numOfThreads; i++) {
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
                    }catch (Exception e) {
                        fail("get the unexpected exception");
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size in netConf tx1 is wrong",numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Cache size in dataStore tx1 is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of deleteAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
                    }catch (Exception e) {
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache data is wrong",numOfThreads - 1,netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test thread safety of deleteAndRollbackOnFailure() with failing read and successful rollback
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random()*numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size in netConf tx1 is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Cache size in dataStore tx1 is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                dataStoreIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier,
                    internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInIdentifier,
                    internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test deleteAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully
     */
    @Test
    public void testConcurrentDeleteToSameNodeReadFailRollbackSucceedInNetConfDTx(){
        int numOfThreads = (int) (Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = 0;
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
                                fail("Get exception");
                            else
                                Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                        }
                    }
                });
            }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads - 1 , netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully
     */
    @Test
    public void testConcurrentDeleteToSameNodeReadFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxDataStoreTestTx1.setReadExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, dataStoreIid1);
                    try{
                        dataStoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure(). One of threads fail to delete and DTx rollback successfully
     */
    @Test
    public void testConcurrentDeleteToSameNodeDeleteFailRollbackSucceedInNetConfOnlyDTx(){
        int numOfThreads = (int) (Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
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
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test deleteAndRollbackOnFailure(). One of threads fail to delete and DTx rollback successfully
     */
    @Test
    public void testConcurrentDeleteToSameNodeDeleteFailRollbackSucceedInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        final int errorOccur = (int)(Math.random() * numOfThreads);
        internalDtxDataStoreTestTx1.createObjForIdentifier(n0);
        for (int i = 0; i < numOfThreads; i++){
            final int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    if (finalI == errorOccur){
                        internalDtxDataStoreTestTx1.setDeleteExceptionByIid(n0, true);
                    }
                    CheckedFuture<Void, DTxException> dataStoreFuture = mixedDTx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION,
                            n0, dataStoreIid1);
                    try{
                        dataStoreFuture.checkedGet();
                    }catch (DTxException e){
                        if (finalI != errorOccur)
                            fail("Get exception");
                        else
                            Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                    }
                }
            });
        }
        threadPool.shutdown();
        while(!threadPool.isTerminated()){
            Thread.yield();
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
    }

    /**
     * Test submit() with multi threads successfully putting data in nodes
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
            Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
            int expectedDataSizeInIdentifier = 1;
            for (int i = 0; i < numOfThreads; i++) {
                Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            }
        }
    }

    /**
     * Test submit() with multi threads successfully putting data in nodes
     */
    @Test
    public void testConcurrentPutAndSubmitInMixedDTx(){
        int numOfThreads = (int)(Math.random()*3) + 1;
        int expectedDataSizeInIdentifier = 1;
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
            Assert.assertEquals("Cache size in netConf tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, nodeId
            ));
        }
        for (InstanceIdentifier<?> nodeId : dataStoreNodes){
            Assert.assertEquals("Cache size in dataStore tx is wrong", numOfThreads,mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, nodeId
            ));
        }
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx1  is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test submit() with multi threads successfully merging data in nodes
     */
    @Test
    public void testConcurrentMergeAndSubmitInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 1;
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
            Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
            for (int i = 0; i < numOfThreads; i++) {
                Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            }
        }
    }

    /**
     * Test submit() with multi threads successfully merging data in nodes
     */
    @Test
    public void testConcurrentMergeAndSubmitInMixedDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 1;
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
            Assert.assertEquals("Cache size in netConf tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, nodeId
            ));
        }
        for (InstanceIdentifier<?> nodeId : dataStoreNodes){
            Assert.assertEquals("Cache size in dataStore tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, nodeId
            ));
        }
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test submit() with multi threads successfully deleting data in nodes
     */
    @Test
    public void testConcurrentDeleteAndSubmitInNetConfOnlyDTx(){
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
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
            Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(nodeIid));
        }
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test submit() with multi threads successfully deleting data in nodes
     */
    @Test
    public void testConcurrentDeleteAndSubmitInMixedDTx(){
        int numOfThreads = (int) (Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads * netconfNodes.size());
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
            Assert.assertEquals("Cache size in netConf tx is wrong", numOfThreads,mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, nodeId
            ));
        }
        for (InstanceIdentifier<?> nodeId : dataStoreNodes){
            Assert.assertEquals("Cache size in dataStore tx is wrong", numOfThreads,mixedDTx.getSizeofCacheByNodeIdAndType(
                    DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, nodeId
            ));
        }
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size indataStore tx2 is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test put()
     */
    @Test public void testPut()  {
        netConfOnlyDTx.put(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
    }

    /**
     * Test merge()
     */
    @Test public void testMerge() {
        netConfOnlyDTx.merge(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
    }

    /**
     * Test delete()
     */
    @Test public void testDelete() {
        netConfOnlyDTx.delete(LogicalDatastoreType.OPERATIONAL, n0, netConfIid1);
    }

    /**
     * Test rollback() with successful rollback
     */
    @Test
    public void testRollbackInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);

        try {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> f = netConfOnlyDTx.rollback();
        try{
            f.checkedGet();
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
            Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        }catch (Exception e) {
            fail("Get rollback exception");
        }
    }

    /**
     * Test rollback() with successful rollback
     */
    @Test
    public void testRollbackInMixedDTx() {
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);

        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFut = mixedDTx.rollback();
        try{
            rollbackFut.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }
        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test rollback() with failing rollback
     */
    @Test
    public void testRollbackFailInNetConfOnlyDTx()
    {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try{
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> f = netConfOnlyDTx.rollback();
        try {
            f.checkedGet();
            fail("Can't get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can;t get RollbackFailedException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test rollback() with failing rollback
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
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxDataStoreTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFut = mixedDTx.rollback();
        try{
            rollbackFut.checkedGet();
            fail("Can't get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can't get RollbackFailedException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test rollback(). DTx wait for all put actions finish before performing rollback
     */
    @Test
    public void testConcurrentPutRollbackInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = netConfOnlyDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Get rollback exception");
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test rollback(). DTx wait for all put actions finish before performing rollback
     */
    @Test
    public void testConcurrentPutRollbackInMixedDTx() {
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 0;
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
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = mixedDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Get rollback exception");
        }
        Assert.assertEquals("Cache size in netConf tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Cache size in dataStore tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size in netConf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test rollback(). DTx wait for all merge actions finish before performing rollback
     */
    @Test
    public void testConcurrentMergeRollbackInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 0;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
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
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = netConfOnlyDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Get rollback exception");
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test rollback(). DTx wait for all merge actions finish before performing rollback
     */
    @Test
    public void testConcurrentMergeRollbackInMixedDTx() {
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 0;
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
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = mixedDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Get rollback exception");
        }
        Assert.assertEquals("Cache size in netConf tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Cache size in dataStore tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
        DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        for (int i = 0; i < numOfThreads; i++)
        {
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test rollback(). DTx wait for all delete actions finish before performing rollback
     */
    @Test
    public void testConcurrentDeleteRollbackInNetConfOnlyDTx() {
        int numOfThreads = (int)(Math.random() * 3) + 1;
        int expectedDataSizeInIdentifier = 1;
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
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = netConfOnlyDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Get rollback exception");
        }
        Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test rollback(). DTx wait for all delete actions finish before performing rollback
     */
    @Test
    public void testConcurrentDeleteRollbackInMixedDTx() {
        int numOfThreads = (int)(Math.random() * 4) + 1;
        int expectedDataSizeInIdentifier = 1;
        threadPool = Executors.newFixedThreadPool(numOfThreads);
        for (int i = 0; i < numOfThreads; i++){
            internalDtxNetconfTestTx1.createObjForIdentifier((InstanceIdentifier<TestIid>)identifiers.get(i));
            internalDtxDataStoreTestTx1.createObjForIdentifier((InstanceIdentifier<TestIid>)identifiers.get(i));
        }
        for (int i = 0; i < numOfThreads; i++) {
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
        while (!threadPool.isTerminated()) {
            Thread.yield();
        }
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = mixedDTx.rollback();
        try{
            rollbackFuture.checkedGet();
        }catch (Exception e){
            fail("Get rollback exception");
        }
        Assert.assertEquals("Cache size in netConf tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfIid1));
        Assert.assertEquals("Cache size in dataStore tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreIid1));
        for (int i = 0; i < numOfThreads; i++) {
            Assert.assertEquals("Cache size in netConf tx1 is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
            Assert.assertEquals("Cache size in dataStore tx1 is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
        }
    }

    /**
     * Test submit() with successful submit
     */
    @Test
    public void testSubmitInNetConfOnlyDTx() {
        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try {
            f.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }
    }

    /**
     * Test submit() with successful submit
     */
    @Test
    public void testSubmitInMixedDTx() {
        CheckedFuture<Void, TransactionCommitFailedException> f = mixedDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }
    }

    /**
     * Test submit() with failing rollback and successful rollback
     */
    @Test
    public void testSubmitRollbackSucceedInNetConfOnlyDTx()  {
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        try {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e) {
            Assert.assertTrue("Can't get TransactionCommitFailedException",e instanceof TransactionCommitFailedException);
        }

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test submit() with failing submit and successful rollback
     */
    @Test
    public void testSubmitRollbackSucceedInMixedDTx()  {
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), netConfIid2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }
        internalDtxDataStoreTestTx2.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = mixedDTx.submit();
        try{
            f.checkedGet();
            fail("Can't get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can't get TransactionCommitFailedException", e instanceof TransactionCommitFailedException);
        }
        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(n0));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(n0));
    }

    /**
     * Test submit() with failing rollback
     */
    @Test
    public void testSubmitRollbackFailInNetConfOnlyDTx()
    {
        internalDtxNetconfTestTx2.setSubmitException(true);
        internalDtxNetconfTestTx2.setDeleteExceptionByIid(n0,true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestIid1(), netConfIid1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestIid1(), netConfIid2);
        try {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f = netConfOnlyDTx.submit();
        try{
            f.checkedGet();
        }catch (Exception e) {
            Assert.assertTrue("Can't get TransactionCommitFailedException",e instanceof TransactionCommitFailedException );
        }
    }

    /**
     * test submit() with failing rollback
     */
    @Test
    public void testSubmitRollbackFailInMixedDTx()
    {
        internalDtxDataStoreTestTx2.setSubmitException(true);
        internalDtxDataStoreTestTx2.setDeleteExceptionByIid(n0, true);
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, n0, new TestIid1(), dataStoreIid2);
        try{
            f1.checkedGet();
        }catch (Exception e) {
            fail("Get exception");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f2 = mixedDTx.submit();
        try{
            f2.checkedGet();
        }catch (Exception e) {
            Assert.assertTrue("Can't get TransactionCommitFailedException", e instanceof TransactionCommitFailedException);
        }
    }
}
