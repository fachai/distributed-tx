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
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.spi.TxException;
import org.opendaylight.distributed.tx.spi.TxProvider;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.Assert.fail;

public class DtxImplTest{
    InstanceIdentifier<NetconfNode1> netConfNodeId1;
    InstanceIdentifier<NetConfNode2> netConfNodeId2;
    InstanceIdentifier<DataStoreNode1> dataStoreNodeId1;
    InstanceIdentifier<DataStoreNode2> dataStoreNodeId2;

    InstanceIdentifier<TestIid1> iid1;
    InstanceIdentifier<TestIid2> iid2;
    InstanceIdentifier<TestIid3> iid3;
    InstanceIdentifier<TestIid4> iid4;

    DTXTestTransaction internalDtxNetconfTestTx1;
    DTXTestTransaction internalDtxNetconfTestTx2;
    DTXTestTransaction internalDtxDataStoreTestTx1;
    DTXTestTransaction internalDtxDataStoreTestTx2;

    Set<InstanceIdentifier<?>> netconfNodes;
    Set<InstanceIdentifier<?>> dataStoreNodes;
    Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap;
    List<InstanceIdentifier<? extends TestIid>> identifiers;
    TestClass testClass;
    DtxImpl netConfOnlyDTx;
    DtxImpl mixedDTx;
    ExecutorService threadPool;

    private class myNetconfTxProvider implements TxProvider{
        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            return nodeId == netConfNodeId1 ? internalDtxNetconfTestTx1 : internalDtxNetconfTestTx2;
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
            return nodeId == dataStoreNodeId1 ? internalDtxDataStoreTestTx1 : internalDtxDataStoreTestTx2;
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

    private enum ProviderType{
        NETCONF, MIX
    }

    private enum OperationType{
        READ, PUT, MERGE, DELETE
    }

    private class TestClass{
        void testWriteAndRollbackOnFailure(ProviderType providerType, OperationType operationType){
            int expectedDataSizeInIdentifier = 1;
            if (operationType == OperationType.DELETE){
                expectedDataSizeInIdentifier = 0;
                internalDtxNetconfTestTx1.createObjForIdentifier(iid1);
                internalDtxNetconfTestTx2.createObjForIdentifier(iid1);
                internalDtxDataStoreTestTx1.createObjForIdentifier(iid1);
                internalDtxDataStoreTestTx2.createObjForIdentifier(iid1);
            }
            if (providerType == ProviderType.NETCONF){
                CheckedFuture<Void, DTxException> netConfFuture1 = writeData(
                        netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, operationType, iid1, netConfNodeId1, new TestIid1());
                CheckedFuture<Void, DTxException> netConfFuture2 = writeData(
                        netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, operationType, iid1, netConfNodeId2, new TestIid1());
                try {
                    netConfFuture1.checkedGet();
                    netConfFuture2.checkedGet();
                }catch (Exception e)
                {
                    fail("Caught unexpected exception");
                }
                Assert.assertEquals("Data size in tx1 is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
                Assert.assertEquals("Data size in tx2 is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(iid1));
            }else {
                CheckedFuture<Void, DTxException> netConfFuture1 = writeData(
                        mixedDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, operationType, iid1, netConfNodeId1, new TestIid1());
                CheckedFuture<Void, DTxException> netConfFuture2 = writeData(
                        mixedDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, operationType, iid1, netConfNodeId2, new TestIid1());
                CheckedFuture<Void, DTxException> dataStoreFuture1 = writeData(
                        mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, operationType, iid1, dataStoreNodeId1, new TestIid1());
                CheckedFuture<Void, DTxException> dataStoreFuture2 = writeData(
                        mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, operationType, iid1, dataStoreNodeId2, new TestIid1());
                try {
                    netConfFuture1.checkedGet();
                    netConfFuture2.checkedGet();
                    dataStoreFuture1.checkedGet();
                    dataStoreFuture2.checkedGet();
                }catch (Exception e)
                {
                    fail("Caught unexpected exception");
                }
                Assert.assertEquals("Data size in netConf tx1 is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
                Assert.assertEquals("Data size in netConf tx2 is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(iid1));
                Assert.assertEquals("Data size in dataStore tx1 is wrong",
                        expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(iid1));
                Assert.assertEquals("Data size in dataStore tx2 is wrong",
                        expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSizeByIid(iid1));
            }
        }

        void testWriteAndRollbackOnFailureRollbackSucceed(ProviderType providerType, OperationType operationType,
                                                          OperationType errorType){
            int expectedDataSizeInIdentifier = 0;
            if (operationType == OperationType.DELETE){
                expectedDataSizeInIdentifier = 1;
                internalDtxNetconfTestTx1.createObjForIdentifier(iid1);
                internalDtxNetconfTestTx2.createObjForIdentifier(iid1);
                internalDtxDataStoreTestTx1.createObjForIdentifier(iid1);
                internalDtxDataStoreTestTx2.createObjForIdentifier(iid1);
            }
            if (providerType == ProviderType.NETCONF){
                CheckedFuture<Void, DTxException> netConfFuture1 =  writeData(netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        operationType, iid1, netConfNodeId1, new TestIid1());
                try {
                    netConfFuture1.checkedGet();
                }catch (Exception e) {
                    fail("Caught unexpected exception");
                }

                setException(internalDtxNetconfTestTx2, iid1, errorType);
                CheckedFuture<Void, DTxException> writeFuture= writeData(netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        operationType, iid1, netConfNodeId2, new TestIid1());
                try {
                    writeFuture.checkedGet();
                }catch (Exception e){
                    Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                }
                Assert.assertEquals("Data size in netConf tx1 is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
                Assert.assertEquals("Data size in netConf tx2 is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(iid1));
            }else{
                CheckedFuture<Void, DTxException> netConfFuture1 = writeData(mixedDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        operationType, iid1, netConfNodeId1,new TestIid1());
                CheckedFuture<Void, DTxException> netConfFuture2 = writeData(mixedDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        operationType, iid1, netConfNodeId2,new TestIid1());
                CheckedFuture<Void, DTxException> dataStoreFuture1 = writeData(mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                        operationType, iid1, dataStoreNodeId1,new TestIid1());
                try{
                    netConfFuture1.checkedGet();
                    netConfFuture2.checkedGet();
                    dataStoreFuture1.checkedGet();
                }catch (Exception e) {
                    fail("Can't get exception");
                }

                setException(internalDtxDataStoreTestTx2, iid1, errorType);
                CheckedFuture<Void, DTxException> writeFuture = writeData(mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                        operationType, iid1, dataStoreNodeId2,new TestIid1());
                try {
                    writeFuture.checkedGet();
                }catch (Exception e){
                    Assert.assertTrue("Can't get EditFailedException", e instanceof DTxException.EditFailedException);
                }
                Assert.assertEquals("Data size in netConf tx1 is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
                Assert.assertEquals("Data size in netConf tx2 is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx2.getTxDataSizeByIid(iid1));
                Assert.assertEquals("Data size in dataStore tx1 is wrong",
                        expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(iid1));
                Assert.assertEquals("Data size in dataStore tx2 is wrong",
                        expectedDataSizeInIdentifier, internalDtxDataStoreTestTx2.getTxDataSizeByIid(iid1));
            }
        }

        void testWriteAndRollbackOnFailureRollbackFail(ProviderType providerType, OperationType operationType){
            CheckedFuture<Void, DTxException> writeFuture = null;
            if (operationType == OperationType.DELETE){
                internalDtxNetconfTestTx2.createObjForIdentifier(iid1);
                internalDtxDataStoreTestTx2.createObjForIdentifier(iid1);
            }
            if (providerType == ProviderType.NETCONF){
                setException(internalDtxNetconfTestTx2, iid1, operationType);
                internalDtxNetconfTestTx2.setSubmitException(true);
                writeFuture = writeData(netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        operationType, iid1, netConfNodeId2,new TestIid1());

            }else{
                setException(internalDtxDataStoreTestTx2, iid1, operationType);
                internalDtxDataStoreTestTx2.setSubmitException(true);
                writeFuture = writeData(mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                        operationType, iid1, dataStoreNodeId2,new TestIid1());
            }
            try {
                writeFuture.checkedGet();
            }catch (Exception e){
                Assert.assertTrue("Can't get RollbackFailedException", e instanceof DTxException.RollbackFailedException);
            }
        }

        void testConcurrentWriteOnFailure(ProviderType providerType, final OperationType operationType){
            int expectedDataSizeInIdentifier = 1;
            int numOfThreads = (int)(Math.random() * 4) + 1;
            threadPool = Executors.newFixedThreadPool(numOfThreads);
            if (operationType == OperationType.DELETE){
                expectedDataSizeInIdentifier = 0;
                for (int i = 0; i < numOfThreads; i++) {
                    internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
                    internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
                }
            }
            if (providerType == ProviderType.NETCONF){
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            CheckedFuture<Void, DTxException> writeFuture = writeData(netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                                    operationType, (InstanceIdentifier<TestIid>)identifiers.get(finalI), netConfNodeId1,new TestIid());
                            try{
                                writeFuture.checkedGet();
                            }catch (Exception e) {
                                fail("Caught unexpected exception");
                            }
                        }
                    });
                }
                threadPool.shutdown();
                while (!threadPool.isTerminated()) {
                    Thread.yield();
                }
                Assert.assertEquals("Cache size in netConf tx1 is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfNodeId1));
                for (int i = 0; i < numOfThreads; i++) {
                    Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                }
            }else {
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            CheckedFuture<Void, DTxException> netConFuture = writeData(mixedDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                                    operationType, (InstanceIdentifier<TestIid>)identifiers.get(finalI), netConfNodeId1,new TestIid());
                            CheckedFuture<Void, DTxException> dataStoreFuture = writeData(mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                    operationType, (InstanceIdentifier<TestIid>)identifiers.get(finalI), dataStoreNodeId1,new TestIid());
                            try{
                                netConFuture.checkedGet();
                                dataStoreFuture.checkedGet();
                            }catch (Exception e) {
                                fail("Caught unexpected exception");
                            }
                        }
                    });
                }
                threadPool.shutdown();
                while (!threadPool.isTerminated()) {
                    Thread.yield();
                }
                Assert.assertEquals("Cache size in netConf tx1 is wrong",numOfThreads,
                        mixedDTx.getSizeofCacheByNodeId(netConfNodeId1));
                Assert.assertEquals("Cache size in dataStore tx1 is wrong",numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                        DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreNodeId1));
                for (int i = 0; i < numOfThreads; i++) {
                    Assert.assertEquals("Data size in netConf tx1 is wrong",
                            expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                    Assert.assertEquals("Data size in dataStore tx1 is wrong",
                            expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                }
            }
        }

        void testConcurrentWriteAndRollbackOnFailureRollbackSucceed(ProviderType providerType, final OperationType operationType,
                                                                    final OperationType errorType){
            int numOfThreads = (int)(Math.random() * 4) + 1;
            int expectedDataSizeInIdentifier = 0;
            threadPool = Executors.newFixedThreadPool(numOfThreads);
            final int errorOccur = (int)(Math.random() * numOfThreads);
            if (operationType == OperationType.DELETE){
                expectedDataSizeInIdentifier = 1;
                for (int i = 0; i < numOfThreads; i++){
                    internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
                    internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
                }
            }
            if (providerType == ProviderType.NETCONF){
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            if (finalI == errorOccur){
                                setException(internalDtxNetconfTestTx1, identifiers.get(finalI), errorType);
                            }
                            CheckedFuture<Void, DTxException> f = writeData(netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                                    operationType, (InstanceIdentifier<TestIid>)identifiers.get(finalI), netConfNodeId1, new TestIid());
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
                if (errorType == OperationType.READ){
                    Assert.assertEquals("Cache size is wrong",numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfNodeId1));
                }else {
                    Assert.assertEquals("Cache size is wrong",numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfNodeId1));
                }
                for (int i = 0; i < numOfThreads; i++) {
                    Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                }
            }else{
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            if (finalI == errorOccur){
                                setException(internalDtxNetconfTestTx1, identifiers.get(finalI), errorType);
                            }
                            CheckedFuture<Void, DTxException> netConfFuture = writeData(mixedDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                                    operationType, (InstanceIdentifier<TestIid>)identifiers.get(finalI), netConfNodeId1, new TestIid());
                            CheckedFuture<Void, DTxException> dataStoreFuture = writeData(mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                    operationType, (InstanceIdentifier<TestIid>)identifiers.get(finalI), dataStoreNodeId1, new TestIid());
                            try{
                                netConfFuture.checkedGet();
                                dataStoreFuture.checkedGet();
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
                if (errorType == OperationType.READ) {
                    Assert.assertEquals("Cache size in netConf tx1 is wrong", numOfThreads - 1, mixedDTx.getSizeofCacheByNodeId(netConfNodeId1));
                    Assert.assertEquals("Cache size in dataStore tx1 is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            dataStoreNodeId1));
                }else{
                    Assert.assertEquals("Cache size in netConf tx1 is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfNodeId1));
                    Assert.assertEquals("Cache size in dataStore tx1 is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            dataStoreNodeId1));
                }
                for (int i = 0; i < numOfThreads; i++) {
                    Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInIdentifier,
                            internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                    Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInIdentifier,
                            internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                }
            }
        }

        void testConcurrentWriteToSameIidRollbackSucceed(ProviderType providerType, final OperationType operationType,
                                                         final OperationType errorType){
            int numOfThreads = (int) (Math.random() * 3) + 1;
            int expectedDataSizeInIdentifier = 0;
            threadPool = Executors.newFixedThreadPool(numOfThreads);
            final int errorOccur = (int)(Math.random() * numOfThreads);
            if (operationType == OperationType.DELETE){
                expectedDataSizeInIdentifier = 1;
                internalDtxNetconfTestTx1.createObjForIdentifier(iid1);
                internalDtxDataStoreTestTx1.createObjForIdentifier(iid1);
            }
            if (providerType == ProviderType.NETCONF){
                for (int i = 0; i < numOfThreads; i++){
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            if (finalI == errorOccur){
                                setException(internalDtxNetconfTestTx1, iid1, errorType);
                            }
                            CheckedFuture<Void, DTxException> writeFuture = writeData(netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                                    operationType, iid1, netConfNodeId1, new TestIid1());
                            try{
                                writeFuture.checkedGet();
                            }catch (DTxException e){
                                if (finalI != errorOccur)
                                    fail("Caught unexpected exception");
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
                if (errorType == OperationType.READ){
                    Assert.assertEquals("Cache size is wrong",
                            numOfThreads - 1, netConfOnlyDTx.getSizeofCacheByNodeId(netConfNodeId1));
                }else {
                    Assert.assertEquals("Cache size is wrong",
                            numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfNodeId1));
                }
                Assert.assertEquals("Data size is wrong",
                        expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
            }else{
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            if (finalI == errorOccur) {
                                setException(internalDtxDataStoreTestTx1, iid1, errorType);
                            }
                            CheckedFuture<Void, DTxException> dataStoreWriteFuture = writeData(mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                    operationType, (InstanceIdentifier<TestIid>)identifiers.get(finalI), dataStoreNodeId1, new TestIid());
                            try {
                                dataStoreWriteFuture.checkedGet();
                            } catch (DTxException e) {
                                if (finalI != errorOccur)
                                    fail("Caught unexpected exception");
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
                if (errorType == OperationType.READ){
                    Assert.assertEquals("Cache size is wrong", numOfThreads - 1,
                            mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreNodeId1));
                }else {
                    Assert.assertEquals("Cache size is wrong", numOfThreads,
                            mixedDTx.getSizeofCacheByNodeIdAndType(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreNodeId1));
                }
                Assert.assertEquals("Data size in datStore tx1 is wrong",
                        expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(iid1));
            }
        }

        void testConcurrentWriteAndSubmit(ProviderType providerType, final OperationType operationType){
            int numOfThreads = (int)(Math.random() * 3) + 1;
            int expectedDataSizeInIdentifier = 1;
            threadPool = Executors.newFixedThreadPool(numOfThreads * netconfNodes.size());
            if (operationType == OperationType.DELETE){
                expectedDataSizeInIdentifier = 0;
                for (int i = 0; i < numOfThreads; i++){
                    internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
                    internalDtxNetconfTestTx2.createObjForIdentifier(identifiers.get(i));
                    internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
                    internalDtxDataStoreTestTx2.createObjForIdentifier(identifiers.get(i));
                }
            }
            if (providerType == ProviderType.NETCONF) {
                for (final InstanceIdentifier<?> nodeIid : netconfNodes) {
                    for (int i = 0; i < numOfThreads; i++) {
                        final int finalI = i;
                        threadPool.execute(new Runnable() {
                            @Override
                            public void run() {
                                writeData(netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, operationType,
                                        (InstanceIdentifier<TestIid>) identifiers.get(finalI), nodeIid, new TestIid1());
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
            }else{
                for (final DTXLogicalTXProviderType type : nodesMap.keySet()){
                    for (final InstanceIdentifier<?> nodeIid : nodesMap.get(type)){
                        for (int i = 0; i < numOfThreads; i++) {
                            final int finalI = i;
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    writeData(mixedDTx, type, operationType,
                                            (InstanceIdentifier<TestIid>) identifiers.get(finalI), nodeIid, new TestIid1());
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
        }

        void testConcurrentWriteRollback(ProviderType providerType, final OperationType operationType){
            int numOfThreads = (int)(Math.random() * 4) + 1;
            int expectedDataSizeInIdentifier = 0;
            threadPool = Executors.newFixedThreadPool(numOfThreads);
            if (operationType == OperationType.DELETE){
                expectedDataSizeInIdentifier = 1;
                for (int i = 0; i < numOfThreads; i++){
                    internalDtxNetconfTestTx1.createObjForIdentifier(identifiers.get(i));
                    internalDtxDataStoreTestTx1.createObjForIdentifier(identifiers.get(i));
                }
            }
            if (providerType == ProviderType.NETCONF) {
                for (int i = 0; i < numOfThreads; i++) {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            writeData(netConfOnlyDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, operationType,
                                    (InstanceIdentifier<TestIid>) identifiers.get(finalI), netConfNodeId1, new TestIid1());
                        }
                    });
                }
                threadPool.shutdown();
                while (!threadPool.isTerminated()) {
                    Thread.yield();
                }
                CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = netConfOnlyDTx.rollback();
                try {
                    rollbackFuture.checkedGet();
                } catch (Exception e) {
                    fail("Get rollback exception");
                }
                Assert.assertEquals("Cache size is wrong", numOfThreads, netConfOnlyDTx.getSizeofCacheByNodeId(netConfNodeId1));
                for (int i = 0; i < numOfThreads; i++) {
                    Assert.assertEquals("Data size is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                }
            }else{
                for (int i = 0; i < numOfThreads; i++)
                {
                    final int finalI = i;
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            writeData(mixedDTx, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, operationType,
                                    (InstanceIdentifier<TestIid>) identifiers.get(finalI), netConfNodeId1, new TestIid1());
                            writeData(mixedDTx, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, operationType,
                                    (InstanceIdentifier<TestIid>) identifiers.get(finalI), dataStoreNodeId1, new TestIid1());
                        }
                    });
                }
                threadPool.shutdown();
                while (!threadPool.isTerminated()) {
                    Thread.yield();
                }
                CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = mixedDTx.rollback();
                try {
                    rollbackFuture.checkedGet();
                } catch (Exception e) {
                    fail("Get rollback exception");
                }
                Assert.assertEquals("Cache size in netConf tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeId(netConfNodeId1));
                Assert.assertEquals("Cache size in dataStore tx is wrong", numOfThreads, mixedDTx.getSizeofCacheByNodeIdAndType(
                        DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreNodeId1));
                for (int i = 0; i < numOfThreads; i++) {
                    Assert.assertEquals("Data size in netConf tx is wrong", expectedDataSizeInIdentifier, internalDtxNetconfTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                    Assert.assertEquals("Data size in dataStore tx is wrong", expectedDataSizeInIdentifier, internalDtxDataStoreTestTx1.getTxDataSizeByIid(identifiers.get(i)));
                }
            }
        }

        private <T extends DataObject> CheckedFuture<Void, DTxException> writeData(DTx dTx, DTXLogicalTXProviderType providerType, OperationType operationType,
                                                      InstanceIdentifier<T> iid, InstanceIdentifier<?> nodeId, T data){
            CheckedFuture<Void, DTxException> writeFuture = null;
            switch (operationType){
                case PUT:
                    writeFuture = dTx.putAndRollbackOnFailure(providerType, LogicalDatastoreType.OPERATIONAL, iid, data, nodeId);
                    break;
                case MERGE:
                    writeFuture = dTx.mergeAndRollbackOnFailure(providerType, LogicalDatastoreType.OPERATIONAL, iid, data, nodeId);
                    break;
                case DELETE:
                    writeFuture = dTx.deleteAndRollbackOnFailure(providerType, LogicalDatastoreType.OPERATIONAL, iid, nodeId);
            }
            return writeFuture;
        }

        private void setException(DTXTestTransaction testTx, InstanceIdentifier<?> iid, OperationType type){
            if (type == OperationType.READ){
                testTx.setReadExceptionByIid(iid, true);
            }else if (type == OperationType.PUT){
                testTx.setPutExceptionByIid(iid, true);
            }else if (type == OperationType.MERGE){
                testTx.setMergeExceptionByIid(iid, true);
            }else{
                testTx.setDeleteExceptionByIid(iid, true);
            }
        }
    }

    @Before
    @Test
    public void testInit(){
        netconfNodes = new HashSet<>();
        this.netConfNodeId1 = InstanceIdentifier.create(NetconfNode1.class);
        netconfNodes.add(netConfNodeId1);
        this.netConfNodeId2 = InstanceIdentifier.create(NetConfNode2.class);
        netconfNodes.add(netConfNodeId2);
        iid1 = InstanceIdentifier.create(TestIid1.class);
        iid2 = InstanceIdentifier.create(TestIid2.class);
        iid3 = InstanceIdentifier.create(TestIid3.class);
        iid4 = InstanceIdentifier.create(TestIid4.class);
        identifiers = Lists.newArrayList(iid1, iid2, iid3, iid4);

        internalDtxNetconfTestTx1 = new DTXTestTransaction();
        internalDtxNetconfTestTx1.addInstanceIdentifiers(iid1, iid2, iid3, iid4);
        internalDtxNetconfTestTx2 = new DTXTestTransaction();
        internalDtxNetconfTestTx2.addInstanceIdentifiers(iid1, iid2, iid3, iid4);
        Map<DTXLogicalTXProviderType, TxProvider> netconfTxProviderMap = new HashMap<>();
        TxProvider netconfTxProvider = new myNetconfTxProvider();
        netconfTxProviderMap.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, netconfTxProvider);
        netConfOnlyDTx = new DtxImpl(netconfTxProvider, netconfNodes, new DTxTransactionLockImpl(netconfTxProviderMap));

        testClass = new TestClass();

        dataStoreNodes = new HashSet<>();
        this.dataStoreNodeId1 = InstanceIdentifier.create(DataStoreNode1.class);
        dataStoreNodes.add(dataStoreNodeId1);
        this.dataStoreNodeId2 = InstanceIdentifier.create(DataStoreNode2.class);
        dataStoreNodes.add(dataStoreNodeId2);
        internalDtxDataStoreTestTx1 = new DTXTestTransaction();
        internalDtxDataStoreTestTx1.addInstanceIdentifiers(iid1, iid2, iid3, iid4);
        internalDtxDataStoreTestTx2 = new DTXTestTransaction();
        internalDtxDataStoreTestTx2.addInstanceIdentifiers(iid1, iid2, iid3, iid4);

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
                        (Set)Sets.newHashSet(netConfNodeId1, netConfNodeId2) : (Set)Sets.newHashSet(dataStoreNodeId1, dataStoreNodeId2);
            }
        });

        mixedDTx = new DtxImpl(txProviderMap, nodesMap, new DTxTransactionLockImpl(txProviderMap));
    }

    /**
     * Test netConf only DTx putAndRollbackOnFailure() with successful put
     */
    @Test
    public void testPutAndRollbackOnFailureInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailure(ProviderType.NETCONF, OperationType.PUT);
    }

    /**
     * Test mixed providers DTx putAndRollbackOnFailure() with successful put
     */
    @Test
    public void testPutAndRollbackOnFailureInMixedDTx(){
        testClass.testWriteAndRollbackOnFailure(ProviderType.MIX, OperationType.PUT);
    }

    /**
     * Test netConf only DTx putAndRollbackOnFailure() with failed read and successful rollback
     */
    @Test
    public void testPutAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.PUT, OperationType.READ);

    }

    /**
     * Test netConf only DTx putAndRollbackOnFailure() with failed put and successful rollback
     */
    @Test
    public void testPutAndRollbackOnFailurePutFailRollbackSucceedInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.PUT, OperationType.PUT);
    }

    /**
     * Test mixed providers DTx putAndRollbackOnFailure() with failed read and successful rollback
     */
    @Test
    public void testPutAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.PUT, OperationType.READ);
    }

    /**
     * Test mixed providers DTx putAndRollbackOnFailure() with failed put and successful rollback
     */
    @Test
    public void testPutAndRollbackOnFailurePutFailRollbackSucceedInMixedDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.PUT, OperationType.PUT);
    }

    /**
     * Test netConf only DTx putAndRollbackOnFailure() with failed rollback
     */
    @Test
    public void testPutAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackFail(ProviderType.NETCONF, OperationType.PUT);
    }

    /**
     * Test mixed providers DTx putAndRollbackOnFailure() with failed rollback
     */
    @Test
    public void testPutAndRollbackOnFailureRollbackFailInMixedDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackFail(ProviderType.MIX, OperationType.PUT);
    }

    /**
     * Test thread safety of netConf only DTx putAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureInNetConfOnlyDTx(){
        testClass.testConcurrentWriteOnFailure(ProviderType.NETCONF, OperationType.PUT);

    }

    /**
     * Test thread safety of mixed providers putAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureInMixedDTx(){
        testClass.testConcurrentWriteOnFailure(ProviderType.MIX, OperationType.PUT);
    }

    /**
     * Test thread safety of netConf only DTx putAndRollbackOnFailure() with failed read and successful rollback for different iids
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx(){
        testClass.testConcurrentWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.PUT,
                OperationType.READ);
    }

    /**
     * Test thread safety of mixed provider DTx putAndRollbackOnFailure() with failed read and successful rollback for different iids
     */
    @Test
    public void testConcurrentPutAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        testClass.testConcurrentWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.PUT,
                OperationType.READ);
    }

    /**
     * Test netConf only DTx putAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully for same iid
     */
    @Test
    public void testConcurrentPutToSameIidReadFailRollbackSucceedInNetConfOnlyDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.NETCONF, OperationType.PUT, OperationType.READ);
    }
    /**
     * Test netConf only DTx putAndRollbackOnFailure(). One of threads fail to put and DTx rollback successfully for same iid
     */
    @Test
    public void testConcurrentPutToSameIidPutFailRollbackSucceedInNetConfOnlyDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.NETCONF, OperationType.PUT, OperationType.PUT);
    }

    /**
     * Test mixed providers DTx putAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully for same iid
     */
    @Test
    public void testConcurrentPutToSameIidReadFailRollbackSucceedInMixedDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.MIX, OperationType.PUT, OperationType.READ);
    }

    /**
     * Test  mixed providers DTx putAndRollbackOnFailure(). One of threads fail to put and DTx rollback successfully for same iid
     */
    @Test
    public void testConcurrentPutToSameIidPutFailRollbackSucceedInMixedDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.MIX, OperationType.PUT, OperationType.PUT);
    }

    /**
     * Test netConf only DTx mergeAndRollbackOnFailure() with successful merge
     */
    @Test
    public void testMergeAndRollbackOnFailureInNetConfOnlyDTx()  {
        testClass.testWriteAndRollbackOnFailure(ProviderType.NETCONF, OperationType.MERGE);
    }

    /**
     * Test mixed providers DTx mergeAndRollbackOnFailure() with successful merge
     */
    @Test
    public void testMergeAndRollbackOnFailureInMixedDTx()  {
        testClass.testWriteAndRollbackOnFailure(ProviderType.MIX, OperationType.MERGE);
    }

    /**
     * Test netConf only DTx mergeAndRollbackOnFailure() with failed read and successful rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.MERGE, OperationType.READ);
    }

    /**
     * Test netConf only DTx mergeAndRollbackOnFailure() with failed merge and successful rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureMergeFailRollbackSucceedInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.MERGE, OperationType.MERGE);
    }

    /**
     * Test mixed providers DTx mergeAndRollbackOnFailure() with failed read and successful rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.MERGE, OperationType.READ);
    }

    /**
     * Test mixed providers DTx mergeAndRollbackOnFailure() with failed merge and successful rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureMergeFailRollbackSucceedInMixedDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.MERGE, OperationType.MERGE);
    }

    /**
     * Test netConf only DTx mergeAndRollbackOnFailure() with failed rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackFail(ProviderType.NETCONF, OperationType.MERGE);
    }

    /**
     * Test mixed providers DTx mergeAndRollbackOnFailure() with failed rollback
     */
    @Test
    public void testMergeAndRollbackOnFailureRollbackFailInMixedDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackFail(ProviderType.MIX, OperationType.MERGE);
    }

    /**
     * Test netConf only DTx thread safety of mergeAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureInNetConfOnlyDTx() {
        testClass.testConcurrentWriteOnFailure(ProviderType.NETCONF, OperationType.MERGE);
    }

    /**
     * Test thread safety of mixed providers DTx mergeAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureInMixedDTx() {
        testClass.testConcurrentWriteOnFailure(ProviderType.MIX, OperationType.MERGE);
    }

    /**
     * Test thread safety of netConf only DTx mergeAndRollbackOnFailure() with failed read and successful rollback for different iids
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        int expectedDataSizeInIdentifier = 0;
        testClass.testConcurrentWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.MERGE,
                OperationType.READ);
    }

    /**
     * Test thread safety of mixed providers DTx mergeAndRollbackOnFailure() with failed read and successful rollback for different iids
     */
    @Test
    public void testConcurrentMergeAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx() {
        testClass.testConcurrentWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.MERGE,
                OperationType.READ);
    }

    /**
     * Test netConf only DTx mergeAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully for same iid
     */
    @Test
    public void testConcurrentMergeToSameIidReadFailRollbackSucceedInNetConfOnlyDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.NETCONF, OperationType.MERGE,
                OperationType.READ);
    }

    /**
     * Test netConf only DTx mergeAndRollbackOnFailure(). One of threads fail to merge and DTx rollback successfully for same iid
     */
    @Test
    public void testConcurrentMergeToSameIidMergeFailRollbackSucceedInNetConfOnlyDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.NETCONF, OperationType.MERGE,
                OperationType.MERGE);
    }

    /**
     * Test mixed providers DTx mergeAndRollbackOnFailure(). One of threads fail to read and DTx successfully rollback for same iid
     */
    @Test
    public void testConcurrentMergeToSameIidReadFailRollbackSucceedInMixedDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.MIX, OperationType.MERGE,
                OperationType.READ);
    }

    /**
     * Test mixed providers DTx mergeAndRollbackOnFailure(). One of threads fail to merge and DTx rollback successfully for same iid
     */
    @Test
    public void testConcurrentMergeToSameIidMergeFailRollbackSucceedInMixedDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.MIX, OperationType.MERGE,
                OperationType.MERGE);
    }

    /**
     * Test netConf only DTx deleteAndRollbackOnFailure() with successful delete
     */
    @Test
    public void testDeleteAndRollbackOnFailureInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailure(ProviderType.NETCONF, OperationType.DELETE);
    }

    /**
     * Test mixed providers DTx deleteAndRollbackOnFailure() with successful delete
     */
    @Test
    public void testDeleteAndRollbackOnFailureInMixedDTx() {
        testClass.testWriteAndRollbackOnFailure(ProviderType.MIX, OperationType.DELETE);
    }

    /**
     * Test netConf only DTx deleteAndRollbackOnFailure() with failed read and successful rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.DELETE,
                OperationType.READ);
    }

    /**
     * Test  netConf only DTx deleteAndRollbackOnFailure () with failed delete and successful rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureDeleteFailRollbackSucceedInNetConfOnlyDTx() {
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.DELETE, OperationType.DELETE);
    }

    /**
     * Test mixed providers DTx deleteAndRollbackOnFailure() with failed read and successful rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.DELETE, OperationType.READ);
    }

    /**
     * Test mixed providers DTx deleteAndRollbackOnFailure () failed delete and successful rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureDeleteFailRollbackSucceedInMixedDTx(){
        testClass.testWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.DELETE, OperationType.DELETE);
    }

    /**
     * Test netConf only DTx deleteAndRollbackOnFailure() with failed rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureRollbackFailInNetConfOnlyDTx() {
        internalDtxNetconfTestTx2.createObjForIdentifier(iid1);
        testClass.testWriteAndRollbackOnFailureRollbackFail(ProviderType.NETCONF, OperationType.DELETE);
    }

    /**
     * Test mixed providers DTx deleteAndRollbackOnFailure() with failed rollback
     */
    @Test
    public void testDeleteAndRollbackOnFailureRollbackFailInMixedDTx(){
        internalDtxDataStoreTestTx2.createObjForIdentifier(iid1);
        testClass.testWriteAndRollbackOnFailureRollbackFail(ProviderType.MIX, OperationType.DELETE);
    }

    /**
     * Test thread safety of netConf only DTx deleteAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureInNetConfOnlyDTx(){
        testClass.testConcurrentWriteOnFailure(ProviderType.NETCONF, OperationType.DELETE);
    }

    /**
     * Test thread safety of mixed providers DTx deleteAndRollbackOnFailure()
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureInMixedDTx(){
        testClass.testConcurrentWriteOnFailure(ProviderType.MIX, OperationType.DELETE);
    }

    /**
     * Test thread safety of netConf only DTx deleteAndRollbackOnFailure() with failed read and successful rollback for different iids
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureReadFailRollbackSucceedInNetConfOnlyDTx(){
        testClass.testConcurrentWriteAndRollbackOnFailureRollbackSucceed(ProviderType.NETCONF, OperationType.DELETE,
                OperationType.READ);
    }

    /**
     * Test thread safety of mixed provider DTx deleteAndRollbackOnFailure() with failed read and successful rollback for different iids
     */
    @Test
    public void testConcurrentDeleteAndRollbackOnFailureReadFailRollbackSucceedInMixedDTx(){
        testClass.testConcurrentWriteAndRollbackOnFailureRollbackSucceed(ProviderType.MIX, OperationType.DELETE,
                OperationType.READ);
    }

    /**
     * Test netConf only DTx deleteAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully for same iid
     */
    @Test
    public void testConcurrentDeleteToSameIidReadFailRollbackSucceedInNetConfDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.NETCONF, OperationType.DELETE,
                OperationType.READ);
    }

    /**
     * Test mixed providers DTx deleteAndRollbackOnFailure(). One of threads fail to read and DTx rollback successfully for same iid in
     */
    @Test
    public void testConcurrentDeleteToSameIidReadFailRollbackSucceedInMixedDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.MIX, OperationType.DELETE,
                OperationType.READ);
    }

    /**
     * Test netConf only DTx deleteAndRollbackOnFailure(). One of threads fail to delete and DTx rollback successfully
     */
    @Test
    public void testConcurrentDeleteToSameIidDeleteFailRollbackSucceedInNetConfOnlyDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.NETCONF, OperationType.DELETE,
                OperationType.DELETE);
    }

    /**
     * Test mixed providers DTx deleteAndRollbackOnFailure(). One of threads fail to delete and DTx rollback successfully
     */
    @Test
    public void testConcurrentDeleteToSameIidDeleteFailRollbackSucceedInMixedDTx(){
        testClass.testConcurrentWriteToSameIidRollbackSucceed(ProviderType.MIX, OperationType.DELETE,
                OperationType.DELETE);
    }

    /**
     * Test netConf only DTx submit() with multi threads to successfully put data to nodes
     */
    @Test
    public void testConcurrentPutAndSubmitInNetConfOnlyDTx(){
        testClass.testConcurrentWriteAndSubmit(ProviderType.NETCONF, OperationType.PUT);
    }

    /**
     * Test mixed providers DTx submit() with multi threads to successfully put data to nodes
     */
    @Test
    public void testConcurrentPutAndSubmitInMixedDTx(){
        testClass.testConcurrentWriteAndSubmit(ProviderType.MIX, OperationType.PUT);
    }

    /**
     * Test netConf only DTx submit() with multi threads to successfully merge data to nodes
     */
    @Test
    public void testConcurrentMergeAndSubmitInNetConfOnlyDTx(){
        testClass.testConcurrentWriteAndSubmit(ProviderType.NETCONF, OperationType.MERGE);
    }

    /**
     * Test mixed providers DTx submit() with multi threads to successfully merge data to nodes
     */
    @Test
    public void testConcurrentMergeAndSubmitInMixedDTx(){
        testClass.testConcurrentWriteAndSubmit(ProviderType.MIX, OperationType.MERGE);
    }

    /**
     * Test netConf only DTx submit() with multi threads to successfully delete data in nodes
     */
    @Test
    public void testConcurrentDeleteAndSubmitInNetConfOnlyDTx(){
        testClass.testConcurrentWriteAndSubmit(ProviderType.NETCONF, OperationType.DELETE);
    }

    /**
     * Test mixed providers DTx submit() with multi threads to successfully delete data in nodes
     */
    @Test
    public void testConcurrentDeleteAndSubmitInMixedDTx(){
        testClass.testConcurrentWriteAndSubmit(ProviderType.MIX, OperationType.DELETE);
    }

    /**
     * Test netConf only DTx rollback() with successful rollback
     */
    @Test
    public void testRollbackInNetConfOnlyDTx() {
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId2);

        try {
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e) {
            fail("Caught unexpected exception");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> f = netConfOnlyDTx.rollback();
        try{
            f.checkedGet();
            Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
            Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(iid1));
        }catch (Exception e) {
            fail("Get rollback exception");
        }
    }

    /**
     * Test mixed providers DTx rollback() with successful rollback
     */
    @Test
    public void testRollbackInMixedDTx() {
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), dataStoreNodeId1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), dataStoreNodeId2);

        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e) {
            fail("Caught unexpected exception");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFut = mixedDTx.rollback();
        try{
            rollbackFut.checkedGet();
        }catch (Exception e) {
            fail("Caught unexpected exception");
        }
        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(iid1));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(iid1));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(iid1));
    }

    /**
     * Test netConf only DTx rollback() with failed rollback
     */
    @Test
    public void testRollbackFailInNetConfOnlyDTx()
    {
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId2);
        try{
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e) {
            fail("Caught unexpected exception");
        }
        internalDtxNetconfTestTx2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> f = netConfOnlyDTx.rollback();
        try {
            f.checkedGet();
            fail("Can't get exception");
        }catch (Exception e) {
            Assert.assertTrue("Can't get RollbackFailedException", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * Test mixed providers DTx rollback() with failed rollback
     */
    @Test
    public void testRollbackFailInMixedDTx()
    {
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), dataStoreNodeId1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), dataStoreNodeId2);
        try{
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e) {
            fail("Caught unexpected exception");
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
     * Test rollback(). NetConf only DTx wait for all put actions to finish before performing rollback
     */
    @Test
    public void testConcurrentPutRollbackInNetConfOnlyDTx() {
        testClass.testConcurrentWriteRollback(ProviderType.NETCONF, OperationType.PUT);
    }

    /**
     * Test rollback(). Mixed providers DTx wait for all put actions finish before performing rollback
     */
    @Test
    public void testConcurrentPutRollbackInMixedDTx() {
        testClass.testConcurrentWriteRollback(ProviderType.MIX, OperationType.PUT);
    }

    /**
     * Test rollback(). NetConf only DTx wait for all merge actions to finish before performing rollback
     */
    @Test
    public void testConcurrentMergeRollbackInNetConfOnlyDTx() {
        testClass.testConcurrentWriteRollback(ProviderType.NETCONF, OperationType.MERGE);
    }

    /**
     * Test rollback(). Mixed providers DTx wait for all merge actions finish before performing rollback
     */
    @Test
    public void testConcurrentMergeRollbackInMixedDTx() {
        testClass.testConcurrentWriteRollback(ProviderType.MIX, OperationType.MERGE);
    }

    /**
     * Test rollback(). NetConf only DTx wait for all delete actions finish before performing rollback
     */
    @Test
    public void testConcurrentDeleteRollbackInNetConfOnlyDTx() {
        testClass.testConcurrentWriteRollback(ProviderType.NETCONF, OperationType.DELETE);
    }

    /**
     * Test rollback(). Mixed providers DTx wait for all delete actions finish before performing rollback
     */
    @Test
    public void testConcurrentDeleteRollbackInMixedDTx() {
        testClass.testConcurrentWriteRollback(ProviderType.MIX, OperationType.DELETE);
    }

    /**
     * Test netConf only DTx submit() with successful submit
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
     * Test mixed providers DTx submit() with successful submit
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
     * Test netConf only DTx submit() with failed submit and successful rollback
     */
    @Test
    public void testSubmitRollbackSucceedInNetConfOnlyDTx()  {
        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId2);
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

        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(iid1));
    }

    /**
     * Test mixed providers DTx submit() with failed submit and successful rollback
     */
    @Test
    public void testSubmitRollbackSucceedInMixedDTx()  {
        int expectedDataSizeInNetConfTx1 = 0, expectedDataSizeInNetConfTx2 = 0, expectedDataSizeInDataStoreTx1 = 0, expectedDataSizeInDataStoreTx2 = 0;
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId1);
        CheckedFuture<Void, DTxException> f2 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), netConfNodeId2);
        CheckedFuture<Void, DTxException> f3 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), dataStoreNodeId1);
        CheckedFuture<Void, DTxException> f4 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), dataStoreNodeId2);
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
        Assert.assertEquals("Data size in netConf tx1 is wrong", expectedDataSizeInNetConfTx1, internalDtxNetconfTestTx1.getTxDataSizeByIid(iid1));
        Assert.assertEquals("Data size in netConf tx2 is wrong", expectedDataSizeInNetConfTx2, internalDtxNetconfTestTx2.getTxDataSizeByIid(iid1));
        Assert.assertEquals("Data size in dataStore tx1 is wrong", expectedDataSizeInDataStoreTx1, internalDtxDataStoreTestTx1.getTxDataSizeByIid(iid1));
        Assert.assertEquals("Data size in dataStore tx2 is wrong", expectedDataSizeInDataStoreTx2, internalDtxDataStoreTestTx2.getTxDataSizeByIid(iid1));
    }

    /**
     * Test netConf only DTx submit() with failed rollback
     */
    @Test
    public void testSubmitRollbackFailInNetConfOnlyDTx()
    {
        internalDtxNetconfTestTx2.setSubmitException(true);
        internalDtxNetconfTestTx2.setDeleteExceptionByIid(iid1,true);

        CheckedFuture<Void, DTxException> f1 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, iid1,new TestIid1(), netConfNodeId1);
        CheckedFuture<Void, DTxException> f2 = netConfOnlyDTx.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, iid1,new TestIid1(), netConfNodeId2);
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
     * test mixed providers DTx submit() with failed rollback
     */
    @Test
    public void testSubmitRollbackFailInMixedDTx()
    {
        internalDtxDataStoreTestTx2.setSubmitException(true);
        internalDtxDataStoreTestTx2.setDeleteExceptionByIid(iid1, true);
        CheckedFuture<Void, DTxException> f1 = mixedDTx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.OPERATIONAL, iid1, new TestIid1(), dataStoreNodeId2);
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
    /* TODO: Test rollback() error cases */
}
