/*
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.distributed.tx.impl;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.CheckedFuture;
import org.junit.Assert;
import org.junit.Test;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.fail;

public class RollbackImplTest {
    InstanceIdentifier<TestData1> identifier1 = InstanceIdentifier.create(TestData1.class);
    InstanceIdentifier<TestData2> identifier2 = InstanceIdentifier.create(TestData2.class);
    InstanceIdentifier<TestNode1> node1 = InstanceIdentifier.create(TestNode1.class);
    InstanceIdentifier<TestNode2> node2 = InstanceIdentifier.create(TestNode2.class);

    private class TestData1 implements DataObject {
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestData2 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestNode1 implements DataObject {
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestNode2 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    /**
     * test rollback method, rollback succeed
     * put data in identifier1 and identifier2, after that invoking rollback
     * rollback succeed
     * no data in all the data IIDs
     */
    @Test
    public void testRollBack() {
        final DTXTestTransaction testTransaction1 = new DTXTestTransaction();
        final DTXTestTransaction testTransaction2 = new DTXTestTransaction();
        testTransaction1.addInstanceIdentifiers(identifier1, identifier2);
        testTransaction2.addInstanceIdentifiers(identifier1, identifier2);

        final CachingReadWriteTx cachingReadWriteTx1 = new CachingReadWriteTx(testTransaction1);
        final CachingReadWriteTx cachingReadWriteTx2 = new CachingReadWriteTx(testTransaction2);

        CheckedFuture<Void, DTxException> f1 = cachingReadWriteTx1.asyncPut(LogicalDatastoreType.OPERATIONAL, identifier1, new TestData1());
        CheckedFuture<Void, DTxException> f2 = cachingReadWriteTx1.asyncPut(LogicalDatastoreType.OPERATIONAL, identifier2, new TestData2());
        CheckedFuture<Void, DTxException> f3 = cachingReadWriteTx2.asyncPut(LogicalDatastoreType.OPERATIONAL, identifier1, new TestData1());
        CheckedFuture<Void, DTxException> f4 = cachingReadWriteTx2.asyncPut(LogicalDatastoreType.OPERATIONAL, identifier2, new TestData2());

        try
        {
            f1.checkedGet();
            f2.checkedGet();
            f3.checkedGet();
            f4.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception from the asyncPut");
        }

        Set<InstanceIdentifier<?>> s = Sets.newHashSet(node1, node2);
        Map<InstanceIdentifier<?>, ? extends CachingReadWriteTx> perNodeCaches;
        perNodeCaches = Maps.toMap(s, new Function<InstanceIdentifier<?>, CachingReadWriteTx>() {
            @Nullable
            @Override
            public CachingReadWriteTx apply(@Nullable InstanceIdentifier<?> instanceIdentifier) {
                return instanceIdentifier == node1? cachingReadWriteTx1:cachingReadWriteTx2;
            }
        });

        Map<InstanceIdentifier<?>, ReadWriteTransaction> perNodeRollbackTxs;
        perNodeRollbackTxs = Maps.toMap(s, new Function<InstanceIdentifier<?>, ReadWriteTransaction>() {
            @Nullable
            @Override
            public ReadWriteTransaction apply(@Nullable InstanceIdentifier<?> instanceIdentifier) {
                return instanceIdentifier == node1 ? testTransaction1 : testTransaction2;
            }
        });

        RollbackImpl testRollBack = new RollbackImpl();
        CheckedFuture<Void, DTxException.RollbackFailedException> rollBackFut = testRollBack.rollback(perNodeCaches, perNodeRollbackTxs);

        try
        {
           rollBackFut.checkedGet();
        }catch (Exception e)
        {
           fail("get the unexpected exception from the rollback method");
        }

        int  expectedDataNumInNode1Identifier1 = 0,
             expectedDataNumInNode1Identifier2 = 0,
             expectedDataNumInNode2Identifier1 = 0,
             expectedDataNumInNode2Identifier2 = 0;
        Assert.assertEquals("size of identifier1 data in transaction1 is wrong", expectedDataNumInNode1Identifier1,testTransaction1.getTxDataSizeByIid(identifier1));
        Assert.assertEquals("size of identifier2 data in transaction1 is wrong", expectedDataNumInNode1Identifier2,testTransaction1.getTxDataSizeByIid(identifier2));
        Assert.assertEquals("size of identifier1 data in transaction2 is wrong", expectedDataNumInNode2Identifier1,testTransaction2.getTxDataSizeByIid(identifier1));
        Assert.assertEquals("size of identifier2 data in transaction2 is wrong", expectedDataNumInNode2Identifier2,testTransaction2.getTxDataSizeByIid(identifier2));
    }

     /**
      * test rollback method, rollback fail case with write exception case
      * put data in identifier1
      *invoke rollback
      *delete exception occurs the rollback fail
      *get DTx.RollbackFailedException
      */
    @Test
    public void testRollbackFailWithWriteException() {
        DTXTestTransaction testTransaction = new DTXTestTransaction();
        testTransaction.addInstanceIdentifiers(identifier1);
        CachingReadWriteTx cachingReadWriteTx = new CachingReadWriteTx(testTransaction);

        CheckedFuture<Void, DTxException> f = cachingReadWriteTx.asyncPut(LogicalDatastoreType.OPERATIONAL, identifier1,new TestData1());
        try
        {
            f.checkedGet();
            Assert.assertEquals("can't put the data into the test transaction", 1, testTransaction.getTxDataSizeByIid(identifier1));
        }catch (Exception e)
        {
            fail("get unexpected exception from the AsyncPut");
        }

        Map<InstanceIdentifier<?>, CachingReadWriteTx> perNodeCaches = Maps.newHashMap();
        perNodeCaches.put(node1, cachingReadWriteTx);

        Map<InstanceIdentifier<?>, ReadWriteTransaction> perNodeRollbackTxs = Maps.newHashMap();
        perNodeRollbackTxs.put(node1, testTransaction);

        testTransaction.setDeleteExceptionByIid(identifier1,true);
        RollbackImpl testRollback = new RollbackImpl();
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture =  testRollback.rollback(perNodeCaches,perNodeRollbackTxs);

        try{
            rollbackFuture.checkedGet();
            fail("can't get the exception from the failed rollback");
        }catch (Exception e)
        {
            Assert.assertTrue("type of exception is wrong", e instanceof DTxException.RollbackFailedException);
        }
    }

    /**
     * test rollback method, rollback fail with submit exception case
     * put data in identifier1
     * invoke rollback
     * submit exception occurs and rollback fail
     * get DTxException.RollbackFailedException exception
     */
    @Test
    public void testRollbackFailWithSubmitException() {
        DTXTestTransaction testTransaction = new DTXTestTransaction();
        testTransaction.addInstanceIdentifiers(identifier1);
        CachingReadWriteTx cachingReadWriteTx = new CachingReadWriteTx(testTransaction);

        CheckedFuture<Void, DTxException> f = cachingReadWriteTx.asyncPut(LogicalDatastoreType.OPERATIONAL, identifier1, new TestData1());
        try
        {
            f.checkedGet();
            Assert.assertEquals("can't put the data into the test transaction", 1, testTransaction.getTxDataSizeByIid(identifier1));
        }catch (Exception e)
        {
            fail("get unexpected exception from the AsyncPut");
        }

        Map<InstanceIdentifier<?>, CachingReadWriteTx> perNodeCaches = Maps.newHashMap();
        perNodeCaches.put(node1, cachingReadWriteTx);

        Map<InstanceIdentifier<?>, ReadWriteTransaction> perNodeRollbackTxs = Maps.newHashMap();
        perNodeRollbackTxs.put(node1, testTransaction);

        testTransaction.setSubmitException(true);
        RollbackImpl testRollback = new RollbackImpl();
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture =  testRollback.rollback(perNodeCaches,perNodeRollbackTxs);

        try{
            rollbackFuture.checkedGet();
            fail("can't get the exception from the failed rollback");
        }catch (Exception e)
        {
            Assert.assertTrue("type of exception is wrong", e instanceof DTxException.RollbackFailedException);
        }
    }
}
