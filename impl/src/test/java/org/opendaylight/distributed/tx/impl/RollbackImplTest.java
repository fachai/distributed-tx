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
import org.opendaylight.distributed.tx.impl.spi.CachingReadWriteTx;
import org.opendaylight.distributed.tx.impl.spi.RollbackImpl;
import org.opendaylight.distributed.tx.spi.Rollback;
import org.opendaylight.distributed.tx.spi.TxCache;
import org.opendaylight.distributed.tx.spi.TxException;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
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

    @Test
    public void testRollBack() throws InterruptedException {
        DTXTestTransaction testTransaction1 = new DTXTestTransaction();
        DTXTestTransaction testTransaction2 = new DTXTestTransaction();

        final CachingReadWriteTx cachingReadWriteTx1 = new CachingReadWriteTx(testTransaction1); //nodeId1
        final CachingReadWriteTx cachingReadWriteTx2 = new CachingReadWriteTx(testTransaction2); //nodeId2

        cachingReadWriteTx1.put(LogicalDatastoreType.OPERATIONAL, identifier1, new TestData1());
        cachingReadWriteTx1.put(LogicalDatastoreType.OPERATIONAL, identifier2, new TestData2());
        cachingReadWriteTx2.put(LogicalDatastoreType.OPERATIONAL, identifier1, new TestData1());
        cachingReadWriteTx2.put(LogicalDatastoreType.OPERATIONAL, identifier2, new TestData2());

        Thread.sleep(20);

        Assert.assertEquals(1,testTransaction1.getTxDataSize(identifier1));
        Assert.assertEquals(1,testTransaction1.getTxDataSize(identifier2));
        Assert.assertEquals(1,testTransaction2.getTxDataSize(identifier1));
        Assert.assertEquals(1,testTransaction2.getTxDataSize(identifier2));

        Set<InstanceIdentifier<?>> s = Sets.newHashSet(node1, node2);
        Map<InstanceIdentifier<?>, ? extends CachingReadWriteTx> perNodeTransactions;

        perNodeTransactions = Maps.toMap(s, new Function<InstanceIdentifier<?>, CachingReadWriteTx>() {
            @Nullable
            @Override
            public CachingReadWriteTx apply(@Nullable InstanceIdentifier<?> instanceIdentifier) {
                return instanceIdentifier == node1? cachingReadWriteTx1:cachingReadWriteTx2;
            }
        });

        RollbackImpl testRollBack = new RollbackImpl();
        CheckedFuture<Void, DTxException.RollbackFailedException> rollBackFut = testRollBack.rollback(perNodeTransactions, perNodeTransactions);

        Thread.sleep(100);

        Assert.assertTrue(rollBackFut.isDone());

        Assert.assertEquals(0,testTransaction1.getTxDataSize(identifier1));
        Assert.assertEquals(0,testTransaction1.getTxDataSize(identifier2));
        Assert.assertEquals(0,testTransaction2.getTxDataSize(identifier1));
        Assert.assertEquals(0,testTransaction2.getTxDataSize(identifier2));
    }

    @Test
    public void testRollbackFailWithDeleteException() throws InterruptedException {
        DTXTestTransaction testTransaction = new DTXTestTransaction();
        CachingReadWriteTx cachingReadWriteTx = new CachingReadWriteTx(testTransaction);

        cachingReadWriteTx.put(LogicalDatastoreType.OPERATIONAL, identifier1,new TestData1());
        Thread.sleep(20);
        Assert.assertEquals(1, testTransaction.getTxDataSize(identifier1));

        Map<InstanceIdentifier<?>, CachingReadWriteTx> perNodeCaches = Maps.newHashMap();
        perNodeCaches.put(node1, cachingReadWriteTx);
        testTransaction.setDeleteException(true); // rollback will fail

        Map<InstanceIdentifier<?>, ReadWriteTransaction> perNodeRollbackTxs = Maps.newHashMap();
        perNodeRollbackTxs.put(node1, testTransaction);

        RollbackImpl testRollback = new RollbackImpl();
        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture =  testRollback.rollback(perNodeCaches,perNodeRollbackTxs);
        Thread.sleep(20);
        Assert.assertTrue(rollbackFuture.isDone());
        try{
            rollbackFuture.checkedGet();
            fail();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the rollbackFail exception", e instanceof DTxException.RollbackFailedException);
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testRollbackFailWithSubmitException() throws InterruptedException{
        DTXTestTransaction testTransaction1 = new DTXTestTransaction();
        DTXTestTransaction testTransaction2 = new DTXTestTransaction();

        final CachingReadWriteTx cachingReadWriteTx1 = new CachingReadWriteTx(testTransaction1); //nodeId1
        final CachingReadWriteTx cachingReadWriteTx2 = new CachingReadWriteTx(testTransaction2); //nodeId2

        cachingReadWriteTx1.put(LogicalDatastoreType.OPERATIONAL, identifier1, new TestData1());
        cachingReadWriteTx1.put(LogicalDatastoreType.OPERATIONAL, identifier2, new TestData2());
        cachingReadWriteTx2.put(LogicalDatastoreType.OPERATIONAL, identifier1, new TestData1());
        cachingReadWriteTx2.put(LogicalDatastoreType.OPERATIONAL, identifier2, new TestData2());

        Thread.sleep(20);

        Set<InstanceIdentifier<?>> s = Sets.newHashSet(node1, node2);
        Map<InstanceIdentifier<?>, ? extends CachingReadWriteTx> perNodeTransactions;

        perNodeTransactions = Maps.toMap(s, new Function<InstanceIdentifier<?>, CachingReadWriteTx>() {
            @Nullable
            @Override
            public CachingReadWriteTx apply(@Nullable InstanceIdentifier<?> instanceIdentifier) {
                return instanceIdentifier == node1? cachingReadWriteTx1:cachingReadWriteTx2;
            }
        });

        testTransaction2.setSubmitException(true);
        RollbackImpl testRollBack = new RollbackImpl();
        CheckedFuture<Void, DTxException.RollbackFailedException> rollBackFut = testRollBack.rollback(perNodeTransactions, perNodeTransactions);

        try
        {
            rollBackFut.checkedGet();
            fail();
        }catch (Exception e)
        {
            Assert.assertTrue(e instanceof DTxException.RollbackFailedException);
        }
    }
}
