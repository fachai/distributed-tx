package org.opendaylight.distributed.tx.impl;

import com.google.common.util.concurrent.CheckedFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.DtxImpl;
import org.opendaylight.distributed.tx.spi.TxException;
import org.opendaylight.distributed.tx.spi.TxProvider;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

public class DtxImplTest{

    InstanceIdentifier<TestClassNode1> n1;
    InstanceIdentifier<TestClassNode2> n2;
    InstanceIdentifier<TestClassNode> n0;

    DTXTestTransaction internalDtxTestTransaction1;
    DTXTestTransaction internalDtxTestTransaction2;
    Set s;

    private class myTxProvider implements TxProvider{
        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {

            return nodeId == n1? internalDtxTestTransaction1 : internalDtxTestTransaction2;
        }
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

    private void testInit(){
        s = new HashSet<InstanceIdentifier<TestClassNode>>();
        this.n1 = InstanceIdentifier.create(TestClassNode1.class);
        s.add(this.n1);
        this.n2 = InstanceIdentifier.create(TestClassNode2.class);
        s.add(n2);
        this.n0 = InstanceIdentifier.create(TestClassNode.class);

        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);
    }

    @Before
    public void testConstructor() {
        testInit();
    }

    @Test
    public void testPutAndRollbackOnFailure() throws InterruptedException {

        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs ; i++) {

            CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
            CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

            Thread.sleep(20);

            Assert.assertTrue(f1.isDone());
            Assert.assertTrue(f2.isDone());
        }



        Assert.assertEquals("size of n1 data is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("size of n2 data is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
    }

    @Test
    public void testMergeAndRollbackOnFailure(){
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);


        CheckedFuture<Void, ReadFailedException> f = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
    }

    @Test
    public void testDeleteAndRollbackOnFailure(){
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);


        CheckedFuture<Void, ReadFailedException> f = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n1);
    }

    @Test public void testPut() {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
    }
    @Test public void testMerge() {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        dtxImpl.merge(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
    }
    @Test public void testDelete() {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        dtxImpl.delete(LogicalDatastoreType.OPERATIONAL, n0, n1);
    }
    @Test public void testRollback(){
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        testPut();
        testMerge();
        testDelete();
        // CheckedFuture<Void, DTxException.RollbackFailedException> f = this.dtxImpl.rollback();
    }
    @Test
    public void testSubmit() {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);


        testPut();
        testMerge();
        testDelete();

        // CheckedFuture<Void, TransactionCommitFailedException> f = this.dtxImpl.submit();
    }
}
