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

import static org.junit.Assert.fail;

public class DtxImplTest{

    InstanceIdentifier<TestClassNode1> n1;
    InstanceIdentifier<TestClassNode2> n2;
    InstanceIdentifier<TestClassNode> n0;

    DTXTestTransaction internalDtxTestTransaction1; //transaction for node1
    DTXTestTransaction internalDtxTestTransaction2; //transaction for node2
    Set s; //nodeId set

    private class myTxProvider implements TxProvider{
        private boolean createTxException = false;
        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            if(!createTxException)
            return nodeId == n1? internalDtxTestTransaction1 : internalDtxTestTransaction2;
            else
                throw new TxException.TxInitiatizationFailedException("create tx exception");
        }
        public void setCreateTxException(boolean createTxException)
        {
            this.createTxException = createTxException;
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
        s = new HashSet<>();
        this.n1 = InstanceIdentifier.create(TestClassNode1.class);
        s.add(n1);
        this.n2 = InstanceIdentifier.create(TestClassNode2.class);
        s.add(n2);
        this.n0 = InstanceIdentifier.create(TestClassNode.class);

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

            Thread.sleep(25);

            Assert.assertTrue(f1.isDone());
            Assert.assertTrue(f2.isDone());
        }

        Assert.assertEquals("size of n1 data is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("size of n2 data is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
    }
    @Test
    public void testPutAndRollbackOnFailureRollbackSucceed() throws InterruptedException {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        //simulate the case in which put some data in n1 and n2
        //then try to put data in n2 but can't read the data
        //the transaction fail but rollback succeed
        //at last no data in both n1 and n2

        for (int i = 0; i < 10 ; i++) {
            dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
            dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        }
        Thread.sleep(20);

        internalDtxTestTransaction2.setReadException(true);
        CheckedFuture<Void, ReadFailedException> f = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        Thread.sleep(20);
        internalDtxTestTransaction2.setReadException(false);

        Thread.sleep(500); // the main Thread must be halted for enough time until the rollback finish
        Assert.assertTrue(f.isDone());

        try{
            f.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof ReadFailedException);
        }
        Assert.assertEquals("Size of the identifier n0's data in n1 is wrong", 0, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in n2 is wrong", 0, internalDtxTestTransaction2.getTxDataSize(n0));

    }

    @Test
    public void testPutAndRollbackOnFailureRollbackFail() throws InterruptedException {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        //simulate the case in which we successfully put the data in n1
        //then try to put data in n2, but an exception occur and begin to rollback
        //when we roll back in n2, submit exception occur and the rollback fail
        //FIXME the exception type check should be changed, because both rollback succeed and rollback fail have the same ReadFailException

        internalDtxTestTransaction2.setPutException(true);
        internalDtxTestTransaction2.setSubmitException(true); //submit fail the rollback will fail

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        Thread.sleep(20);
        internalDtxTestTransaction2.setPutException(false);

        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("Transaction1 get the unexpected exception, the test has failed ");
        }

        Thread.sleep(100);
        Assert.assertTrue(f2.isDone());

        try{
            f2.checkedGet();
            fail("Transaction2 can't get the exception, the test has failed");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected ReadFailException, the test failed", e instanceof ReadFailedException);
        }

        //the order of rollback is random, we can't use the size of the data in transaction to do the test
//        Assert.assertEquals("Size of the identifier n0's data in transaction1 is wrong", 0, internalDtxTestTransaction1.getTxDataSize(n0));
//        Assert.assertEquals("Size of the identifier n0's data in transaction2 is wrong", 0, internalDtxTestTransaction2.getTxDataSize(n0));
    }
    @Test
    public void testMergeAndRollbackOnFailure(){
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);


        CheckedFuture<Void, ReadFailedException> f = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
    }

    @Test
    public void testDeleteAndRollbackOnFailure() throws InterruptedException {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);
        //simulate the case in which we put data in n1 first
        //and then successfully delete it
        CheckedFuture<Void, ReadFailedException> putFuture = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);

        Thread.sleep(20);
        Assert.assertTrue(putFuture.isDone());
        Assert.assertEquals(1, internalDtxTestTransaction1.getTxDataSize(n0));

        CheckedFuture<Void, ReadFailedException> deleteFuture = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n1);

        Thread.sleep(20);
        Assert.assertTrue(deleteFuture.isDone());
        Assert.assertEquals("Size in n1 is wrong", 0, internalDtxTestTransaction1.getTxDataSize(n0));

    }

    @Test
    public void testDeleteAndRollbackOnFailureRollbackSucceed() throws InterruptedException {

        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        //simulate the case in which we successfully put the data in n1 and n2
        //but fail in deleting the data in n2 later
        //transaction fail but rollback succeed
        //at last no data in both n1 and n2

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        Thread.sleep(20);
        Assert.assertTrue(f1.isDone());
        Assert.assertTrue(f2.isDone());

//        Assert.assertEquals("Size of data in n1 is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
//        Assert.assertEquals("Size of data in 2 is wrong", 1, internalDtxTestTransaction2.getTxDataSize(n0));

        internalDtxTestTransaction2.setDeleteException(true); //delete action will fail
        f2 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n2);
        Thread.sleep(20);
        internalDtxTestTransaction2.setDeleteException(false); // after the delete action fail we must set the exception false to rollback successfully

        Thread.sleep(200);
        Assert.assertTrue(f2.isDone());
        try{
            f2.checkedGet();
            fail("Can't get the exception the test failed");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception the test failed", e instanceof ReadFailedException);
        }

        Assert.assertEquals("Size in n1 is wrong", 0, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size in n2 is wrong", 0, internalDtxTestTransaction2.getTxDataSize(n0));

    }
    @Test
    public void testDeleteAndRollbackOnFailureRollbackFail() throws InterruptedException {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);
        //simulate the case in which we successfully put the data in n1 and n2
        //but fail to delete the data in n2, dtx begin to rollback
        //when rollback the n2 submit exception occur and the rollback fail

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        Thread.sleep(20);
        Assert.assertTrue(f1.isDone());
        Assert.assertTrue(f2.isDone());


        internalDtxTestTransaction2.setDeleteException(true);
        internalDtxTestTransaction2.setSubmitException(true);
        f2 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n2);

        Thread.sleep(100);
        Assert.assertTrue(f2.isDone());
        try{
            f2.checkedGet();
            fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception the test failed", e instanceof ReadFailedException);
        }
//        Assert.assertEquals("Size of the data in transaction1 is wrong the test failed", 1, internalDtxTestTransaction1.getTxDataSize(n0));
//        Assert.assertEquals("Size of the data in transaction2 is wrong the test failed", 1, internalDtxTestTransaction2.getTxDataSize(n0));


    }

    @Test public void testPut()  {
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
    @Test public void testRollback() throws InterruptedException {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);
//      put data in both n1 and n2
//      rollback no data will exit in n1 and n2

        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        Thread.sleep(30);

        Assert.assertEquals(1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals(1,internalDtxTestTransaction2.getTxDataSize(n0));

        CheckedFuture<Void, DTxException.RollbackFailedException> f = dtxImpl.rollback();
        Thread.sleep(50);
        Assert.assertTrue(f.isDone());

        Assert.assertEquals("Size of n0's data in n1 is wrong after rollback", 0, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size of n0's data in n2 is wrong after rollback", 0, internalDtxTestTransaction2.getTxDataSize(n0));
    }
    @Test
    public void testRollbackFail() throws InterruptedException
    {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        //put data in n1 and n2
        //roll back fail
        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        Thread.sleep(30);

        internalDtxTestTransaction2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> f = dtxImpl.rollback();
        Thread.sleep(50);
        try
        {
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue(e instanceof DTxException.RollbackFailedException);
        }

    }
    @Test
    public void testSubmit() throws InterruptedException {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();

        Thread.sleep(50);
        Assert.assertTrue(f.isDone());
        try
        {
            f.checkedGet();
        }catch (Exception e)
        {
            fail("Get the unexpected Exception the test failed");
        }
    }
    @Test
    public void testSubmitFailRollbackSucceed() throws InterruptedException {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        //transaction2 submit fail
        //rollback succeed
        //submit will throw an exception

        internalDtxTestTransaction2.setSubmitException(true);
        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        Thread.sleep(20);
        Assert.assertTrue(f1.isDone());
        Assert.assertTrue(f2.isDone());

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        Thread.sleep(30);
        internalDtxTestTransaction2.setSubmitException(false); //no submit exception occur rollback will succeed

        Thread.sleep(150);
        Assert.assertTrue(f.isDone());

        try{
            f.checkedGet();
        }catch (Exception e)
        {
            //FIXME!!! RollbackSucceed and Fail should throw two diferent exception
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the TransactionCommitFailedException",cause.getClass().equals(DTxException.SubmitFailedException.class) );
        }
        Assert.assertEquals("n1 can't get rolledback", 0, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("n2 can't get rolledback", 0, internalDtxTestTransaction2.getTxDataSize(n0));
    }
    @Test
    public void testSubmitFailRollbackFail() throws InterruptedException
    {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        DtxImpl dtxImpl = new DtxImpl(new myTxProvider(), s);

        //put data in n1 and n2
        //submit fail
        //Rollback fail

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        Thread.sleep(20);
        Assert.assertTrue(f1.isDone());
        Assert.assertTrue(f2.isDone());

        internalDtxTestTransaction2.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();

        Thread.sleep(150);
        Assert.assertTrue(f.isDone());

        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException );
        }

    }
    @Test
    public void testSubmitCreateRollbackTxFail() throws InterruptedException {
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        myTxProvider txProvider = new myTxProvider();
        DtxImpl dtxImpl = new DtxImpl(txProvider, s);
        // TxProvider can't create the new rollback transaction
        //transaction fail
        //distributedSubmitedFuture will be set a SubmitFailedException
        //no rollback

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        txProvider.setCreateTxException(true);
        Thread.sleep(100);
        Assert.assertTrue(f.isDone());

        try{
            f.checkedGet();
            fail("Can't get the exception the test fail");
        } catch (Exception e) {
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue(cause.getClass().equals(DTxException.SubmitFailedException.class));
            System.out.println(((DTxException.SubmitFailedException)cause).getFailedSubmits());
        }
    }
}
