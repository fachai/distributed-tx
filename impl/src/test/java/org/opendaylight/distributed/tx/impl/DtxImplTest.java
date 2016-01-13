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
    DtxImpl dtxImpl;

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
        internalDtxTestTransaction1 = new DTXTestTransaction();
        internalDtxTestTransaction2 = new DTXTestTransaction();
        dtxImpl = new DtxImpl(new myTxProvider(), s);
    }

    @Before
    public void testConstructor() {
        testInit();
    }

    /**
     *simulate the case no data exist in node1 and node2, successfully put data in node1 and node2
     */
    @Test
    public void testPutAndRollbackOnFailure() {
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs ; i++) {
            CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
            try
            {
                f1.checkedGet();
            }catch (Exception e)
            {
                fail("get unexpected exception from the putAndRollbackOnFailure method");
            }

            CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
            try
            {
                f2.checkedGet();
            }catch (Exception e)
            {
                fail("get unexpected exception from the putAndRollbackOnFailure method");
            }
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("size of n1 data is wrong", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("size of n2 data is wrong", expectedDataSizeInTx2, internalDtxTestTransaction1.getTxDataSize(n0));
    }

    /**
     *simulate the case no data in both n1 and n2
     *then try to put data in n1 and n2
     *after successfully put data in n1, n2 hit error the transaction fail but rollback succeed
     *at last no data in both n1 and n2
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackSucceed() {
        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception when try to put data in transaction1");
        }

        internalDtxTestTransaction2.setReadException(true);
        CheckedFuture<Void, ReadFailedException> f = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        try{
            f.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof ReadFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in n1 is wrong", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in n2 is wrong", expectedDataSizeInTx2, internalDtxTestTransaction2.getTxDataSize(n0));
    }

    /**
     *simulate the case data exist in n1, but no data in n2
     *try to put data in n2, but hit error
     *rollback succeed, at last the original data is back to the n1 and no data exist in n2
     */
    @Test
    public void testPutAndRollbackOnFailureWithObjExRollbackSucceed() {
        internalDtxTestTransaction1.createObjForIdentifier(n0);

        internalDtxTestTransaction2.setReadException(true);
        CheckedFuture<Void, ReadFailedException> f = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        try{
            f.checkedGet();
            fail("Can't get the exception");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof ReadFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in n1 is wrong", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in n2 is wrong", expectedDataSizeInTx2, internalDtxTestTransaction2.getTxDataSize(n0));
    }

    /**
     *simulate the case in which no data in n1 and n2 at the beginning,
     *successfully put the data in n1
     *then try to put data in n2, but an exception occur and begin to rollback
     *when roll back in n2, submit exception occur and the rollback fail
     */
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackFail() {
        internalDtxTestTransaction2.setPutException(true);
        internalDtxTestTransaction2.setSubmitException(true); //submit fail the rollback will fail

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

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
            Assert.assertTrue("Can't get the expected ReadFailException, the test failed", e instanceof ReadFailedException);
        }
    }

    /**
     *simulate the case no data exist in n1 and n2 at the beginning
     *we successfully merge data in n1 and n2
     */
    @Test
    public void testMergeAndRollbackOnFailure()  {
        //FIXME! now treat the merge just as the put action, we should try to simulate the merge later
        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs ; i++) {
            CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
            CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
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
        Assert.assertEquals("size of n1 data is wrong", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("size of n2 data is wrong", expectedDataSizeInTx2, internalDtxTestTransaction1.getTxDataSize(n0));
    }

    /**
     *test the case in which no data exist in n1 and n2
     *successfully merge data in n1
     *when try to merge data in n2 hit error
     *rollback succeed and at last no data exist in n1 and n2
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackSucceed() {
        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxTestTransaction2.setMergeException(true);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        try{
            f2.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof ReadFailedException);
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in n1 is wrong", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in n2 is wrong", expectedDataSizeInTx2, internalDtxTestTransaction2.getTxDataSize(n0));
    }

    /**
     *test the case in which data exist in n1, but no data in n2
     *when try to merge data in n2, it hit error
     *rollback succeed and at last original data is back to n1 and no data in n2
     */
    @Test
    public void testMergeAndRollbackOnFailureWithObjExRollbackSucceed() {
        internalDtxTestTransaction1.createObjForIdentifier(n0);
        internalDtxTestTransaction2.setMergeException(true);
        CheckedFuture<Void, ReadFailedException> f = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        try{
            f.checkedGet();
            fail("Can't get the exception");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof ReadFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("Size of the identifier n0's data in n1 is wrong", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in n2 is wrong", expectedDataSizeInTx2, internalDtxTestTransaction2.getTxDataSize(n0));
    }

    /**
     *simulate the case no data exist in both n1 and n2
     * successfully merge data in n1, but hit error when merge data in n2
     * rollback meet submit error in transaction2, rollback fail too
     */
    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackFail() {
        internalDtxTestTransaction2.setMergeException(true);
        internalDtxTestTransaction2.setSubmitException(true); //submit fail the rollback will fail
        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
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
            Assert.assertTrue("Can't get the expected ReadFailException", e instanceof ReadFailedException);
        }
    }

    /**
     *simulate the case in which data exist in n1
     *we successfully delete it
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjEx() {
        internalDtxTestTransaction1.createObjForIdentifier(n0);
        CheckedFuture<Void, ReadFailedException> deleteFuture = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n1);

        try
        {
            deleteFuture.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        int expectedDataSizeInTx1 = 0;
        Assert.assertEquals("Size in n1 is wrong", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
    }

    /**
     *simulate the case in which data exist in n1 and n2
     *successfully delete data in n1, but fail in n2
     *transaction fail but rollback succeed
     *at last original data is back to n1 and n2
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackSucceed() {
        internalDtxTestTransaction1.createObjForIdentifier(n0);
        internalDtxTestTransaction2.createObjForIdentifier(n0);
        internalDtxTestTransaction2.setDeleteException(true); //delete action will fail

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception ");
        }

        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n2);
        try{
            f2.checkedGet();
            fail("Can't get the exception");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception ", e instanceof ReadFailedException);
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("Size in n1 is wrong", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size in n2 is wrong", expectedDataSizeInTx2, internalDtxTestTransaction2.getTxDataSize(n0));
    }

    /**
     *simulate the case in which data exist in n1 and n2
     *successfully delete data in n1 but fail to delete the data in n2, dtx begin to rollback
     *when rollback the n2 submit exception occur and the rollback fail
     */
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackFail() {
        internalDtxTestTransaction1.createObjForIdentifier(n0);
        internalDtxTestTransaction2.createObjForIdentifier(n0);

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n1);
        try
        {
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxTestTransaction2.setDeleteException(true);
        internalDtxTestTransaction2.setSubmitException(true);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n2);
        try{
            f2.checkedGet();
            fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception the test failed", e instanceof ReadFailedException);
        }

    }

    /**
     * test best effort put method
     */
    @Test public void testPut()  {
        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
    }

    /**
     * test best effort merge method
     */
    @Test public void testMerge() {
        dtxImpl.merge(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
    }

    /**
     * test best effort delete method
     */
    @Test public void testDelete() {
        dtxImpl.delete(LogicalDatastoreType.OPERATIONAL, n0, n1);
    }

    /**
     *put data in both n1 and n2
     *rollback successfully no data will exist in n1 and n2
     */
    @Test public void testRollback() {
        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        try
        {
            f1.checkedGet();
            f2.checkedGet();
            Assert.assertEquals(1, internalDtxTestTransaction1.getTxDataSize(n0));
            Assert.assertEquals(1, internalDtxTestTransaction2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("get unexpected exception from the put");
        }

        CheckedFuture<Void, DTxException.RollbackFailedException> f = dtxImpl.rollback();
        try{
            f.checkedGet();
            int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
            Assert.assertEquals("Size of n0's data in n1 is wrong after rollback", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
            Assert.assertEquals("Size of n0's data in n2 is wrong after rollback", expectedDataSizeInTx2, internalDtxTestTransaction2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("rollback get unexpected exception");
        }
    }

    /**
     *put data in n1 and n2
     *invoke rollback
     *when rollback n2 transaction, submit exception occur
     *rollback fail
     */
    @Test
    public void testRollbackFail()
    {
        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        try{
            f1.checkedGet();
            f2.checkedGet();
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }
        internalDtxTestTransaction2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> f = dtxImpl.rollback();
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
     *submit succeed case
     */
    @Test
    public void testSubmit() {
        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        try
        {
            f.checkedGet();
        }catch (Exception e)
        {
            fail("Get the unexpected Exception");
        }
    }

    /**
     *put data in n1 and n2, and submit
     *transaction2 submit fail
     *rollback succeed
     */
    @Test
    public void testSubmitWithoutObjExRollbackSucceed()  {
        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            Assert.assertEquals(1,internalDtxTestTransaction1.getTxDataSize(n0));
            Assert.assertEquals(1,internalDtxTestTransaction2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        internalDtxTestTransaction2.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            //RollbackSucceed and Fail should throw two diferent exception??
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the TransactionCommitFailedException",cause.getClass().equals(DTxException.SubmitFailedException.class) );
        }

        int expectedDataSizeInTx1 = 0, expectedDataSizeInTx2 = 0;
        Assert.assertEquals("n1 can't get rolledback", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("n2 can't get rolledback", expectedDataSizeInTx2, internalDtxTestTransaction2.getTxDataSize(n0));
    }

    /**
     *data exist in n1 and n2
     * successfully delete data in n1 and n2
     * invoke submit, n2 submit fail
     * rollback succeed
     */
    @Test
    public void testSubmitWithObjExRollbackSucceed() {
        internalDtxTestTransaction2.createObjForIdentifier(n0);
        internalDtxTestTransaction1.createObjForIdentifier(n0);
        internalDtxTestTransaction2.setSubmitException(true);

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            Assert.assertEquals(0,internalDtxTestTransaction1.getTxDataSize(n0));
            Assert.assertEquals(0,internalDtxTestTransaction2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            //RollbackSucceed and Fail should throw two different exception??
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the TransactionCommitFailedException",cause.getClass().equals(DTxException.SubmitFailedException.class) );
        }

        int expectedDataSizeInTx1 = 1, expectedDataSizeInTx2 = 1;
        Assert.assertEquals("n1 can't get rolledback", expectedDataSizeInTx1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("n2 can't get rolledback", expectedDataSizeInTx2, internalDtxTestTransaction2.getTxDataSize(n0));
    }

    /**
     *put data in n1 and n2
     *n2 submit fail and rollback but rollback hit delete exception and rollback fail
     */
    @Test
    public void testSubmitRollbackFail()
    {
        internalDtxTestTransaction2.setSubmitException(true);
        internalDtxTestTransaction2.setDeleteException(true);

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0,new TestClassNode(), n2);
        try
        {
            f1.checkedGet();
            f2.checkedGet();
            Assert.assertEquals(1,internalDtxTestTransaction1.getTxDataSize(n0));
            Assert.assertEquals(1,internalDtxTestTransaction2.getTxDataSize(n0));
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException );
        }
    }

    /**
     *TxProvider can't create the new rollback transaction
     *submit will fail
     *distributedSubmitedFuture will be set a SubmitFailedException
     *no rollback
     */
    @Test
    public void testSubmitCreateRollbackTxFail() {
        myTxProvider txProvider = new myTxProvider();
        DtxImpl dtxImpl = new DtxImpl(txProvider, s);

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        txProvider.setCreateTxException(true);

        try{
            f.checkedGet();
            fail("Can't get the exception the test fail");
        } catch (Exception e) {
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue(cause.getClass().equals(DTxException.SubmitFailedException.class));
        }
    }
}
