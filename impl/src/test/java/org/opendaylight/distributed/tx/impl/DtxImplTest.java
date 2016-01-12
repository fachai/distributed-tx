package org.opendaylight.distributed.tx.impl;

import com.google.common.util.concurrent.CheckedFuture;
import javassist.bytecode.stackmap.BasicBlock;
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

    @Test
    public void testPutAndRollbackOnFailure() throws InterruptedException {

        /*
        simulate the case no data exist in n1 and n2
         */
        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs ; i++) {

            CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
            CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        }

        Thread.sleep(30);

        Assert.assertEquals("size of n1 data is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("size of n2 data is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
    }
    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackSucceed() throws InterruptedException {

        /*
        simulate the case no data in both n1 and n2
        then try to put data in n1 and n2
        n2 hit error the transaction fail but rollback succeed
        at last no data in both n1 and n2
        */
        dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);

        Thread.sleep(30); //ensure data has write to n1 so we can check whether rollback succeed

        internalDtxTestTransaction2.setReadException(true);
        CheckedFuture<Void, ReadFailedException> f = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        Thread.sleep(20);
        internalDtxTestTransaction2.setReadException(false); // no read exception, rollback succeed


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
    public void testPutAndRollbackOnFailureWithObjExRollbackSucceed() throws InterruptedException{
       /*
       simulate the case data exist in n1
       try to put data in n2, but hit error
       rollback succeed, at last the original data is put in the n1 and no data exist n2
        */
        internalDtxTestTransaction1.createObjForIdentifier(n0);


        internalDtxTestTransaction2.setReadException(true);
        CheckedFuture<Void, ReadFailedException> f = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        Thread.sleep(20);
        internalDtxTestTransaction2.setReadException(false); // no read exception, rollback succeed

        try{
            f.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof ReadFailedException);
        }
        Assert.assertEquals("Size of the identifier n0's data in n1 is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in n2 is wrong", 0, internalDtxTestTransaction2.getTxDataSize(n0));
    }

    @Test
    public void testPutAndRollbackOnFailureWithoutObjExRollbackFail() throws InterruptedException {

        /*
        simulate the case in which we successfully put the data in n1
        then try to put data in n2, but an exception occur and begin to rollback
        when we roll back in n2, submit exception occur and the rollback fail
         */
        //FIXME the exception type check should be changed, because both rollback succeed and rollback fail share the same kind of ReadFailException

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
    public void testMergeAndRollbackOnFailure() throws InterruptedException {
        //FIXME! now treat the merge just as the put action, we should try to simulate the merge later
        /*
        simulate the case merge succeed
         */

        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs ; i++) {

            CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
            CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        }

        Thread.sleep(30);

        Assert.assertEquals("size of n1 data is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("size of n2 data is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
    }

    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackSucceed() throws InterruptedException {
        /*
        test the case in which no data exist in n1 and n2
        successfully merge data in n1
        when try to merge data in n2 hit error
        rollback succeed and at last no data exist in n1 and n2
         */

        dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);

        Thread.sleep(30); //ensure data write into n1, make the rollback successful

        internalDtxTestTransaction2.setMergeException(true);
        CheckedFuture<Void, ReadFailedException> f = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);


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
    public void testMergeAndRollbackOnFailureWithObjExRollbackSucceed() throws InterruptedException{
        /*
        test the case in which data exist in n1
        when try to merge data in n2 hit error
        rollback succeed and at last original data is put in n1 and no data in n2
         */
        internalDtxTestTransaction1.createObjForIdentifier(n0);

        internalDtxTestTransaction2.setMergeException(true);
        CheckedFuture<Void, ReadFailedException> f = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);

        try{
            f.checkedGet();
            fail("Can't get the exception, the test failed");
        }
        catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected exception", e instanceof ReadFailedException);
        }
        Assert.assertEquals("Size of the identifier n0's data in n1 is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size of the identifier n0's data in n2 is wrong", 0, internalDtxTestTransaction2.getTxDataSize(n0));



    }

    @Test
    public void testMergeAndRollbackOnFailureWithoutObjExRollbackFail() throws InterruptedException {

        /*
        simulate the case merge fail and rollback fail

         */

        internalDtxTestTransaction2.setMergeException(true);
        internalDtxTestTransaction2.setSubmitException(true); //submit fail the rollback will fail

        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.mergeAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        Thread.sleep(20);

        try{
            f1.checkedGet();
        }catch (Exception e)
        {
            fail("Transaction1 get the unexpected exception, the test has failed ");
        }


        try{
            f2.checkedGet();
            fail("Transaction2 can't get the exception, the test has failed");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the expected ReadFailException, the test failed", e instanceof ReadFailedException);
        }
    }

    @Test
    public void testDeleteAndRollbackOnFailureWithObjEx() throws InterruptedException {
        /*
        simulate the case in which we put data in n1 first
        and then successfully delete it
         */
       internalDtxTestTransaction1.createObjForIdentifier(n0);

        CheckedFuture<Void, ReadFailedException> deleteFuture = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n1);

        Thread.sleep(30);
        Assert.assertTrue(deleteFuture.isDone());
        Assert.assertEquals("Size in n1 is wrong", 0, internalDtxTestTransaction1.getTxDataSize(n0));

    }


    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackSucceed() throws InterruptedException {

        /*
        simulate the case in which data exist in n1 and n2
        successfully delete data in n2, but fail in n2
        transaction fail but rollback succeed
        at last original data is back to n1 and n2
         */

        internalDtxTestTransaction1.createObjForIdentifier(n0);
        internalDtxTestTransaction2.createObjForIdentifier(n0);

        internalDtxTestTransaction2.setDeleteException(true); //delete action will fail
        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n2);
        Thread.sleep(20);
        internalDtxTestTransaction2.setDeleteException(false); // after the delete action fail we must set the exception false to rollback successfully


        try{
            f2.checkedGet();
            fail("Can't get the exception the test failed");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the expected exception the test failed", e instanceof ReadFailedException);
        }

        Assert.assertEquals("Size in n1 is wrong", 1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("Size in n2 is wrong", 1, internalDtxTestTransaction2.getTxDataSize(n0));

    }
    @Test
    public void testDeleteAndRollbackOnFailureWithObjExRollbackFail() throws InterruptedException {

        /*
        simulate the case in which data exist in n1 and n2
        try to delete but fail to delete the data in n2, dtx begin to rollback
        when rollback the n2 submit exception occur and the rollback fail
         */
        internalDtxTestTransaction1.createObjForIdentifier(n0);
        internalDtxTestTransaction2.createObjForIdentifier(n0);

        dtxImpl.deleteAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, n1);

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

    @Test public void testPut()  {

        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);

    }
    @Test public void testMerge() {

        dtxImpl.merge(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
    }
    @Test public void testDelete() {

        dtxImpl.delete(LogicalDatastoreType.OPERATIONAL, n0, n1);
    }
    @Test public void testRollback() throws InterruptedException {
        /*
        put data in both n1 and n2
        rollback successfully no data will exist in n1 and n2
         */

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
        /*
        put data in n1 and n2
        roll back fail
         */
        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
        dtxImpl.put(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
        Thread.sleep(30);

        internalDtxTestTransaction2.setSubmitException(true);
        CheckedFuture<Void, DTxException.RollbackFailedException> f = dtxImpl.rollback();

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
        /*
        submit succeed case
         */

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();

        Thread.sleep(50);
        try
        {
            f.checkedGet();
        }catch (Exception e)
        {
            fail("Get the unexpected Exception the test failed");
        }
    }

    @Test
    public void testSubmitWithoutObjExRollbackSucceed() throws InterruptedException {

        /*
        transaction2 submit fail
        rollback succeed
        submit will throw an exception
         */

        internalDtxTestTransaction2.setSubmitException(true);
//        CheckedFuture<Void, ReadFailedException> f1 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n1);
//        CheckedFuture<Void, ReadFailedException> f2 = dtxImpl.putAndRollbackOnFailure(LogicalDatastoreType.OPERATIONAL, n0, new TestClassNode(), n2);
//        Thread.sleep(20);
//        Assert.assertTrue(f1.isDone());
//        Assert.assertTrue(f2.isDone());

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        Thread.sleep(30);
        internalDtxTestTransaction2.setSubmitException(false); //no submit exception occur rollback will succeed


        try{
            f.checkedGet();
        }catch (Exception e)
        {
            //RollbackSucceed and Fail should throw two diferent exception??
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the TransactionCommitFailedException",cause.getClass().equals(DTxException.SubmitFailedException.class) );
        }
        Assert.assertEquals("n1 can't get rolledback", 0, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("n2 can't get rolledback", 0, internalDtxTestTransaction2.getTxDataSize(n0));
    }
    @Test
    public void testSubmitWithObjExRollbackSucceed() throws InterruptedException {
        /*
        transaction2 submit fail
        rollback succeed
        submit will throw an exception
         */
        internalDtxTestTransaction2.createObjForIdentifier(n0);
        internalDtxTestTransaction1.createObjForIdentifier(n0);
        internalDtxTestTransaction2.setSubmitException(true);

        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();
        Thread.sleep(30);
        internalDtxTestTransaction2.setSubmitException(false); //no submit exception occur rollback will succeed


        try{
            f.checkedGet();
        }catch (Exception e)
        {
            //RollbackSucceed and Fail should throw two different exception??
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the TransactionCommitFailedException",cause.getClass().equals(DTxException.SubmitFailedException.class) );
        }
        Assert.assertEquals("n1 can't get rolledback", 1, internalDtxTestTransaction1.getTxDataSize(n0));
        Assert.assertEquals("n2 can't get rolledback", 1, internalDtxTestTransaction2.getTxDataSize(n0));

    }
    @Test
    public void testSubmitRollbackFail() throws InterruptedException
    {
        /*
        submit fail
        rollback fail
         */

        internalDtxTestTransaction2.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> f = dtxImpl.submit();

        try{
            f.checkedGet();
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailedException",e instanceof TransactionCommitFailedException );
        }

    }
    @Test
    public void testSubmitCreateRollbackTxFail() throws InterruptedException {
        /*
        TxProvider can't create the new rollback transaction
        transaction fail
        distributedSubmitedFuture will be set a SubmitFailedException
        no rollback
          */

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
            System.out.println(((DTxException.SubmitFailedException)cause).getFailedSubmits());
        }
    }
}
