package org.opendaylight.distributed.tx.impl;

import com.google.common.util.concurrent.CheckedFuture;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.CachingReadWriteTx;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import org.junit.Assert;

import static org.junit.Assert.fail;

public class CachingReadWriteTxTest {
    DTXTestTransaction testTx;
    InstanceIdentifier<DTXTestTransaction.myDataObj> instanceIdentifier = InstanceIdentifier.create(DTXTestTransaction.myDataObj.class);

    @Before
    public void testInit(){ this.testTx = new DTXTestTransaction(); }
    @Test
    public void testConstructor() {
        new CachingReadWriteTx(new DTXTestTransaction());
    }

    @Test
    public void testAsyncPut() throws InterruptedException {
        /* FIXME The case should test right read after read in DTXTestTransaction is fixed. */
        // testTx.setReadException(true);
        DTXTestTransaction testTx = new DTXTestTransaction();

        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, instanceIdentifier, new DTXTestTransaction.myDataObj());

            Thread.sleep(25);
            Assert.assertEquals(cf.isDone(), true);
        }

        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs);
        Assert.assertEquals("size in DtxTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncPutReadError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();

        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        // read fail case
        testTx.setReadException(true);

        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs; i++ ) {

            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, instanceIdentifier, new DTXTestTransaction.myDataObj());

            Thread.sleep(20);
            Assert.assertEquals(cf.isDone(), true);

            try {
                cf.checkedGet();
                fail("Can't get the exception, the test has failed");
            } catch (Exception e) {
                //When we set an exception in SettableFuture the exception will be wrapped by an ExecutionException
                Throwable cause = e.getCause().getCause();
                Assert.assertTrue("Can't get the EditFailedException the test failed", cause instanceof DTxException.EditFailedException);
            }

        }
        System.out.println("Size of the ReadErrorCase is " + numberOfObjs);
        Assert.assertEquals("Size in cacheRWTx is wrong", 0, cacheRWTx.getSizeOfCache() );
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncPutWriteError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();

        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        //read success write fail case
        testTx.setPutException(true);
        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs; i++ ) {

            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(20);
            Assert.assertEquals(cf.isDone(), true);

            try {
                cf.checkedGet();
                fail("Can't get the exception, the test failed");
            } catch (Exception e) {
                /*FIXME!! now just test the type of the original exception in both readError and writeError case, exception of two cases should be different?  */
                Throwable cause = e.getCause().getCause();
                Assert.assertTrue("Can't get the RuntimeException the test failed", cause.getClass().equals(RuntimeException.class));
            }

        }
        System.out.println("Size of the WriteErrorCase is " + numberOfObjs);
        Assert.assertEquals("Size of cached data in cachingReadWriteTx is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in Dtxtransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier));
    }



    @Test
    public void testAsyncMerge() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(20);
            Assert.assertEquals(cf.isDone(), true);
        }

        System.out.println("size is "+cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), 10);
        Assert.assertEquals("size of the data in DTXTestTransaction is wrong", numberOfObjs, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncMergeReadError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        testTx.setReadException(true);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++)
        {
            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(20);
            Assert.assertEquals(cf.isDone(), true);

            try
            {
                cf.checkedGet();
                fail("Can't get the exception the test failed");
            }catch(Exception e)
            {
                Assert.assertTrue("Can't get the expected exception the test failed", e instanceof ReadFailedException);
            }
        }

        Assert.assertEquals("The size of cached data in the CachingReadWriteTransaction is wrong", 0, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier) );

    }

    @Test
    public void testAsyncMergeWriteError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        testTx.setMergeException(true);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++)
        {
            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(20);
            Assert.assertEquals(cf.isDone(), true);

            try
            {
                cf.checkedGet();
                fail("Can't get the exception the test failed");
            }catch(Exception e)
            {
                Assert.assertTrue("Can't get the expected exception the test failed", e instanceof ReadFailedException);
            }
        }

        Assert.assertEquals("The size of cached data in the CachingReadWriteTransaction is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier) );
    }

    @Test
    public  void testAsyncDelete() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        Thread.sleep(20);
        Assert.assertTrue(cf.isDone());

        CheckedFuture<Void, ReadFailedException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        Thread.sleep(20);
        Assert.assertEquals(f.isDone(), true);

        Assert.assertEquals("Size in cacheRWTx is wrong", 2, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncDeleteReadError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        Thread.sleep(20); //we must ensure all the put action has been ended???
        Assert.assertTrue(cf.isDone());

        testTx.setReadException(true);

        CheckedFuture<Void, ReadFailedException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        Thread.sleep(20);

        Assert.assertEquals(cf.isDone(), true);

        try{
                f.checkedGet();
                fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
                Throwable cause = e.getCause().getCause();
                Assert.assertTrue("Can't get the EditFailedException the test failed", cause instanceof DTxException.EditFailedException);
        }


        Assert.assertEquals("Size in CachingReadWriteTx is wrong", 1, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size in the DTXTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncDeleteWriteError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        CheckedFuture<Void, ReadFailedException> f1 = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        Thread.sleep(20);
        Assert.assertTrue(f1.isDone());

        testTx.setDeleteException(true);

        CheckedFuture<Void, ReadFailedException> f2 = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        Thread.sleep(20);

        Assert.assertEquals(f2.isDone(), true);

        try{
                f2.checkedGet();
                fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the RuntimeException the test failed", cause.getClass().equals(RuntimeException.class));
        }

        Assert.assertEquals("Size of the caching data in CachingReadWriteTx is wrong", 2, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in the DTXTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));

    }

    @Test
    public void testMerge(){
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(new DTXTestTransaction());

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            cacheRWTx.merge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }
        // Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs);
    }
    @Test
    public void testPut() throws InterruptedException {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(new DTXTestTransaction());

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            cacheRWTx.put(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }
        Thread.sleep(20);

        Assert.assertEquals("Size is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());

    }

    @Test
    public void testDelete() throws InterruptedException {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(new DTXTestTransaction());

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        int numberOfDeleted = 5;

        for(int i = 0; i < numberOfDeleted; i++){
            cacheRWTx.delete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        }
        Thread.sleep(20);

        Assert.assertEquals("Size is wrong", numberOfObjs + numberOfDeleted, cacheRWTx.getSizeOfCache());
    }

    @Test
    public void testSubmitSucceed(){
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        CheckedFuture<Void, TransactionCommitFailedException> cf = cacheRWTx.submit();

        try {
            cf.checkedGet();
        } catch (TransactionCommitFailedException e) {
            fail();
        }
    }

    @Test
    public void testSubmitFail() {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.setSubmitException(true);
        CheckedFuture<Void, TransactionCommitFailedException> cf = cacheRWTx.submit();

        try
        {
            cf.checkedGet();
            fail("Can't get the exception, the test failed");
        }catch(Exception e)
        {
            Assert.assertTrue("Can't get the TransactionCommitFailException, the test failed", e instanceof TransactionCommitFailedException);
        }

    }
    @Test
    public void testConcurrentAsyncPut(){
        // This is the test routine of concurrent asyncPut
    }

    @Test
    public void testConcurrentAsyncMerge(){
        // This is the test routine of concurrent asyncMerge
    }

    @Test
    public void testConcurrentAsyncDelete(){
        // This is the test routine of concurrent asyncMerge
    }

    @Test
    public void testAsyncPutWithException(){
        // FIXME
    }

}
