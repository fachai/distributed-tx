package org.opendaylight.distributed.tx.impl;

import com.google.common.util.concurrent.CheckedFuture;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.CachingReadWriteTx;
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
    public void testAsyncPutWithoutObjEx() throws InterruptedException {
        /* FIXME The case should test right read after read in DTXTestTransaction is fixed. */

        /*
        test the case no data exist at the beginning and successfully AsyncPut
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, instanceIdentifier, new DTXTestTransaction.myDataObj());

            Thread.sleep(30);
            Assert.assertTrue(cf.isDone());
        }

        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs);
        Assert.assertEquals("size in DtxTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncPutWithObjEx() throws InterruptedException{
        /*
         test the case data exist at the begining and successfully AsyncPut
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(instanceIdentifier);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, instanceIdentifier, new DTXTestTransaction.myDataObj());

            Thread.sleep(30);
            Assert.assertTrue(cf.isDone());
        }

        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs);
        Assert.assertEquals("size in DtxTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncPutWithoutObjExReadError() throws InterruptedException {
        /*
        test the case when try to AsyncPut read error occur and put failed
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        testTx.setReadException(true);

        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs; i++ ) {

            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, instanceIdentifier, new DTXTestTransaction.myDataObj());

            try {
                cf.checkedGet();
                fail("Can't get the exception, the test has failed");
            } catch (Exception e) {
                 /*FIXME!! now just test the type of the original exception in both readError and writeError case, exception of two cases should be different?  */
                Throwable cause = e.getCause().getCause();
                Assert.assertTrue("Can't get the EditFailedException the test failed", cause instanceof DTxException.EditFailedException);
            }

        }
        System.out.println("Size of the ReadErrorCase is " + numberOfObjs);
        Assert.assertEquals("Size in cacheRWTx is wrong", 0, cacheRWTx.getSizeOfCache() );
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncPutWithoutObjExWriteError() throws InterruptedException {
        /*
        test the case try to AsyncPut put error occur and put failed
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        testTx.setPutException(true);
        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs; i++ ) {

            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            try {
                cf.checkedGet();
                fail("Can't get the exception, the test failed");
            } catch (Exception e) {

                Throwable cause = e.getCause().getCause();
                Assert.assertTrue("Can't get the RuntimeException the test failed", cause.getClass().equals(RuntimeException.class));
            }

        }
        System.out.println("Size of the WriteErrorCase is " + numberOfObjs);
        Assert.assertEquals("Size of cached data in cachingReadWriteTx is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in Dtxtransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier));
    }



    @Test
    public void testAsyncMergeWithoutObjEx() throws InterruptedException {
        /*
        test the case no data exist at the beginning successfully Asyncmerge
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(30);
            Assert.assertTrue(cf.isDone());
        }

        System.out.println("size is "+cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), 10);
        Assert.assertEquals("size of the data in DTXTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncMergeWithObjEx() throws InterruptedException{
        /*
        test the case data exist at the beginning successfully Asyncmerge
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(instanceIdentifier);

        int numberOfObjs = 10;
        //AsyncMerge succeed case
        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(30);
            Assert.assertTrue(cf.isDone());
        }

        System.out.println("size is "+cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), 10);
        Assert.assertEquals("size of the data in DTXTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncMergeWithoutObjExReadError() throws InterruptedException {
        /*
        try to Async merge but read error occur and merge failed
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.setReadException(true);

        int numberOfObjs = 10;


        for(int i = 0; i < numberOfObjs; i++)
        {
            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            try
            {
                cf.checkedGet();
                fail("Can't get the exception the test failed");
            }catch(Exception e)
            {
                Throwable cause = e.getCause().getCause();
                Assert.assertTrue("Can't get the expected exception the test failed", cause instanceof DTxException.EditFailedException);
            }
        }

        Assert.assertEquals("The size of cached data in the CachingReadWriteTransaction is wrong", 0, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier) );

    }

    @Test
    public void testAsyncMergeWithoutObjExWriteError() throws InterruptedException {
        /*
        test the case try to Async merge, write error occur and test fail
        */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.setMergeException(true);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++)
        {
            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            try
            {
                cf.checkedGet();
                fail("Can't get the exception the test failed");
            }catch(Exception e)
            {
                Throwable cause = e.getCause().getCause();
                Assert.assertTrue("Can't get the expected exception the test failed", cause.getClass().equals(RuntimeException.class));
            }
        }
        Assert.assertEquals("The size of cached data in the CachingReadWriteTransaction is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier) );
    }

    @Test
    public  void testAsyncDeleteWithoutObjEx() throws InterruptedException {
        /*
        test the case no data exist at the beginning and we put a data in it and then successfully delete it
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        Thread.sleep(30);
        Assert.assertTrue(cf.isDone());

        CheckedFuture<Void, ReadFailedException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        Thread.sleep(30);
        Assert.assertEquals(f.isDone(), true);

        Assert.assertEquals("Size in cacheRWTx is wrong", 2, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncDeleteWithObjEx() throws InterruptedException{
        /*
        test the case data exist at the beginning, successfully delete it
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(instanceIdentifier);

        CheckedFuture<Void, ReadFailedException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        Thread.sleep(30);
        Assert.assertEquals(f.isDone(), true);

        Assert.assertEquals("Size in cacheRWTx is wrong", 1, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", 0, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncDeleteWithObjExReadError() throws InterruptedException {
        /*
        test the case data exist at the beginning, try to Async delete the data
        read error occur delete fail
         */
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        testTx.createObjForIdentifier(instanceIdentifier);
        testTx.setReadException(true);

        CheckedFuture<Void, ReadFailedException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));

        try{
                f.checkedGet();
                fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
                Throwable cause = e.getCause().getCause();
                Assert.assertTrue("Can't get the EditFailedException the test failed", cause instanceof DTxException.EditFailedException);
        }
        Assert.assertEquals("Size in CachingReadWriteTx is wrong", 0, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size in the DTXTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));
    }

    @Test
    public void testAsyncDeleteWithObjExWriteError() throws InterruptedException {
        /*
        test the case data exist at the beginning, try to delete it successfully read and delete error occur
        delete fail
         */
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(instanceIdentifier);

        testTx.setDeleteException(true);

        CheckedFuture<Void, ReadFailedException> f2 = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        try{
                f2.checkedGet();
                fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
            Throwable cause = e.getCause().getCause();
            Assert.assertTrue("Can't get the RuntimeException the test failed", cause.getClass().equals(RuntimeException.class));
        }

        Assert.assertEquals("Size of the caching data in CachingReadWriteTx is wrong", 1, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in the DTXTestTransaction is wrong", 1, testTx.getTxDataSize(instanceIdentifier));

    }

    @Test
    public void testMerge() throws InterruptedException {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            cacheRWTx.merge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        Thread.sleep(30);
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), 10);
    }

    @Test
    public void testPut() throws InterruptedException {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            cacheRWTx.put(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }
        Thread.sleep(30);
        Assert.assertEquals("Size is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());

    }

    @Test
    public void testDelete() throws InterruptedException {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        int numberOfDeleted = 5;

        for(int i = 0; i < numberOfDeleted; i++){
            cacheRWTx.delete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        }
        Thread.sleep(30);

        Assert.assertEquals("Size is wrong", numberOfObjs + numberOfDeleted, cacheRWTx.getSizeOfCache());
    }

    @Test
    public void testSubmitSucceed(){
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
