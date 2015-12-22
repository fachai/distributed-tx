package org.opendaylight.distributed.tx.impl;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.matchers.InstanceOf;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.TransactionStatus;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.CachingReadWriteTx;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;

import javax.annotation.Nullable;
import org.junit.Assert;

import static org.junit.Assert.fail;

public class CachingReadWriteTxTest {
    DTXTestTransaction testTx;

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
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(20);
            Assert.assertEquals(cf.isDone(), true);
        }
        System.out.println("size is "+cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs);
        System.out.println("size of data in DtxTestTransaction is " + testTx.getTxDataSize());
        Assert.assertEquals("The size in DtxTestTransaction is wrong", testTx.getTxDataSize(), numberOfObjs);
    }

    @Test
    public void testAsyncPutReadError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();

        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        //simulate the read fail case
        //an ReadFailedException nesting the EditFailedException is expected

        int numberOfObjs = 10;


        for (int i = 0; i < numberOfObjs; i++ ) {

            testTx.setReadException(true);

            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(20);
            Assert.assertEquals(cf.isDone(), true);

            try {
                cf.checkedGet();
                fail("Can't get the exception, the test has failed");
            } catch (Exception e) {
                Assert.assertTrue("Can't get the excepted Exception the test failed", e instanceof ReadFailedException);
            }

        }
        System.out.println("The Size of the ReadErrorCase is " + numberOfObjs);
        Assert.assertEquals("The size is wrong", 0, cacheRWTx.getSizeOfCache() );


    }

    @Test
    public void testAsyncPutWriteError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();

        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        //simulate the Read success Write fail case

        int numberOfObjs = 10;


        for (int i = 0; i < numberOfObjs; i++ ) {

            testTx.setPutException(true);

            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(20);
            Assert.assertEquals(cf.isDone(), true);

            try {
                cf.checkedGet();
                fail("Can't get the exception, the test has failed");
            } catch (Exception e) {
                Assert.assertTrue("Can't get the excepted Exception the test failed", e instanceof ReadFailedException);
            }

        }
        System.out.println("The Size of the WriteErrorCase is " + numberOfObjs);
        Assert.assertEquals("The size of cached data in cachingReadWriteTx is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("The size of the number in Dtxtransaction is wrong", 0, testTx.getTxDataSize());


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
        Assert.assertEquals("size of the data in DTXTestTransaction is wrong", numberOfObjs, testTx.getTxDataSize());
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
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", 0, testTx.getTxDataSize() );

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
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", 0, testTx.getTxDataSize() );
    }

    @Test
    public  void testAsyncDelete() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        int numberOfDeleted = 5;

        for(int i = 0; i < numberOfDeleted; i++){
            CheckedFuture<Void, ReadFailedException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
            Thread.sleep(20);
            Assert.assertEquals(f.isDone(), true);
        }

        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs + numberOfDeleted);
        Assert.assertEquals("size of the data in DTXTestTransaction is wrong", testTx.getTxDataSize(), numberOfObjs - numberOfDeleted);
    }

    @Test
    public void testAsyncDeleteReadError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs; i++)
        {
            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        Thread.sleep(20); //we must ensure all the Merge action has been ended???

        int numberOfReadErrorDeleted = 5;

        testTx.setReadException(true);

        for (int i = 0; i < numberOfReadErrorDeleted; i++) {

            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
            Thread.sleep(20);

            Assert.assertEquals(cf.isDone(), true);

            try{
                cf.checkedGet();
                fail("Can't get the exception the test failed");
            }catch (Exception e)
            {
                Assert.assertTrue("Can't get the expected exception the test failed", e instanceof ReadFailedException);
            }
        }

        Assert.assertEquals("size of the caching data in CachingReadWriteTx is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size of the data in the DTXTestTransaction is wrong", numberOfObjs, testTx.getTxDataSize());
    }

    @Test
    public void testAsyncDeleteWriteError() throws InterruptedException {
        DTXTestTransaction testTx = new DTXTestTransaction();

        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for (int i = 0; i < numberOfObjs; i++)
        {
            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        Thread.sleep(20); //we must ensure all the Merge action has been ended???

        int numberOfWriteErrorDeleted = 5;

        testTx.setDeleteException(true);

        for (int i = 0; i < numberOfWriteErrorDeleted ; i++) {
            CheckedFuture<Void, ReadFailedException> cf = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
            Thread.sleep(20);

            Assert.assertEquals(cf.isDone(), true);

            try{
                cf.checkedGet();
                fail("Can't get the exception the test failed");
            }catch (Exception e)
            {
                Assert.assertTrue("Can't get the expected exception the test failed", e instanceof ReadFailedException);
            }
        }

        Assert.assertEquals("size of the caching data in CachingReadWriteTx is wrong", numberOfObjs + numberOfWriteErrorDeleted, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size of the data in the DTXTestTransaction is wrong", numberOfObjs, testTx.getTxDataSize());

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
    public void testPut(){
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(new DTXTestTransaction());

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            cacheRWTx.put(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }
    }

    @Test
    public void testDelete(){
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(new DTXTestTransaction());

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, ReadFailedException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        int numberOfDeleted = 5;

        for(int i = 0; i < numberOfDeleted; i++){
            cacheRWTx.delete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
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
