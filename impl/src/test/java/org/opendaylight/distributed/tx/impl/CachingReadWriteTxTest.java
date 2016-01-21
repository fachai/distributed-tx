package org.opendaylight.distributed.tx.impl;

import com.google.common.util.concurrent.CheckedFuture;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.CachingReadWriteTx;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import org.junit.Assert;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CachingReadWriteTxTest {
    DTXTestTransaction testTx;
    private final ExecutorService executorPoolPerCache = Executors.newCachedThreadPool();

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

        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, DTxException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(15);
            Assert.assertEquals(cf.isDone(), true);
        }
        System.out.println("size is "+cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs);
    }
    @Test
    public void testAsyncPutReadError() throws InterruptedException {
    }

    @Test
    public void testAsyncPutWriteError() throws InterruptedException {
    }

    @Test
    public void testAsyncMerge() throws InterruptedException {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(new DTXTestTransaction());

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, DTxException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());

            Thread.sleep(15);
            Assert.assertEquals(cf.isDone(), true);
        }

        System.out.println("size is "+cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), 10);
    }

    @Test
    public  void testAsyncDelete() throws InterruptedException {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(new DTXTestTransaction());

        int numberOfObjs = 10;

        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, DTxException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        int numberOfDeleted = 5;

        for(int i = 0; i < numberOfDeleted; i++){
            CheckedFuture<Void, DTxException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
            Thread.sleep(15);
            Assert.assertEquals(f.isDone(), true);
        }

        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs + numberOfDeleted);
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
            CheckedFuture<Void, DTxException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
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
