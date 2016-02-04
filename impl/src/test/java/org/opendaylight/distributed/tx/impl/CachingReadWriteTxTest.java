package org.opendaylight.distributed.tx.impl;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.CheckedFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.CachingReadWriteTx;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import static org.junit.Assert.fail;

public class CachingReadWriteTxTest {
    DTXTestTransaction testTx;
    InstanceIdentifier<DTXTestTransaction.myDataObj> n0 = InstanceIdentifier.create(DTXTestTransaction.myDataObj.class);
    @Before
    public void testInit(){
        this.testTx = new DTXTestTransaction();
    }

    /**
     * test the constructor of the cachingReadWriteTx
     */
    @Test
    public void testConstructor() {
       new CachingReadWriteTx(testTx);
    }

    /**
     * test data exist in n0, successfully read the data
     */
    @Test
    public void testReadWithObjEx()
    {
        testTx.createObjForIdentifier(n0);
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        Optional<DTXTestTransaction.myDataObj> readData = Optional.absent();

        CheckedFuture<Optional<DTXTestTransaction.myDataObj>, ReadFailedException> readResult = cacheRWTx.read(LogicalDatastoreType.OPERATIONAL, n0);
        try{
            readData = readResult.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception from the read method");
        }

        Assert.assertTrue("Can't read from the transaction", readData.isPresent());
    }

    /**
     * test data exist in n0, when read the data, exception occur
     */
    @Test
    public void testReadFailWithObjEx()
    {
        testTx.createObjForIdentifier(n0);
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.setReadException(n0, true);
        Optional<DTXTestTransaction.myDataObj> readData = Optional.absent();

        CheckedFuture<Optional<DTXTestTransaction.myDataObj>, ReadFailedException> readResult = cacheRWTx.read(LogicalDatastoreType.OPERATIONAL, n0);
        try{
            readData = readResult.checkedGet();
            fail("can't get the exception from the transaction");
        }catch (Exception e)
        {
            Assert.assertTrue("type of exception is wrong", e instanceof ReadFailedException);
        }
    }

    /**
     *test the case no data exist at the beginning and successfully AsyncPut
     */
    @Test
    public void testAsyncPutWithoutObjEx() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1; //make sure at least one obj is put in the transaction
        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, DTxException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, n0, new DTXTestTransaction.myDataObj());
            try{
                cf.checkedGet();
            }catch (Exception e)
            {
                fail("get unexpected exception from the asyncPut");
            }
        }

        int expectedDataSizeInTx = 1;
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs);
        Assert.assertEquals("size in DtxTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case data exist at the beginning and successfully AsyncPut
     */
    @Test
    public void testAsyncPutWithObjEx() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(n0);

        int numberOfObjs = (int)(Math.random()*10) + 1;;
        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, DTxException> cf =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, n0, new DTXTestTransaction.myDataObj());
            try{
                cf.checkedGet();
            }catch (Exception e)
            {
                fail("get unexpected exception from the asyncPut");
            }
        }

        int expectedDataSizeInTx = 1;
        Assert.assertEquals("size is wrong", cacheRWTx.getSizeOfCache(), numberOfObjs);
        Assert.assertEquals("size in DtxTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case try to AsyncPut, but read error occur and put failed
     */
    @Test
    public void testAsyncPutWithoutObjExReadError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs; i++ ) {
            testTx.setReadException(n0,true);
            CheckedFuture<Void, DTxException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, n0, new DTXTestTransaction.myDataObj());
            try {
                cf.checkedGet();
                fail("Can't get the exception, the test has failed");
            } catch (Exception e) {
                Assert.assertTrue("Can't get the EditFailedException the test failed", e instanceof DTxException.ReadFailedException);
            }
        }

        int expectedDataSizeInTx = 0, expectedCacheDataSize = 0;
        Assert.assertEquals("Size in cacheRWTx is wrong", expectedCacheDataSize, cacheRWTx.getSizeOfCache() );
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case try to AsyncPut, put error occur and put failed
     */
    @Test
    public void testAsyncPutWithoutObjExWriteError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs; i++ ) {
            testTx.setPutException(n0,true);
            CheckedFuture<Void, DTxException> cf = cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, n0, new DTXTestTransaction.myDataObj());
            try {
                cf.checkedGet();
                fail("Can't get the exception, the test failed");
            } catch (Exception e) {
                Assert.assertTrue("Can't get the RuntimeException the test failed", e instanceof DTxException);
            }
        }

        int expectedDataSizeInTx = 0;
        Assert.assertEquals("Size of cached data in cachingReadWriteTx is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in Dtxtransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case no data exist at the beginning, successfully AsyncMerge object
     */
    @Test
    public void testAsyncMergeWithoutObjEx() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, DTxException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
            try{
                cf.checkedGet();
            }catch (Exception e)
            {
                fail("get unexpected exception from the asyncMerge");
            }
        }

        int expectedDataSizeInTx = 1;
        Assert.assertEquals("size is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case data exist at the beginning and successfully AsyncMerge data into it
     */
    @Test
    public void testAsyncMergeWithObjEx() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(n0);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for(int i = 0; i < numberOfObjs; i++){
            CheckedFuture<Void, DTxException> cf =  cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
            try{
                cf.checkedGet();
            }catch (Exception e)
            {
                fail("get unexpected exception from the asyncMerge");
            }
        }

        int expectedDataSizeInTx = 1;
        Assert.assertEquals("size is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case try to Async merge but read error occur and merge failed
     */
    @Test
    public void testAsyncMergeWithoutObjExReadError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for(int i = 0; i < numberOfObjs; i++)
        {
            testTx.setReadException(n0,true);
            CheckedFuture<Void, DTxException> cf = cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, n0, new DTXTestTransaction.myDataObj());
            try
            {
                cf.checkedGet();
                fail("Can't get the exception the test failed");
            }catch(Exception e)
            {
                Assert.assertTrue("Can't get the expected exception the test failed", e instanceof DTxException.ReadFailedException);
            }
        }

        int expectedDataSizeInTx = 0, expectedCacheDataSize = 0;
        Assert.assertEquals("The size of cached data in the CachingReadWriteTransaction is wrong", expectedCacheDataSize, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0) );
    }

    /**
     *test the case try to Async merge, write error occur and test fail
     */
    @Test
    public void testAsyncMergeWithoutObjExWriteError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for(int i = 0; i < numberOfObjs; i++)
        {
            testTx.setMergeException(n0,true);
            CheckedFuture<Void, DTxException> cf = cacheRWTx.asyncMerge(LogicalDatastoreType.OPERATIONAL, n0, new DTXTestTransaction.myDataObj());
            try
            {
                cf.checkedGet();
                fail("Can't get the exception the test failed");
            }catch(Exception e)
            {
                Assert.assertTrue("Can't get the expected exception the test failed", e instanceof DTxException);
            }
        }

        int expectedDataSizeInTx = 0;
        Assert.assertEquals("The size of cached data in the CachingReadWriteTransaction is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0) );
    }

    /**
     *test the case no data exist at the beginning and we put a data in it and then successfully AsyncDelete it
     */
    @Test
    public  void testAsyncDeleteWithoutObjEx() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        CheckedFuture<Void, DTxException> f1 =  cacheRWTx.asyncPut(LogicalDatastoreType.OPERATIONAL, n0, new DTXTestTransaction.myDataObj());
        CheckedFuture<Void, DTxException> f2 = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, n0);
        try
        {
             f1.checkedGet();
             f2.checkedGet();
        }catch (Exception e)
        {
             fail("get the unexpected exception from the AsyncDelete method, test fail");
        }

        int expectedDataSizeInTx = 0, expectedCacheDataSize = 2;
        Assert.assertEquals("Size in cacheRWTx is wrong", expectedCacheDataSize, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case data exist at the beginning, successfully AsyncDelete it
     */
    @Test
    public void testAsyncDeleteWithObjEx() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(n0);

        CheckedFuture<Void, DTxException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, n0);
        try
        {
           f.checkedGet();
        }catch (Exception e)
        {
            fail("get the unexpected exception the test failed");
        }

        int expectedDataSizeInTx = 0, expectedCacheDataSize = 1;
        Assert.assertEquals("Size in cacheRWTx is wrong", expectedCacheDataSize, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case data exist at the beginning, try to AsyncDelete the data
     *read error occur and delete fail
     */
    @Test
    public void testAsyncDeleteWithObjExReadError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(n0);
        testTx.setReadException(n0,true);

        CheckedFuture<Void, DTxException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, n0);

        try{
                f.checkedGet();
                fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
                Assert.assertTrue("Can't get the EditFailedException the test failed", e instanceof DTxException.ReadFailedException);
        }

        int expectedDataSizeInTx = 1, expectedCacheDataSize = 0;
        Assert.assertEquals("Size in CachingReadWriteTx is wrong", expectedCacheDataSize, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size in the DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     *test the case data exist at the beginning, successfully read but delete error occur when try to AsyncDelete it
     *delete fail
     */
    @Test
    public void testAsyncDeleteWithObjExWriteError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(n0);
        testTx.setDeleteException(n0,true);

        CheckedFuture<Void, DTxException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, n0);
        try
        {
            f.checkedGet();
            fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
            Assert.assertTrue("Can't get the RuntimeException the test failed", e instanceof DTxException);
        }

        int expectedDataSizeInTx = 1, expectedCacheDataSize = 1;
        Assert.assertEquals("Size of the caching data in CachingReadWriteTx is wrong", expectedCacheDataSize, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in the DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSize(n0));
    }

    /**
     * test the best effort merge method
     */
    @Test
    public void testMerge() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;;
        for(int i = 0; i < numberOfObjs; i++){
            cacheRWTx.merge(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }
    }

    /**
     * test the best effort put method
     */
    @Test
    public void testPut() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;;
        for(int i = 0; i < numberOfObjs; i++){
            cacheRWTx.put(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }
    }

    /**
     *test best effort delete method
     */
    @Test
    public void testDelete() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;;
        for(int i = 0; i < numberOfObjs; i++){
            cacheRWTx.put(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class), new DTXTestTransaction.myDataObj());
        }

        int numberOfDeleted = numberOfObjs - 1;
        for(int i = 0; i < numberOfDeleted; i++){
            cacheRWTx.delete(LogicalDatastoreType.OPERATIONAL, InstanceIdentifier.create(DTXTestTransaction.myDataObj.class));
        }
    }

    /**
     * test the case in which submit succeed and no exception occur
     */
    @Test
    public void testSubmitSucceed(){
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        CheckedFuture<Void, TransactionCommitFailedException> cf = cacheRWTx.submit();
        try {
            cf.checkedGet();
        } catch (TransactionCommitFailedException e) {
            fail("submit fail cause get the unexpected exception");
        }
    }

    /**
     * test the case in which submit failed
     */
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
}
