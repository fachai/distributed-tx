/*
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
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
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import static org.junit.Assert.fail;

public class CachingReadWriteTxTest {
    DTXTestTransaction testTx;
    InstanceIdentifier<DTXTestTransaction.myDataObj> n0 = InstanceIdentifier.create(DTXTestTransaction.myDataObj.class);
    @Before
    public void testInit(){
        this.testTx = new DTXTestTransaction();
        testTx.addInstanceIdentifiers(n0);
    }

    /**
     * test the constructor
     */
    @Test
    public void testConstructor() {
       new CachingReadWriteTx(testTx);
    }

    /**
     * test Read method, successfully read
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
     * test Read method, read failed due to the exception
     */
    @Test
    public void testReadFailWithObjEx()
    {
        testTx.createObjForIdentifier(n0);
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.setReadExceptionByIid(n0, true);

        CheckedFuture<Optional<DTXTestTransaction.myDataObj>, ReadFailedException> readResult = cacheRWTx.read(LogicalDatastoreType.OPERATIONAL, n0);
        try{
            readResult.checkedGet();
            fail("can't get the exception from the transaction");
        }catch (Exception e)
        {
            Assert.assertTrue("type of exception is wrong", e instanceof ReadFailedException);
        }
    }

    /**
     * test AsyncPut method,successfully put
     * no data exists in the data IID at the beginning
     * successfully put several times to data IID n0
     * ensure the size of cached data and size of data in IID is correct
     */
    @Test
    public void testAsyncPutWithoutObjEx() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
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
        Assert.assertEquals("size in DtxTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test AsyncPut method, successfully put
     * data exists in the data IID at the beginning
     * successfully put several times to the same data IID
     * ensure the size of cached data and size of data in the tx is correct
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
        Assert.assertEquals("size in DtxTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test AsyncPut method, read error
     * no data exists in the data IID n0 at the beginning
     * put several times to that IID, every time read error occurs
     * ensure no data cached in the tx and no data is put into the tx
     * get DTxException.ReadFailedException from the putFuture each time
     */
    @Test
    public void testAsyncPutWithoutObjExReadError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs; i++ ) {
            testTx.setReadExceptionByIid(n0,true);
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
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test AsyncPut method, write error
     * no data exists in the data IID n0 at the beginning
     * put several times to that IID, every time put error occurs
     * ensure the size of cached data in the tx and the data in the tx is correct
     * get DTxException from the putFuture
     */
    @Test
    public void testAsyncPutWithoutObjExWriteError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for (int i = 0; i < numberOfObjs; i++ ) {
            testTx.setPutExceptionByIid(n0,true);
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
        Assert.assertEquals("Size of the data in Dtxtransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test AsyncMerge method, successfully merge
     * no data exists in the data IID n0 at the beginning
     * merge several times to that IID, each merge is successful
     * ensure the size of cached data in the tx and the data in the tx is correct
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
        Assert.assertEquals("Size of the cached data is wrong", numberOfObjs, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test AsyncMerge method, successfully merge
     * data exists in the data IID n0 at the beginning
     * merge several times to that IID, each merge is successful
     * ensure the size of cached data in the tx and the data in the tx is correct
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
        Assert.assertEquals("size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test AsyncMerge method,read error
     * no data exists in the data IID n0 at the beginning
     * merge several times to that IID, every time read error occurs
     * ensure the size of cached data in the tx and the data in the tx is correct
     * get DTxException.ReadFailedException from the mergeFuture
     */
    @Test
    public void testAsyncMergeWithoutObjExReadError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for(int i = 0; i < numberOfObjs; i++)
        {
            testTx.setReadExceptionByIid(n0,true);
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
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0) );
    }

    /**
     * test AsyncMerge method, write error
     * no data exists in the data IID n0 at the beginning
     * merge several times to that IID, every time merge error occurs
     * ensure the size of cached data in the tx and the data in the tx is correct
     * get DTxException from the mergeFuture
     */
    @Test
    public void testAsyncMergeWithoutObjExWriteError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);

        int numberOfObjs = (int)(Math.random()*10) + 1;
        for(int i = 0; i < numberOfObjs; i++)
        {
            testTx.setMergeExceptionByIid(n0,true);
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
        Assert.assertEquals("The size of the data in the DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0) );
    }

    /**
     * test AsyncDelete method, successfully delete
     * data exists in the data IID n0 at the beginning
     * successfully delete the data in that IID
     * ensure the size of cached data in the tx and the data in the tx is correct
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
        Assert.assertEquals("Size of cached data in cacheRWTx is wrong", expectedCacheDataSize, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size of the data in DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test AsyncDelete method, read error
     * data exists in the data IID n0 at the beginning
     * delete the data in that IID, read error occur
     * ensure the size of cached data in the tx and the data in the tx is correct
     * get DTxException.ReadFailedException from the delete future
     */
    @Test
    public void testAsyncDeleteWithObjExReadError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(n0);
        testTx.setReadExceptionByIid(n0,true);

        CheckedFuture<Void, DTxException> f = cacheRWTx.asyncDelete(LogicalDatastoreType.OPERATIONAL, n0);

        try{
                f.checkedGet();
                fail("Can't get the exception the test failed");
        }catch (Exception e)
        {
                Assert.assertTrue("Can't get the EditFailedException the test failed", e instanceof DTxException.ReadFailedException);
        }

        int expectedDataSizeInTx = 1, expectedCacheDataSize = 0;
        Assert.assertEquals("Size of cached data CachingReadWriteTx is wrong", expectedCacheDataSize, cacheRWTx.getSizeOfCache());
        Assert.assertEquals("Size in the DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test AsyncDelete method, write error
     * data exists in the data IID n0 at the beginning
     * delete the data in that IID, write error occur
     * ensure the size of cached data in the tx and the data in the tx is correct
     * get DTxException from the delete future
     */
    @Test
    public void testAsyncDeleteWithObjExWriteError() {
        CachingReadWriteTx cacheRWTx = new CachingReadWriteTx(testTx);
        testTx.createObjForIdentifier(n0);
        testTx.setDeleteExceptionByIid(n0,true);

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
        Assert.assertEquals("Size of the data in the DTXTestTransaction is wrong", expectedDataSizeInTx, testTx.getTxDataSizeByIid(n0));
    }

    /**
     * test submit method, successfully submit
     * no exception got
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
     * test submit method, submit failed with exception
     * get the TransactionCommitFailedException from the submit future
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
