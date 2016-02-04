package org.opendaylight.distributed.tx.impl;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.TransactionStatus;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class DTXTestTransaction implements ReadWriteTransaction {
    Map<InstanceIdentifier<?>, Boolean> readExceptionMap = new HashMap<>();
    Map<InstanceIdentifier<?>, Boolean> putExceptionMap = new HashMap<>();
    Map<InstanceIdentifier<?>, Boolean> mergeExceptionMap = new HashMap<>();
    Map<InstanceIdentifier<?>, Boolean> deleteExceptionMap = new HashMap<>();
    boolean submitException = false;

    private Map<InstanceIdentifier<?>,ArrayList<DataObject>> txDataMap = new HashMap<>();

    public void setReadException(InstanceIdentifier<?> instanceIdentifier, boolean ept){
        this.readExceptionMap.put(instanceIdentifier, ept);
    }
    public void setPutException(InstanceIdentifier<?> instanceIdentifier, boolean ept)
    {
        this.putExceptionMap.put(instanceIdentifier, ept);
    }
    public void setMergeException(InstanceIdentifier<?> instanceIdentifier, boolean ept)
    {
        this.mergeExceptionMap.put(instanceIdentifier, ept);
    }
    public void setDeleteException(InstanceIdentifier<?> instanceIdentifier, boolean ept)
    {
        this.deleteExceptionMap.put(instanceIdentifier, ept);
    }
    public void setSubmitException( boolean ept)
    {
        this.submitException = ept;
    }

    /**
     * this method is used to get the data size of the specific instanceIdentifier
     * @param instanceIdentifier the specific identifier
     * @return the size of data
     */
    public int getTxDataSize(InstanceIdentifier<?> instanceIdentifier) {
        return this.txDataMap.containsKey(instanceIdentifier)?
                this.txDataMap.get(instanceIdentifier).size() : 0;
    }

    /**
     * this method is used to manually create the data object for the specific instanceIdentifier
     * @param instanceIdentifier the specific identifier
     */
    public void createObjForIdentifier(InstanceIdentifier<?> instanceIdentifier)
    {
        txDataMap.put(instanceIdentifier, new ArrayList<DataObject>(Sets.newHashSet(new myDataObj())));
    }

    @Override
    public <T extends DataObject> CheckedFuture<Optional<T>, ReadFailedException> read(LogicalDatastoreType logicalDatastoreType, final InstanceIdentifier<T> instanceIdentifier) {
        T obj = null;

        if(txDataMap.containsKey(instanceIdentifier) && txDataMap.get(instanceIdentifier).size() > 0)
            obj = (T)txDataMap.get(instanceIdentifier).get(0);

        if(!readExceptionMap.containsKey(instanceIdentifier))
            readExceptionMap.put(instanceIdentifier, false);

        final Optional<T> retOpt = Optional.fromNullable(obj);

        final SettableFuture<Optional<T>> retFuture = SettableFuture.create();
        Runnable readResult = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!readExceptionMap.get(instanceIdentifier)){
                    retFuture.set(retOpt);
                }else {
                    setReadException(instanceIdentifier,false); //it ensure all the exception will occur once
                    retFuture.setException(new Throwable("simulated error"));
                }
                retFuture.notifyAll();
            }
        };

        new Thread(readResult).start();

        Function<Exception, ReadFailedException> f = new Function<Exception, ReadFailedException>() {
            @Nullable
            @Override
            public ReadFailedException apply(@Nullable Exception e) {
                return new ReadFailedException("Merge failed and rollback", e);
            }
        };

        return Futures.makeChecked(retFuture, f);
    }

    @Override
    public <T extends DataObject> void put(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<T> instanceIdentifier, T t) {
       if(!txDataMap.containsKey(instanceIdentifier))
           txDataMap.put(instanceIdentifier, new ArrayList<DataObject>());

       if(!putExceptionMap.containsKey(instanceIdentifier))
           putExceptionMap.put(instanceIdentifier,false);

       if(!putExceptionMap.get(instanceIdentifier)) {
           txDataMap.get(instanceIdentifier).clear();
           txDataMap.get(instanceIdentifier).add(t);
       }
        else {
           setPutException(instanceIdentifier, false);
           throw new RuntimeException(" put exception");
       }
    }

    @Override
    public <T extends DataObject> void put(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<T> instanceIdentifier, T t, boolean b) {
        if(!txDataMap.containsKey(instanceIdentifier))
            txDataMap.put(instanceIdentifier, new ArrayList<DataObject>());

        if(!putExceptionMap.containsKey(instanceIdentifier))
            putExceptionMap.put(instanceIdentifier,false);

        if(!putExceptionMap.get(instanceIdentifier))
        {
            txDataMap.get(instanceIdentifier).clear();
            txDataMap.get(instanceIdentifier).add(t);
        }
        else {
            setPutException(instanceIdentifier,false);
            throw new RuntimeException("put exception");
        }
    }

    @Override
    public <T extends DataObject> void merge(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<T> instanceIdentifier, T t) {
        if(!txDataMap.containsKey(instanceIdentifier))
            txDataMap.put(instanceIdentifier, new ArrayList<DataObject>());

        if(!mergeExceptionMap.containsKey(instanceIdentifier))
            mergeExceptionMap.put(instanceIdentifier, false);

        if(!mergeExceptionMap.get(instanceIdentifier)) {
            txDataMap.get(instanceIdentifier).clear();
            txDataMap.get(instanceIdentifier).add(t);
        }
        else {
            setMergeException(instanceIdentifier,false);
            throw new RuntimeException(" merge exception");
        }
    }

    @Override
    public <T extends DataObject> void merge(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<T> instanceIdentifier, T t, boolean b) {
        if(!txDataMap.containsKey(instanceIdentifier))
            txDataMap.put(instanceIdentifier, new ArrayList<DataObject>());

        if(!mergeExceptionMap.containsKey(instanceIdentifier))
            mergeExceptionMap.put(instanceIdentifier, false);

        if(!mergeExceptionMap.get(instanceIdentifier)) {
            txDataMap.get(instanceIdentifier).clear();
            txDataMap.get(instanceIdentifier).add(t);
        }
        else {
            setMergeException(instanceIdentifier,false);
            throw new RuntimeException("merge exception");
        }
    }

    @Override
    public boolean cancel() {
        return false;
    }

    @Override
    public void delete(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<?> instanceIdentifier) {
        if(!txDataMap.containsKey(instanceIdentifier))
            return;

        if(!deleteExceptionMap.containsKey(instanceIdentifier))
            deleteExceptionMap.put(instanceIdentifier, false);

        if(!deleteExceptionMap.get(instanceIdentifier))
            if(txDataMap.get(instanceIdentifier).size()>0)
                txDataMap.get(instanceIdentifier).clear();
            else {
                setDeleteException(instanceIdentifier,false);
                return;
            }
        else {
            setDeleteException(instanceIdentifier,false);
            throw new RuntimeException(" delete exception");
        }
    }

    @Override
    public CheckedFuture<Void, TransactionCommitFailedException> submit() {
        final SettableFuture<Void> retFuture = SettableFuture.create();

        final Runnable submitResult = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!submitException){
                  retFuture.set(null);
                }else {
                    setSubmitException(false);
                    retFuture.setException(new RuntimeException("submit error"));
                }
                retFuture.notifyAll();
            }
        };

        new Thread(submitResult).start();

        Function<Exception, TransactionCommitFailedException> f = new Function<Exception, TransactionCommitFailedException>() {
            @Nullable
            @Override
            public TransactionCommitFailedException apply(@Nullable Exception e) {
                return new TransactionCommitFailedException("submit failed", e);
            }
        };

        return Futures.makeChecked(retFuture, f);
    }

    @Override
    public ListenableFuture<RpcResult<TransactionStatus>> commit() {
        return null;
    }

    @Override
    public Object getIdentifier() {
        return null;
    }

    public static class myDataObj implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return myDataObj.class;
        }
    }
}
