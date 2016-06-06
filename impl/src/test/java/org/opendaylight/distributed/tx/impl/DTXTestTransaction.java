package org.opendaylight.distributed.tx.impl;

import com.google.common.base.Function;
import com.google.common.base.Optional;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class DTXTestTransaction implements ReadWriteTransaction {
    Map<InstanceIdentifier<?>, Boolean> readExceptionMap = new ConcurrentHashMap<>();
    Map<InstanceIdentifier<?>, Boolean> putExceptionMap = new ConcurrentHashMap<>();
    Map<InstanceIdentifier<?>, Boolean> mergeExceptionMap = new ConcurrentHashMap<>();
    Map<InstanceIdentifier<?>, Boolean> deleteExceptionMap = new ConcurrentHashMap<>();
    boolean  submitException = false;
    static int delayTime = 50;

    private Map<InstanceIdentifier<?>,ConcurrentLinkedDeque<DataObject>> txDataMap = new ConcurrentHashMap<>();

    public void setReadExceptionByIid(InstanceIdentifier<?> instanceIdentifier, boolean ept){
        this.readExceptionMap.put(instanceIdentifier, ept);
    }
    public void setPutExceptionByIid(InstanceIdentifier<?> instanceIdentifier, boolean ept)
    {
        this.putExceptionMap.put(instanceIdentifier, ept);
    }
    public void setMergeExceptionByIid(InstanceIdentifier<?> instanceIdentifier, boolean ept)
    {
        this.mergeExceptionMap.put(instanceIdentifier, ept);
    }
    public void setDeleteExceptionByIid(InstanceIdentifier<?> instanceIdentifier, boolean ept)
    {
        this.deleteExceptionMap.put(instanceIdentifier, ept);
    }
    public void setSubmitException( boolean ept)
    {
        this.submitException = ept;
    }

    public void addInstanceIdentifiers(InstanceIdentifier<?>... iids){
        for (InstanceIdentifier<?> iid : iids) {
            this.readExceptionMap.put(iid, false);
            this.deleteExceptionMap.put(iid, false);
            this.putExceptionMap.put(iid, false);
            this.mergeExceptionMap.put(iid, false);
            this.txDataMap.put(iid, new ConcurrentLinkedDeque<DataObject>());
        }
    }


    /**
     * get the data size of the specific data IID
     * @param instanceIdentifier the specific data IID
     * @return size of data for the specific IID
     */
    public int getTxDataSizeByIid(InstanceIdentifier<?> instanceIdentifier) {
        return this.txDataMap.get(instanceIdentifier).size();
    }

    /**
     * create the data object for the specific data IID
     * @param instanceIdentifier the specific IID
     */
    public void createObjForIdentifier(InstanceIdentifier<?> instanceIdentifier)
    {
        txDataMap.get(instanceIdentifier).add(new myDataObj());
    }

    @Override
    public <T extends DataObject> CheckedFuture<Optional<T>, ReadFailedException> read(LogicalDatastoreType logicalDatastoreType, final InstanceIdentifier<T> instanceIdentifier) {
        T obj = null;

        if(txDataMap.get(instanceIdentifier).size() > 0)
            obj = (T)txDataMap.get(instanceIdentifier).getFirst();

        final Optional<T> retOpt = Optional.fromNullable(obj);

        final SettableFuture<Optional<T>> retFuture = SettableFuture.create();
        Runnable readResult = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(delayTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!readExceptionMap.get(instanceIdentifier)){
                    retFuture.set(retOpt);
                }else {
                    //ensure the read exception for the IID just occur once
                    setReadExceptionByIid(instanceIdentifier,false);
                    retFuture.setException(new Throwable("read error"));
                }
                retFuture.notifyAll();
            }
        };

        new Thread(readResult).start();

        Function<Exception, ReadFailedException> f = new Function<Exception, ReadFailedException>() {
            @Nullable
            @Override
            public ReadFailedException apply(@Nullable Exception e) {
                return new ReadFailedException("Read failed", e);
            }
        };

        return Futures.makeChecked(retFuture, f);
    }

    @Override
    public <T extends DataObject> void put(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<T> instanceIdentifier, T t) {
        if(!putExceptionMap.get(instanceIdentifier)) {
               txDataMap.get(instanceIdentifier).clear();
               txDataMap.get(instanceIdentifier).add(t);
        }
        else {
           setPutExceptionByIid(instanceIdentifier, false);
           throw new RuntimeException(" put exception");
       }
    }

    @Override
    public <T extends DataObject> void put(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<T> instanceIdentifier, T t, boolean b) {
        if(!putExceptionMap.get(instanceIdentifier)) {
                txDataMap.get(instanceIdentifier).clear();
                txDataMap.get(instanceIdentifier).add(t);
        }
        else {
            setPutExceptionByIid(instanceIdentifier,false);
            throw new RuntimeException("put exception");
        }
    }

    @Override
    public <T extends DataObject> void merge(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<T> instanceIdentifier, T t) {
        if(!mergeExceptionMap.get(instanceIdentifier)) {
                txDataMap.get(instanceIdentifier).clear();
                txDataMap.get(instanceIdentifier).add(t);
        }
        else {
            setMergeExceptionByIid(instanceIdentifier,false);
            throw new RuntimeException(" merge exception");
        }
    }

    @Override
    public <T extends DataObject> void merge(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<T> instanceIdentifier, T t, boolean b) {
        if(!mergeExceptionMap.get(instanceIdentifier)) {
                txDataMap.get(instanceIdentifier).clear();
                txDataMap.get(instanceIdentifier).add(t);
        }
        else {
            setMergeExceptionByIid(instanceIdentifier,false);
            throw new RuntimeException("merge exception");
        }
    }

    @Override
    public boolean cancel() {
        return false;
    }

    @Override
    public void delete(LogicalDatastoreType logicalDatastoreType, InstanceIdentifier<?> instanceIdentifier) {
        if(!deleteExceptionMap.get(instanceIdentifier)) {
            if (txDataMap.get(instanceIdentifier).size() > 0)
                txDataMap.get(instanceIdentifier).clear();
        }
        else {
            setDeleteExceptionByIid(instanceIdentifier,false);
            throw new RuntimeException("delete exception");
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
