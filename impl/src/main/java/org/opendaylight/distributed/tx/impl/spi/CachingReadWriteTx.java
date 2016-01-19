package org.opendaylight.distributed.tx.impl.spi;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.*;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.TransactionStatus;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.spi.CachedData;
import org.opendaylight.distributed.tx.spi.DTXReadWriteTransaction;
import org.opendaylight.distributed.tx.spi.TxCache;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.data.api.ModifyAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingReadWriteTx implements TxCache, DTXReadWriteTransaction, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CachingReadWriteTx.class);
    private final ReadWriteTransaction delegate;
    public final Deque<CachedData> cache = new ConcurrentLinkedDeque<>();
    private final ListeningExecutorService executorService;
    private final ExecutorService executorPoolPerCache;

    public CachingReadWriteTx(@Nonnull final ReadWriteTransaction delegate, ExecutorService executorPoolPerCache) {
        this.delegate = delegate;
        // this.executorPoolPerCache = executorPoolPerCache;
        this.executorPoolPerCache = Executors.newCachedThreadPool();
        this.executorService = MoreExecutors.listeningDecorator(this.executorPoolPerCache);
    }

    @Override public Iterator<CachedData> iterator() {
        return cache.descendingIterator();
    }

    @Override public <T extends DataObject> CheckedFuture<Optional<T>, ReadFailedException> read(
        final LogicalDatastoreType logicalDatastoreType, final InstanceIdentifier<T> instanceIdentifier) {
        return delegate.read(logicalDatastoreType, instanceIdentifier);
    }
    @Override public Object getIdentifier() {
        return delegate.getIdentifier();
    }

    @Override public void delete(final LogicalDatastoreType logicalDatastoreType,
        final InstanceIdentifier<?> instanceIdentifier) {
        /*  This is best effort API so that no exception will be thrown. */
        this.asyncDelete(logicalDatastoreType, instanceIdentifier);
    }

    public int getSizeOfCache(){
        return this.cache.size();
    }

    public CheckedFuture<Void, ReadFailedException> asyncDelete(final LogicalDatastoreType logicalDatastoreType,
                                      final InstanceIdentifier<?> instanceIdentifier) {
        @SuppressWarnings("unchecked")
        final CheckedFuture<Optional<DataObject>, ReadFailedException> readFuture = delegate
                .read(logicalDatastoreType, (InstanceIdentifier<DataObject>) instanceIdentifier);

        final SettableFuture<Void> retFuture = SettableFuture.create();

        Futures.addCallback(readFuture, new FutureCallback<Optional<DataObject>>() {
            @Override public void onSuccess(final Optional<DataObject> result) {
                cache.add(new CachedData(instanceIdentifier, result.get(), ModifyAction.DELETE));

                final ListenableFuture asyncPutFuture = executorService.submit(new Callable() {
                    @Override
                    public Object call() throws Exception {
                        delegate.delete(logicalDatastoreType, instanceIdentifier);
                        return null;
                    }
                });

                Futures.addCallback(asyncPutFuture, new FutureCallback() {
                    @Override
                    public void onSuccess(@Nullable Object result) {
                        retFuture.set(null);
                        LOG.info("asyncPut put done and return !!!!");
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        LOG.info("asyncPut put exception");
                        retFuture.setException(t);
                    }
                });
            }

            @Override public void onFailure(final Throwable t) {
                retFuture.setException(new DTxException.EditFailedException("failed to read from node in delete action", t));
            }
        });

        return Futures.makeChecked(retFuture, new Function<Exception, ReadFailedException>() {
            @Nullable
            @Override
            public ReadFailedException apply(@Nullable Exception e) {
                return new ReadFailedException("Asynchronous delete from cache failed ", e);
            }
        });
    }

    @Override public <T extends DataObject> void merge(final LogicalDatastoreType logicalDatastoreType,
        final InstanceIdentifier<T> instanceIdentifier, final T t) {
        this.asyncMerge(logicalDatastoreType, instanceIdentifier, t);
    }

    public <T extends DataObject> CheckedFuture<Void, ReadFailedException>asyncMerge(final LogicalDatastoreType logicalDatastoreType,
                                                       final InstanceIdentifier<T> instanceIdentifier, final T t) {
        final CheckedFuture<Optional<T>, ReadFailedException> readFuture = delegate
                .read(logicalDatastoreType, instanceIdentifier);

        final SettableFuture<Void> retFuture = SettableFuture.create();

        Futures.addCallback(readFuture, new FutureCallback<Optional<T>>() {
            @Override public void onSuccess(final Optional<T> result) {
                cache.add(new CachedData(instanceIdentifier, result.get(), ModifyAction.MERGE));

                final ListenableFuture asyncPutFuture = executorService.submit(new Callable() {
                    @Override
                    public Object call() throws Exception {
                        delegate.merge(logicalDatastoreType, instanceIdentifier, t);
                        return null;
                    }
                });

                Futures.addCallback(asyncPutFuture, new FutureCallback() {
                    @Override
                    public void onSuccess(@Nullable Object result) {
                        retFuture.set(null);
                        LOG.info("asyncPut device put done and return");
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        LOG.info("asyncPut device put exception");
                        retFuture.setException(t);
                    }
                });
            }

            @Override public void onFailure(final Throwable t) {
                retFuture.setException(new DTxException.EditFailedException("failed to read from node in merge action", t));
            }
        });

        return Futures.makeChecked(retFuture, new Function<Exception, ReadFailedException>() {
            @Nullable
            @Override
            public ReadFailedException apply(@Nullable Exception e) {
                return new ReadFailedException("cache merge failure", e);
            }
        });
    }

    @Deprecated
    @Override public <T extends DataObject> void merge(final LogicalDatastoreType logicalDatastoreType,
        final InstanceIdentifier<T> instanceIdentifier, final T t, final boolean b) {
        delegate.merge(logicalDatastoreType, instanceIdentifier, t, b);
        // TODO how to handle ensure parents ? we dont have control over that here, so we probably have to cache the whole subtree
        // Not support it.
    }

    @Override public <T extends DataObject> void put(final LogicalDatastoreType logicalDatastoreType,
        final InstanceIdentifier<T> instanceIdentifier, final T t) {
        this.asyncPut(logicalDatastoreType, instanceIdentifier, t);
    }

    public <T extends DataObject> CheckedFuture<Void, ReadFailedException> asyncPut(final LogicalDatastoreType logicalDatastoreType,
                                                     final InstanceIdentifier<T> instanceIdentifier, final T t) {
        final SettableFuture<Void> retFuture = SettableFuture.create();

        if(false){
            final CheckedFuture<Optional<T>, ReadFailedException> read = delegate
                    .read(logicalDatastoreType, instanceIdentifier);

            while(!read.isDone()){Thread.yield();};
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            delegate.put(logicalDatastoreType, instanceIdentifier, t);

            Runnable readResult = new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    retFuture.set(null);
                }
            };
            new Thread(readResult).start();
        }else{
            final CheckedFuture<Optional<T>, ReadFailedException> read = delegate
                    .read(logicalDatastoreType, instanceIdentifier);
            Futures.addCallback(read, new FutureCallback<Optional<T>>() {
                @Override
                public void onSuccess(final Optional<T> result) {
                    cache.add(new CachedData(instanceIdentifier, result.orNull(), ModifyAction.REPLACE));

                    final ListenableFuture asyncPutFuture = executorService.submit(new Callable() {
                        @Override
                        public Object call() throws Exception {
                            LOG.info("asyncPut put obj {}", Integer.toHexString(System.identityHashCode(t)));
                            delegate.put(logicalDatastoreType, instanceIdentifier, t);
                            return null;
                        }
                    });

                    Futures.addCallback(asyncPutFuture, new FutureCallback() {
                        @Override
                        public void onSuccess(@Nullable Object result) {
                            retFuture.set(null);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            retFuture.setException(t);
                        }
                    });
                }

                @Override
                public void onFailure(final Throwable t) {
                    retFuture.setException(new DTxException.EditFailedException("failed to read from node in put action", t));
                }
            });
        }

        return Futures.makeChecked(retFuture, new Function<Exception, ReadFailedException>() {
            @Nullable
            @Override
            public ReadFailedException apply(@Nullable Exception e) {
                return new ReadFailedException("Cache put failed", e);
            }
        });
    }

    @Deprecated
    @Override public <T extends DataObject> void put(final LogicalDatastoreType logicalDatastoreType,
        final InstanceIdentifier<T> instanceIdentifier, final T t, final boolean ensureParents) {
        delegate.put(logicalDatastoreType, instanceIdentifier, t, ensureParents);
    }

    @Override public boolean cancel() {
        return delegate.cancel();
    }

    @Override public CheckedFuture<Void, TransactionCommitFailedException> submit() {
        return delegate.submit();
    }

    @Deprecated
    @Override public ListenableFuture<RpcResult<TransactionStatus>> commit() {
        return delegate.commit();
    }

    @Override public void close() throws IOException {
        cancel();
        cache.clear();
    }
}
