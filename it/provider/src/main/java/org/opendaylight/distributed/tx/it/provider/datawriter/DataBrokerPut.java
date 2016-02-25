package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.*;
import org.opendaylight.controller.md.sal.binding.api.BindingTransactionChain;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionChain;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DatastoreTestData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterListKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by sunny on 16-2-25.
 */
public class DataBrokerPut extends AbstractDataStoreWriter {
    private DataBroker dataBroker;
    private static final Logger LOG = LoggerFactory.getLogger(DataBrokerPut.class);

    public DataBrokerPut(BenchmarkTestInput input, DataBroker db,int outerElements, int innerElements)
    {
        super(input, outerElements, innerElements);
        this.dataBroker = db;
    }
    @Override
    public ListenableFuture<Void> writeData() {
        final SettableFuture<Void> setFuture = SettableFuture.create();
        long putsPerTx = input.getPutsPerTx();

        List<List<InnerList>> innerLists = buildInnerList();

        final BindingTransactionChain transactionChain = dataBroker.createTransactionChain(this);
        WriteTransaction tx = transactionChain.newWriteOnlyTransaction();

        startTime = System.nanoTime();
        int counter = 0;
        for (int i = 0; i < outerElements ; i++) {
            for (InnerList innerList : innerLists.get(i)) {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, new OuterListKey(i))
                        .child(InnerList.class, innerList.getKey());

                tx.put(LogicalDatastoreType.CONFIGURATION, innerIid, innerList);
                counter++;

                if (counter == putsPerTx) {
                    Futures.addCallback(tx.submit(), new FutureCallback<Void>() {
                        @Override
                        public void onSuccess(@Nullable Void aVoid) {

                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            //test will fail
                            setFuture.setException(throwable);
                        }
                    });
                    counter = 0;
                    tx = transactionChain.newWriteOnlyTransaction();
                }
            }
        }

            // *** Clean up and close the transaction chain ***
            // Submit the outstanding transaction even if it's empty and wait for it to finish
            // We need to empty the transaction chain before closing it

            CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = tx.submit();
            Futures.addCallback(restSubmitFuture, new FutureCallback<Void>() {
                @Override
                public void onSuccess(@Nullable Void aVoid) {
                    setFuture.set(null);
                    LOG.info("Transactions: submitted completed");
                    endTime = System.nanoTime();
                    try {
                        transactionChain.close();
                    }
                    catch (IllegalStateException e){
                        LOG.info("Can't close the transaction chain");
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    setFuture.setException(throwable);
                    try {
                        transactionChain.close();
                    }
                    catch (IllegalStateException e){
                        LOG.info("Can't close the transaction chain");
                    }
                }

            });
        return setFuture;
    }

    @Override
    public void onTransactionChainFailed(TransactionChain<?, ?> chain,
                                         AsyncTransaction<?, ?> transaction, Throwable cause) {
        LOG.error("Broken chain {} in DataBrokerPut, transaction {}, cause {}",
                chain, transaction.getIdentifier(), cause);
    }

    @Override
    public void onTransactionChainSuccessful(TransactionChain<?, ?> chain) {
        LOG.info("DataBrokerPut closed successfully, chain {}", chain);
    }
}
