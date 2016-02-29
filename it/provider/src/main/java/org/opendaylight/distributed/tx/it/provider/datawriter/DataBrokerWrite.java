package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import javassist.runtime.Inner;
import org.opendaylight.controller.md.sal.binding.api.BindingTransactionChain;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.*;
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
public class DataBrokerWrite extends AbstractDataStoreWriter implements TransactionChainListener{
    private DataBroker dataBroker;
    private static final Logger LOG = LoggerFactory.getLogger(DataBrokerWrite.class);
    private List<ListenableFuture<Void>> submitFutures = Lists.newArrayList();

    public DataBrokerWrite(BenchmarkTestInput input, DataBroker db, int outerElements, int innerElements)
    {
        super(input, db, outerElements, innerElements);
        this.dataBroker = db;
    }
    @Override
    public ListenableFuture<Void> writeData() {
        final SettableFuture<Void> setFuture = SettableFuture.create();
        long putsPerTx = input.getPutsPerTx();

        //when the operation is delete we should build the test data first
        if (input.getOperation() == BenchmarkTestInput.Operation.DELETE)
        {
            boolean buildTestData = build();//build the test data for the operation
            if (!buildTestData)
            {
                setFuture.setException(new Throwable("can't build the test data for the delete operation"));
                return setFuture;
            }
        }

        BindingTransactionChain chain = dataBroker.createTransactionChain(this);
        WriteTransaction tx = chain.newWriteOnlyTransaction();

        List<List<InnerList>> innerLists = buildInnerLists();
        startTime = System.nanoTime();
        long counter = 0;
        for (int i = 0; i < outerElements ; i++) {
            for (InnerList innerList : innerLists.get(i)) {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, new OuterListKey(i))
                        .child(InnerList.class, innerList.getKey());
                if (input.getOperation() == BenchmarkTestInput.Operation.PUT) {
                    tx.put(LogicalDatastoreType.CONFIGURATION, innerIid, innerList);
                }else if (input.getOperation() == BenchmarkTestInput.Operation.MERGE){
                    tx.merge(LogicalDatastoreType.CONFIGURATION, innerIid, innerList);
                }else {
                    tx.delete(LogicalDatastoreType.CONFIGURATION, innerIid);
                }
                counter++;

                if (counter == putsPerTx) {
                    CheckedFuture<Void, TransactionCommitFailedException> submitFut = tx.submit();
                    Futures.addCallback(submitFut, new FutureCallback<Void>() {
                        @Override
                        public void onSuccess(@Nullable Void aVoid) {

                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            testFail = true;
                        }
                    });
                    counter = 0;
                    tx = chain.newWriteOnlyTransaction();
                }
            }
        }
        /**
         * Clean up and close the transaction chain
         * Submit the outstanding transaction even if it's empty and wait for it to finish
         * We need to empty the transaction chain before closing it
         */
        CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = tx.submit();

        try
        {
            restSubmitFuture.checkedGet();
            if (testFail)
            {
                setFuture.setException(new Throwable("test fail"));
                return setFuture;
            }
            endTime = System.nanoTime();
            setFuture.set(null);

        }catch (Exception e)
        {
            setFuture.setException(e);
        }finally {
            try{
                chain.close();;
            }catch (Exception chainCloseException)
            {
                LOG.info("Can't close the transaction chain");
            }
        }
        return setFuture;
    }

    @Override
    public void onTransactionChainFailed(TransactionChain<?, ?> transactionChain, AsyncTransaction<?, ?> asyncTransaction, Throwable throwable) {

    }

    @Override
    public void onTransactionChainSuccessful(TransactionChain<?, ?> transactionChain) {

    }
}
