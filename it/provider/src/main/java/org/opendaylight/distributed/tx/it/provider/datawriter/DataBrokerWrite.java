package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.opendaylight.controller.md.sal.binding.api.BindingTransactionChain;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.*;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DatastoreTestData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by sunny on 16-2-25.
 * this class is use to test the writing performance of the databroker
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
    public void writeData() {

        long putsPerTx = input.getPutsPerTx();

        //when the operation is delete we should build the test data first
        if (input.getOperation() == BenchmarkTestInput.Operation.DELETE)
        {
            boolean buildTestData = build();//build the test data for the operation
            if (!buildTestData)
            {
                return;
            }
        }

        BindingTransactionChain chain = dataBroker.createTransactionChain(this);
        WriteTransaction tx = chain.newWriteOnlyTransaction();

        List<OuterList> outerLists = buildOuterList(outerElements, innerElements);
        startTime = System.nanoTime();
        long counter = 0;
        for ( OuterList outerList : outerLists ) {
            for (InnerList innerList : outerList.getInnerList()) {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, outerList.getKey())
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
                            txSucceed++;
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            txError++;
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
            txSucceed++;
            endTime = System.nanoTime();

        }catch (Exception e)
        {
            txError++;
        }finally {
            try{
                chain.close();;
            }catch (Exception chainCloseException)
            {
                LOG.info("Can't close the transaction chain");
            }
        }

    }

    @Override
    public void onTransactionChainFailed(TransactionChain<?, ?> transactionChain, AsyncTransaction<?, ?> asyncTransaction, Throwable throwable) {

    }

    @Override
    public void onTransactionChainSuccessful(TransactionChain<?, ?> transactionChain) {

    }
}
