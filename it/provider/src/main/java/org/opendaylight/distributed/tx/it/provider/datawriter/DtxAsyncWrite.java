package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.base.Function;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.api.DTxProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DatastoreTestData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DtxAsyncWrite extends AbstractDataStoreWriter {
    private DTx dtx;
    private DTxProvider dTxProvider;
    private static final Logger LOG = LoggerFactory.getLogger(DtxAsyncWrite.class);
    private Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap;

    public DtxAsyncWrite(BenchmarkTestInput input, DTxProvider dTxProvider, DataBroker dataBroker, Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap, int outerElements, int innerElements)
    {
        super(input, dataBroker, outerElements,innerElements);
        this.dTxProvider = dTxProvider;
        this.nodesMap = nodesMap;
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

        InstanceIdentifier<DatastoreTestData> nodeId = InstanceIdentifier.create(DatastoreTestData.class);
        List<ListenableFuture<Void>> putFutures = new ArrayList<ListenableFuture<Void>>((int) putsPerTx); //store all the put futures

        int counter = 0;
        List<OuterList> outerLists = buildOuterList(outerElements, innerElements);
        dtx = dTxProvider.newTx(nodesMap);
        startTime = System.nanoTime();
        for ( OuterList outerList : outerLists ) {
            for (InnerList innerList : outerList.getInnerList() ) {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, outerList.getKey())
                        .child(InnerList.class, innerList.getKey());

                CheckedFuture<Void, DTxException> writeFuture;
                if (input.getOperation() == BenchmarkTestInput.Operation.PUT) {
                    writeFuture = dtx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, innerList, nodeId);
                }else if (input.getOperation() == BenchmarkTestInput.Operation.MERGE){
                    writeFuture = dtx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, innerList, nodeId);
                }else{
                    writeFuture = dtx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, nodeId);
                }
                putFutures.add(writeFuture);
                counter++;

                if (counter == putsPerTx)
                {
                    //aggregate all the put futures into a listenable future, this future can make sure all the Async write has finish caching the data
                    ListenableFuture<Void> aggregatePutFuture = Futures.transform(Futures.allAsList(putFutures), new Function<List<Void>, Void>() {
                        @Nullable
                        @Override
                        public Void apply(@Nullable List<Void> voids) {
                            return null;
                        }
                    });

                    try{
                        aggregatePutFuture.get();
                        CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dtx.submit();
                        try{
                            submitFuture.checkedGet();
                            txSucceed++;
                        }catch (TransactionCommitFailedException e)
                        {
                            LOG.info("DTX Async submit failed");
                            txError++;
                        }
                    }catch (Exception e)
                    {
                        LOG.info("DTX Async put failed");
                        txError++;
                        dtx.cancel();
                    }

                    counter = 0;
                    dtx = dTxProvider.newTx(nodesMap); //after each submit we should get new Dtx
                    putFutures = new ArrayList<ListenableFuture<Void>>((int) putsPerTx);
                }
            }
        }
        //submit the outstanding transactions
        ListenableFuture<Void> aggregatePutFuture = Futures.transform(Futures.allAsList(putFutures), new Function<List<Void>, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable List<Void> voids) {
                return null;
            }
        });

        try{
            aggregatePutFuture.get();
            CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
            try
            {
                restSubmitFuture.checkedGet();
                txSucceed++;
                endTime = System.nanoTime();
            }catch (Exception e)
            {
                txError ++;
            }
        }catch (Exception e)
        {
            txError ++;
        }
    }
}
