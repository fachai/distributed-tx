package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.api.DTxProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DatastoreTestData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterListKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by sunny on 16-2-25.
 */
public class DtxAsyncWrite extends AbstractDataStoreWriter {
    private DTx dtx;
    private DTxProvider dTxProvider;
    private static final Logger LOG = LoggerFactory.getLogger(DtxAsyncWrite.class);
    private List<ListenableFuture<Void>> submitFutures = Lists.newArrayList();
    private Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap;

    public DtxAsyncWrite(BenchmarkTestInput input, DTxProvider dTxProvider, Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap, int outerElements, int innerElements)
    {
        super(input,outerElements,innerElements);
        this.dTxProvider = dTxProvider;
        this.nodesMap = nodesMap;
    }
    @Override
    public ListenableFuture<Void> writeData() {
        final SettableFuture<Void> setFuture = SettableFuture.create();
        long putsPerTx = input.getPutsPerTx();

        List<List<InnerList>> innerLists = buildInnerLists();
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

        InstanceIdentifier<DatastoreTestData> nodeId = InstanceIdentifier.create(DatastoreTestData.class);
        //store all the put futures
        List<ListenableFuture<Void>> putFutures = new ArrayList<ListenableFuture<Void>>((int) putsPerTx);
        startTime = System.nanoTime();

        int counter = 0;
        dtx = dTxProvider.newTx(nodesMap);
        LOG.info("Dtx {} test begin", input.getOperation());
        for (int i = 0; i < outerElements ; i++) {
            for (InnerList innerList : innerLists.get(i)) {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, new OuterListKey(i))
                        .child(InnerList.class, innerList.getKey());

                CheckedFuture<Void, DTxException> tx;
                if (input.getOperation() == BenchmarkTestInput.Operation.PUT) {
                    tx = dtx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, innerList, nodeId);
                }else if (input.getOperation() == BenchmarkTestInput.Operation.MERGE){
                    tx = dtx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, innerList, nodeId);
                }else{
                    tx = dtx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, nodeId);
                }

                putFutures.add(tx);
                counter++;

                if (counter == putsPerTx)
                {
                    //aggregate all the put futures into a listenable future, this future can make sure all the Async write has finish cache the data
                    ListenableFuture<Void> aggregatePutFuture = Futures.transform(Futures.allAsList(putFutures), new Function<List<Void>, Void>() {
                        @Nullable
                        @Override
                        public Void apply(@Nullable List<Void> voids) {
                            return null;
                        }
                    });

                    try{
                        aggregatePutFuture.get();
                    }catch (Exception e)
                    {
                        LOG.info("DTX Async put failed");
                        testFail = true;
                    }
                    CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dtx.submit();
                    try{
                        submitFuture.checkedGet();
                    }catch (TransactionCommitFailedException e)
                    {
                        LOG.info("DTX Async submit failed");
                    }

                    counter = 0;
                    dtx = dTxProvider.newTx(nodesMap); //after each submit we should get new Dtx
                    putFutures = new ArrayList<ListenableFuture<Void>>((int) putsPerTx);
                }
            }
        }
        ListenableFuture<Void> aggregatePutFuture = Futures.transform(Futures.allAsList(putFutures), new Function<List<Void>, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable List<Void> voids) {
                return null;
            }
        });

        CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
        Futures.addCallback(restSubmitFuture, new submitFutureCallback(setFuture));

        return setFuture;
    }

}
