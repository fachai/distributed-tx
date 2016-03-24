package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.CheckedFuture;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.api.DTxProvider;
import org.opendaylight.distributed.tx.it.provider.DataStoreListBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DatastoreTestData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.OperationType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DtxSyncWriter extends AbstractDataWriter {
    private DTx dtx;
    private DTxProvider dTxProvider;
    private Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap;
    private DataBroker dataBroker;

    public DtxSyncWriter(BenchmarkTestInput input, DTxProvider dTxProvider, DataBroker dataBroker, Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap)
    {
        super(input);
        this.dTxProvider = dTxProvider;
        this.nodesMap = nodesMap;
        this.dataBroker = dataBroker;
    }

    @Override
    public void writeData() {
        long putsPerTx = input.getPutsPerTx();
        DataStoreListBuilder dataStoreListBuilder = new DataStoreListBuilder(dataBroker, input.getOuterList(), input.getInnerList());
        //when the operation is delete we should build the test data first
        if (input.getOperation() == OperationType.DELETE)
        {
            boolean buildTestData = dataStoreListBuilder.writeTestList();//build the test data for the operation
            if (!buildTestData)
            {
                return;
            }
        }

        InstanceIdentifier<DatastoreTestData> nodeId = InstanceIdentifier.create(DatastoreTestData.class);

        int counter = 0;
        List<OuterList> outerLists = dataStoreListBuilder.buildOuterList();
        startTime = System.nanoTime();
        dtx = dTxProvider.newTx(nodesMap);
        for ( OuterList outerList : outerLists ) {
            for (InnerList innerList : outerList.getInnerList() ) {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, outerList.getKey())
                        .child(InnerList.class, innerList.getKey());

                CheckedFuture<Void, DTxException> writeFuture ;
                if (input.getOperation() == OperationType.PUT) {
                    writeFuture = dtx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, innerList, nodeId);

                }else if (input.getOperation() == OperationType.MERGE){
                    writeFuture = dtx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, innerList, nodeId);

                }else{
                    writeFuture = dtx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, LogicalDatastoreType.CONFIGURATION, innerIid, nodeId);
                }
                counter++;

                try{
                    writeFuture.checkedGet();
                }catch (Exception e)
                {
                    txError++;
                    counter = 0;
                    dtx = dTxProvider.newTx(nodesMap);
                    continue;
                }

                if (counter == putsPerTx)
                {
                    CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dtx.submit();
                    try{
                        submitFuture.checkedGet();
                        txSucceed++;
                    }catch (TransactionCommitFailedException e)
                    {
                        txError++;
                    }
                    counter = 0;
                    dtx = dTxProvider.newTx(nodesMap);
                }
            }
        }
        //submit the outstanding transactions
        CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
        try
        {
            restSubmitFuture.checkedGet();
            endTime = System.nanoTime();
            txSucceed++;
        }catch (Exception e)
        {
            txError++;
        }
    }
}
