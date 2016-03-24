package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.CheckedFuture;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.it.provider.DataStoreListBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DatastoreTestData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.OperationType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import java.util.List;

public class DataBrokerWriter extends AbstractDataWriter{
    private DataBroker dataBroker;

    public DataBrokerWriter(BenchmarkTestInput input, DataBroker db)
    {
        super(input);
        this.dataBroker = db;
    }
    @Override
    public void writeData() {
        long putsPerTx = input.getPutsPerTx();
        DataStoreListBuilder dataStoreListBuilder = new DataStoreListBuilder(dataBroker, input.getOuterList(), input.getInnerList());

        //when the operation is delete we should put the test data first
        if (input.getOperation() == OperationType.DELETE)
        {
            boolean buildTestData = dataStoreListBuilder.writeTestList();//build the test data for the operation
            if (!buildTestData)
            {
                return;
            }
        }

        WriteTransaction tx = dataBroker.newWriteOnlyTransaction();
        List<OuterList> outerLists = dataStoreListBuilder.buildOuterList();
        startTime = System.nanoTime();
        long counter = 0;
        for ( OuterList outerList : outerLists ) {
            for (InnerList innerList : outerList.getInnerList()) {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, outerList.getKey())
                        .child(InnerList.class, innerList.getKey());
                if (input.getOperation() == OperationType.PUT) {
                    tx.put(LogicalDatastoreType.CONFIGURATION, innerIid, innerList);
                }else if (input.getOperation() == OperationType.MERGE){
                    tx.merge(LogicalDatastoreType.CONFIGURATION, innerIid, innerList);
                }else {
                    tx.delete(LogicalDatastoreType.CONFIGURATION, innerIid);
                }
                counter++;

                if (counter == putsPerTx) {
                    CheckedFuture<Void, TransactionCommitFailedException> submitFut = tx.submit();
                    try{
                        submitFut.checkedGet();
                        txSucceed++;
                    }catch (Exception e)
                    {
                        txError++;
                    }
                    counter = 0;
                    tx = dataBroker.newWriteOnlyTransaction();
                }
            }
        }
        CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = tx.submit();

        try
        {
            restSubmitFuture.checkedGet();
            txSucceed++;
            endTime = System.nanoTime();

        }catch (Exception e)
        {
            txError++;
        }
    }
}
