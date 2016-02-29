package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import javassist.runtime.Inner;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionChain;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DatastoreTestData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterListKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerListBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerListKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sunny on 16-2-25.
 */
public abstract class AbstractDataStoreWriter extends AbstractDataWriter   {
    int outerElements, innerElements;
    boolean testFail = false;
    DataBroker db;

    public AbstractDataStoreWriter(BenchmarkTestInput input, DataBroker db, int outerElements, int innerElements)
    {
        super(input);
        this.db = db;
        this.outerElements = outerElements;
        this.innerElements = innerElements;
    }

    /**
     * this method is usd to build the test data fot delete operation
     * @return
     */
    public boolean build() {

        List<List<InnerList>> innerLists = buildInnerLists();
        WriteTransaction transaction = this.db.newWriteOnlyTransaction();
        for (int i = 0; i < outerElements ; i++) {
            for (InnerList innerList : innerLists.get(i) )
            {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, new OuterListKey(i))
                        .child(InnerList.class, innerList.getKey());

                transaction.put(LogicalDatastoreType.CONFIGURATION, innerIid, innerList);
                CheckedFuture<Void, TransactionCommitFailedException> submitFuture = transaction.submit();

                try{
                    submitFuture.checkedGet();
                }catch (Exception e)
                {
                    return false;
                }
                transaction = this.db.newWriteOnlyTransaction();
            }
        }
        return true;
    }

    //build the test data
    List<List<InnerList>> buildInnerLists() {
        List<List<InnerList>> innerLists = new ArrayList<>(outerElements);

        for (int i = 0; i < outerElements ; i++) {
            final String itemStr = "Item-" + String.valueOf(i) + "-";
            List<InnerList> innerList = new ArrayList<>(innerElements);

            for( int j = 0; j < innerElements; j++ ) {
                innerList.add(new InnerListBuilder()
                        .setKey( new InnerListKey( j ) )
                        .setName(j)
                        .setValue( itemStr + String.valueOf( j ) )
                        .build());
            }
            innerLists.add(innerList);
        }

        return innerLists;
    }
}
