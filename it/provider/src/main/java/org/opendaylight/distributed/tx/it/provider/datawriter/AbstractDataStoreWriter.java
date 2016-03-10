package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.CheckedFuture;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DatastoreTestData;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterListBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterListKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerListBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerListKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDataStoreWriter extends AbstractDataWriter   {
    int outerElements, innerElements;
    DataBroker db;

    public AbstractDataStoreWriter(BenchmarkTestInput input, DataBroker db, int outerElements, int innerElements)
    {
        super(input);
        this.db = db;
        this.outerElements = outerElements;
        this.innerElements = innerElements;
    }
    /**
     * this method is used to build the test data for the delete operation
     */
    public boolean build() {
        List<OuterList> outerLists = buildOuterList(outerElements, innerElements);
        WriteTransaction transaction = this.db.newWriteOnlyTransaction();
        for ( OuterList outerList : outerLists)
        {
            for (InnerList innerList : outerList.getInnerList() )
            {
                InstanceIdentifier<InnerList> innerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, outerList.getKey())
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

    /**
     * build the data for the write operation
     * @param outerElements
     * @param innerElements
     * @return the test data
     */
    public List<OuterList> buildOuterList(int outerElements, int innerElements) {
        List<OuterList> outerList = new ArrayList<OuterList>(outerElements);
        for (int j = 0; j < outerElements; j++) {
            outerList.add(new OuterListBuilder()
                    .setId( j )
                    .setInnerList(buildInnerList(j, innerElements))
                    .setKey(new OuterListKey( j ))
                    .build());
        }
        return outerList;
    }

    private List<InnerList> buildInnerList( int index, int elements ) {
        List<InnerList> innerList = new ArrayList<InnerList>( elements );
        final String itemStr = "Item-" + String.valueOf(index) + "-";
        for( int i = 0; i < elements; i++ ) {
            innerList.add(new InnerListBuilder()
                    .setKey( new InnerListKey( i ) )
                    .setName(i)
                    .setValue( itemStr + String.valueOf( i ) )
                    .build());
        }
        return innerList;
    }
}
