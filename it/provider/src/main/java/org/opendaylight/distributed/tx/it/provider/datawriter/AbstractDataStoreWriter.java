package org.opendaylight.distributed.tx.it.provider.datawriter;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.common.api.data.TransactionChainListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerListBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerListKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sunny on 16-2-25.
 */
public abstract class AbstractDataStoreWriter extends AbstractDataWriter  implements TransactionChainListener {
    int outerElements, innerElements;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataStoreWriter.class);

    public AbstractDataStoreWriter(BenchmarkTestInput input,int outerElements, int innerElements)
    {
        super(input);
        this.outerElements = outerElements;
        this.innerElements = innerElements;
    }

    protected List<List<InnerList>> buildInnerList() {
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
