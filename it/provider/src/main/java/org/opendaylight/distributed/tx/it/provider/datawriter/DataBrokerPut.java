package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.ListenableFuture;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.controller.md.sal.binding.impl.rev131028.modules.module.configuration.binding.broker.impl.binding.broker.impl.DataBroker;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;

/**
 * Created by sunny on 16-2-25.
 */
public class DataBrokerPut extends AbstractDataWriter {
    DataBroker dataBroker;

    public DataBrokerPut(BenchmarkTestInput input, DataBroker db)
    {
        super(input);
        this.dataBroker = db;
    }

    @Override
    public ListenableFuture<Void> writeData() {
        return null;
    }
}
