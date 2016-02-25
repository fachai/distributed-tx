package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.CheckedFuture;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;

/**
 * Created by sunny on 16-2-25.
 */
public class DtxSyncPut extends AbstractDataWriter {

    public DtxSyncPut(BenchmarkTestInput input, DTx dTx)
    {
        super(input, dTx);
    }

    @Override
    public CheckedFuture<Void, Exception> writeData() {
        return null;
    }
}
