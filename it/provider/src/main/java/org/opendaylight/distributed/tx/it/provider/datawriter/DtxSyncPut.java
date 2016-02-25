package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;

/**
 * Created by sunny on 16-2-25.
 */
public class DtxSyncPut extends AbstractDataWriter {
    DTx dtx;

    public DtxSyncPut(BenchmarkTestInput input, DTx dTx)
    {
        super(input);
        this.dtx = dTx;
    }
    
    @Override
    public ListenableFuture<Void> writeData() {
        return null;
    }
}
