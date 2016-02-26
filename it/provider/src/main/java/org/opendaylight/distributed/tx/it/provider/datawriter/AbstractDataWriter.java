package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;

/**
 * Created by sunny on 16-2-24.
 */
public abstract class AbstractDataWriter {
    long startTime, endTime;
    BenchmarkTestInput input;

    public AbstractDataWriter(BenchmarkTestInput input)
    {
        this.input = input;
    }

    public abstract ListenableFuture<Void> writeData();

    public long getExecTime()
    {
        return (endTime - startTime)/1000;
    }
}
