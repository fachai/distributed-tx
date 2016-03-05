package org.opendaylight.distributed.tx.it.provider.datawriter;

import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;

/**
 * Created by sunny on 16-2-24.
 * this class is the parent class for both the data store test classes and netconf test classes
 */
public abstract class AbstractDataWriter {
    long startTime, endTime;
    BenchmarkTestInput input;
    long txSucceed = 0, txError = 0;//number of the successful transaction submits and failed submits

    public AbstractDataWriter(BenchmarkTestInput input)
    {
        this.input = input;
    }

    //writing data into the data store all the netconf devices
    public abstract void writeData();

    public long getTxSucceed(){
        return txSucceed;
    }

    public long getExecTime()
    {
        return ((endTime - startTime)/1000);
    }
}
