package org.opendaylight.distributed.tx.spiimpl;


import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.MountPoint;
import org.opendaylight.controller.md.sal.binding.api.MountPointService;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker;
import org.opendaylight.controller.sal.binding.api.BindingAwareConsumer;
import org.opendaylight.distributed.tx.spi.TxException;
import org.opendaylight.distributed.tx.spi.TxProvider;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by fachai on 1/7/16.
 */
public class DataStoreTxProvider implements TxProvider, AutoCloseable, BindingAwareConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MountServiceTxProvider.class);
    private DataBroker dataBroker = null;

    /**
     * Initialize per node transaction.
     *
     * @param path IID for particular node
     * @return per node tx
     * @throws TxException.TxInitiatizationFailedException thrown when unable to initialize the tx
     */
    @Nonnull
    @Override
    public ReadWriteTransaction newTx(@Nullable InstanceIdentifier<?> path) {
        return dataBroker.newReadWriteTransaction();
    }

    @Override
    public boolean isDeviceLocked(InstanceIdentifier<?> device) {
        return false;
    }

    @Override
    public boolean lockTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {
        return true;
    }

    @Override
    public void releaseTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {
    }

    @Override
    public void close() throws Exception {
        dataBroker = null;
    }

    @Override
    public void onSessionInitialized(final BindingAwareBroker.ConsumerContext session) {
        dataBroker = session.getSALService(DataBroker.class);
    }
}

