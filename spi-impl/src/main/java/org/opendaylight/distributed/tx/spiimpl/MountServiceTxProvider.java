package org.opendaylight.distributed.tx.spiimpl;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;
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
 * Per node transaction provider SPI. Distributed tx treats every node just as an instance of ReadWriteTransaction.
 * This provider interface hides the details of its creation, whether the per node transactions come from MountPoints or are app specific.
 */
public class MountServiceTxProvider implements TxProvider, AutoCloseable, BindingAwareConsumer {

    private volatile MountPointService mountService;
    private static final Logger LOG = LoggerFactory.getLogger(MountServiceTxProvider.class);
    private final TxProviderLock txLock = new TxProviderLock();

    /**
     *
     * Initialize per node transaction.
     *
     * @param path IID for particular node
     * @return per node tx
     * @throws TxException.TxInitiatizationFailedException thrown when unable to initialize the tx
     */
    @Nonnull @Override public ReadWriteTransaction newTx(InstanceIdentifier<?> path) {
        Preconditions.checkState(mountService != null, "MountPoint service dependency no tinitialized");
        final Optional<MountPoint> mountPoint = mountService.getMountPoint(path);

        if(mountPoint.isPresent()) {
            final MountPoint mpNode = mountPoint.get();
            // Get the DataBroker for the mounted node
            final DataBroker dataBroker = mpNode.getService(DataBroker.class).get();
            // Open an read and write transaction using the databroker.

            return dataBroker.newReadWriteTransaction();
        } else {
            throw new TxException.TxInitiatizationFailedException("Unable to create tx for " + path + ", Mountpoint does not exist");
        }
    }

    @Override
    public boolean isDeviceLocked(InstanceIdentifier<?> device) {
        return txLock.isDeviceLocked(device);
    }

    @Override
    public boolean lockTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {

        return txLock.lockDevices(deviceSet);
    }

    @Override
    public void releaseTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {
        txLock.releaseDevices(deviceSet);
    }

    @Override public void close() throws Exception {
        mountService = null;
    }

    @Override public void onSessionInitialized(final BindingAwareBroker.ConsumerContext session) {
        mountService = session.getSALService(MountPointService.class);
    }
}