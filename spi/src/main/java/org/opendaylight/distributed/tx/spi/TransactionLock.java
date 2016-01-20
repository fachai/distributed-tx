package org.opendaylight.distributed.tx.spi;

import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

/**
 * Created by cisco on 1/19/16.
 */
public interface TransactionLock {
    boolean lockDevice(DTXLogicalTXProviderType t, InstanceIdentifier<?> device);
    boolean isLocked(DTXLogicalTXProviderType t, InstanceIdentifier<?> device);
    void releaseDevice(DTXLogicalTXProviderType t, InstanceIdentifier<?> device);
}
