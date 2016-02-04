package org.opendaylight.distributed.tx.spi;

import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import java.util.Map;
import java.util.Set;

/**
 * Created by cisco on 1/19/16.
 */
public interface TransactionLock {
    boolean lockDevice(InstanceIdentifier<?> device);
    boolean isLocked(InstanceIdentifier<?> device);
    void releaseDevice(InstanceIdentifier<?> device);
    boolean lockDevices(Set<InstanceIdentifier<?>> deviceSet);
    boolean lockDevices(Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> deviceMap);
    void releaseDevices(Set<InstanceIdentifier<?>> deviceSet);
    void releaseDevices(Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> deviceMap);
}
