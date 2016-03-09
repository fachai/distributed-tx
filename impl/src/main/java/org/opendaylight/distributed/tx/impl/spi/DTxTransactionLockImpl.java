package org.opendaylight.distributed.tx.impl.spi;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.spi.TransactionLock;
import org.opendaylight.distributed.tx.spi.TxProvider;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DTxTransactionLockImpl implements TransactionLock {
    private volatile Set<InstanceIdentifier<?>> lockSet = new HashSet<>();
    private static final Logger LOG = LoggerFactory.getLogger(DTxTransactionLockImpl.class);
    private final Map<DTXLogicalTXProviderType, TxProvider>txProviderMap;

    public DTxTransactionLockImpl(Map<DTXLogicalTXProviderType, TxProvider> providerMap){
        this.txProviderMap = providerMap;
    }

    @Override
    public boolean isLocked(DTXLogicalTXProviderType type, InstanceIdentifier<?> device) {
        return this.txProviderMap.get(type).isDeviceLocked(device);
    }

    @Override
    public boolean lockDevices(DTXLogicalTXProviderType type, Set<InstanceIdentifier<?>> deviceSet) {
        return this.txProviderMap.get(type).lockTransactionDevices(deviceSet);
    }
    @Override
    public boolean lockDevices(final Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> deviceMap) {
        boolean allLocked = true;

        synchronized (DTxTransactionLockImpl.this) {
            for(DTXLogicalTXProviderType t : deviceMap.keySet()){
                if(!this.txProviderMap.get(t).lockTransactionDevices(deviceMap.get(t))){
                    allLocked = false;
                    break;
                }
            }

            if(!allLocked){
                this.releaseDevices(deviceMap);
            }
        }

        return allLocked;
    }

    @Override
    public void releaseDevices(DTXLogicalTXProviderType type, Set<InstanceIdentifier<?>> deviceSet) {
        this.txProviderMap.get(type).releaseTransactionDevices(deviceSet);
    }

    @Override
    public void releaseDevices(final Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> deviceMap) {
        Maps.asMap(this.txProviderMap.keySet(), new Function<DTXLogicalTXProviderType, Object>() {
            @Nullable
            @Override
            public Object apply(@Nullable DTXLogicalTXProviderType logicalTXProviderType) {
                txProviderMap.get(logicalTXProviderType).releaseTransactionDevices(deviceMap.get(logicalTXProviderType));
                return null;
            }
        });
    }
}

