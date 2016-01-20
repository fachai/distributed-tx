package org.opendaylight.distributed.tx.impl.spi;

import com.google.common.base.Preconditions;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.spi.TransactionLock;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class DTxTransactionLockImpl implements TransactionLock {
    final private Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> lockMap = new HashMap<>();

    public DTxTransactionLockImpl(){
        for(DTXLogicalTXProviderType type : DTXLogicalTXProviderType.values()){
            lockMap.put(type, new HashSet<InstanceIdentifier<?>>());
        }
    }

    @Override
    public synchronized  boolean lockDevice(DTXLogicalTXProviderType type, InstanceIdentifier<?> device) {
        Preconditions.checkArgument(lockMap.containsKey(type), "tx provider type doesn't exist");
        if(lockMap.get(type).contains(device)) {
            return false;
        }else {
            lockMap.get(type).add(device);
        }

        return true;
    }

    @Override
    public synchronized boolean isLocked(DTXLogicalTXProviderType type, InstanceIdentifier<?> device) {
        Preconditions.checkArgument(lockMap.containsKey(type), "tx provider type doesn't exist");
        return lockMap.get(type).contains(device);
    }

    @Override
    public synchronized void releaseDevice(DTXLogicalTXProviderType type, InstanceIdentifier<?> device) {
        Preconditions.checkArgument(lockMap.containsKey(type), "tx provider type doesn't exist");
        Preconditions.checkArgument(lockMap.get(type).contains(device), "tx provider type doesn't exist");
        lockMap.get(type).remove(device);
    }
}
