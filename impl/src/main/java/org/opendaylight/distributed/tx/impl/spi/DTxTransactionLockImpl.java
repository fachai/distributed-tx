package org.opendaylight.distributed.tx.impl.spi;

import com.google.common.base.Preconditions;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.spi.TransactionLock;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DTxTransactionLockImpl implements TransactionLock {
    private volatile Set<InstanceIdentifier<?>> lockSet = new HashSet<>();
    private static final Logger LOG = LoggerFactory.getLogger(DTxTransactionLockImpl.class);
    private int lockCnt = 0;
    private int unlockCnt = 0;

    public DTxTransactionLockImpl(){
    }

    @Override
    public boolean lockDevice(InstanceIdentifier<?> device) {
        boolean ret = true;
        synchronized (DTxTransactionLockImpl.this) {
            if (lockSet.contains(device)) {
                ret = false;
            } else {
                lockSet.add(device);
            }
        }

        return ret;
    }

    @Override
    public boolean isLocked(InstanceIdentifier<?> device) {
        boolean ret ;
        synchronized (DTxTransactionLockImpl.this) {
            ret = lockSet.contains(device);
        }

        return ret;
    }

    @Override
    public void releaseDevice(InstanceIdentifier<?> device) {
        synchronized (DTxTransactionLockImpl.this) {
            lockSet.remove(device);
        }
    }

    @Override
    public boolean lockDevices(Set<InstanceIdentifier<?>> deviceSet) {
        boolean ret = true;
        synchronized (DTxTransactionLockImpl.this) {
            Set<InstanceIdentifier<?>> s = new HashSet<>();
            s.addAll(this.lockSet);

            s.retainAll(deviceSet);

            if(s.size() > 0)
                ret = false;
            else {
                lockSet.addAll(deviceSet);
            }
        }

        return ret;
    }
    @Override
    public boolean lockDevices(Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> deviceMap) {
        boolean ret = true;
        synchronized (DTxTransactionLockImpl.this) {
            for(DTXLogicalTXProviderType t : deviceMap.keySet()) {
                Set<InstanceIdentifier<?>> s = new HashSet<>();
                s.addAll(this.lockSet);

                s.retainAll(deviceMap.get(t));

                if(s.size() > 0) {
                    ret = false;
                    break;
                }
            }

            if(ret){
                for(DTXLogicalTXProviderType t : deviceMap.keySet()) {
                    this.lockSet.addAll(deviceMap.get(t));
                }
            }
        }
        this.lockCnt++;
        if(!ret){
            LOG.info("lock fialed)" + this.lockCnt + " " +this.unlockCnt);
        }

        return true;
    }

    @Override
    public void releaseDevices(Set<InstanceIdentifier<?>> deviceSet) {
        this.lockSet.removeAll(deviceSet);
    }

    @Override
    public void releaseDevices(Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> deviceMap) {
        this.unlockCnt ++;
        synchronized (DTxTransactionLockImpl.this) {
            for(DTXLogicalTXProviderType t : deviceMap.keySet()) {
                this.lockSet.removeAll(deviceMap.get(t));
            }
        }
    }
}

