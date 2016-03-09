/*
 * Copyright and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.distributed.tx.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.DTxProviderImpl;
import org.opendaylight.distributed.tx.spi.TxException;
import org.opendaylight.distributed.tx.spi.TxProvider;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DistributedTxProviderTest {
    private TxProvider txProvider;
    private DTxProviderImpl dTxProvider;
    private ExecutorService threadPool;
    private volatile int exceptionOccurNum = 0; //Number of exceptions got from the provider
    //followings are different nodeId
    InstanceIdentifier<TestClassNode1> node1 = InstanceIdentifier.create(TestClassNode1.class);
    InstanceIdentifier<TestClassNode2> node2 = InstanceIdentifier.create(TestClassNode2.class);
    InstanceIdentifier<TestClassNode3> node3 = InstanceIdentifier.create(TestClassNode3.class);
    InstanceIdentifier<TestClassNode4> node4 = InstanceIdentifier.create(TestClassNode4.class);
    InstanceIdentifier<TestClassNode5> node5 = InstanceIdentifier.create(TestClassNode5.class);
    Map<DTXLogicalTXProviderType, TxProvider> m = new HashMap<>();

    private class myTxProvider implements TxProvider{
        boolean isLocked = true;

        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            return new DTXTestTransaction();
        }

        @Override
        public boolean isDeviceLocked(InstanceIdentifier<?> device) {
            return false;
        }

        @Override
        public boolean lockTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {
            boolean ret = isLocked;
            this.isLocked = !ret;
            return ret;
        }

        @Override
        public void releaseTransactionDevices(Set<InstanceIdentifier<?>> deviceSet) {

        }
    }

    //these classes are used to be create the nodeId
    private class TestClassNode1  implements DataObject {
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }
    private class TestClassNode2 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestClassNode3 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestClassNode4 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    private class TestClassNode5 implements DataObject{
        @Override
        public Class<? extends DataContainer> getImplementedInterface() {
            return null;
        }
    }

    //Task1 get the dtx from the dtxProvider for node1, node2 and node3
    private class Task1 implements Runnable{

        @Override
        public void run() {
            Set<InstanceIdentifier<?>> s1 = Sets.newSet(node1, node2, node3);
            try
            {
                dTxProvider.newTx(s1);
            }catch (Exception e)
            {
                exceptionOccurNum++;
                Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.DTxInitializationFailedException);
            }
        }
    }
   //Task2 get the dtx from the dtxProvider for node3, node4, node5
    private class Task2 implements Runnable{

        @Override
        public void run() {
            Set<InstanceIdentifier<?>> s2 = Sets.newSet(node3, node4, node5);
            try{
                dTxProvider.newTx(s2);
            }catch (Exception e)
            {
                exceptionOccurNum++;
                Assert.assertTrue("get the wrong kind of exception", e instanceof DTxException.DTxInitializationFailedException);
            }
        }
    }

    /**
     * initiate the dtxProvider
     */
    @Before
    public void testOnSessionInitiated() {
        txProvider = new myTxProvider();
        m.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, txProvider);
        dTxProvider = new DTxProviderImpl(m);
    }

    /**
     * two thread try to get Dtx from the DtxProvider
     * but they have some nodes in common
     * one of the threads will get the exception
     */
    @Test
    public void testNewTx()
    {
          threadPool = Executors.newFixedThreadPool(2);
          threadPool.execute(new Task1());
          threadPool.execute(new Task2());
          threadPool.shutdown();
          while(!threadPool.isTerminated())
          {
              //make sure all the thread has terminate
          }
         //make sure just one exception has occurred the other task successfully get the dtx
          Assert.assertTrue("no exception occur test fail", exceptionOccurNum == 1);
    }

    @Test
    public void testClose() throws Exception {
        m.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, txProvider);
        DTxProviderImpl provider = new DTxProviderImpl(m);
        // ensure no exceptions
        // currently this method is empty
        provider.close();
    }
}
