/*
 * Copyright and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.distributed.tx.impl;

import com.google.common.util.concurrent.CheckedFuture;
import com.sun.jmx.snmp.tasks.ThreadService;
import org.eclipse.xtend.lib.annotations.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.impl.spi.DTxProviderImpl;
import org.opendaylight.distributed.tx.spi.TxException;
import org.opendaylight.distributed.tx.spi.TxProvider;
import org.opendaylight.yangtools.yang.binding.DataContainer;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.fail;

public class DistributedTxProviderTest {
    private TxProvider txProvider;
    private DTxProviderImpl dTxProvider;
    private DTx dTx1;
    private DTx dTx2;
    private DTx dTx3;

    InstanceIdentifier<TestClassNode1> node1 = InstanceIdentifier.create(TestClassNode1.class);
    InstanceIdentifier<TestClassNode2> node2 = InstanceIdentifier.create(TestClassNode2.class);
    InstanceIdentifier<TestClassNode3> node3 = InstanceIdentifier.create(TestClassNode3.class);
    InstanceIdentifier<TestClassNode4> node4 = InstanceIdentifier.create(TestClassNode4.class);
    InstanceIdentifier<TestClassNode5> node5 = InstanceIdentifier.create(TestClassNode5.class);

    private class myTxProvider implements TxProvider{

        @Override
        public ReadWriteTransaction newTx(InstanceIdentifier<?> nodeId) throws TxException.TxInitiatizationFailedException {
            return new DTXTestTransaction();
        }
    }

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

    private class CreateNewTx1 implements Runnable{

        @Override
        public void run() {

            int sleepTime = (int)(Math.random()*100);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Set<InstanceIdentifier<?>> nodeSet = Sets.newSet(node1, node2, node3);
            try
            {
               dTx1 = dTxProvider.newTx(nodeSet);
            }catch (Exception e)
            {
                Assert.assertTrue(e instanceof DTxException.DTxInitializationFailedException);
                System.out.println(e.getMessage());
            }
        }
    }

    private class CreateNewTx2 implements Runnable{

        @Override
        public void run() {

            int sleepTime = (int)(Math.random()*100);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Set<InstanceIdentifier<?>> nodeSet = Sets.newSet(node3, node4, node5);
            try
            {
               dTx2 = dTxProvider.newTx(nodeSet);
            }catch (Exception e)
            {
                Assert.assertTrue(e instanceof DTxException.DTxInitializationFailedException);
                System.out.println(e.getMessage());
            }
        }
    }

    private class CreateNewTx3 implements Runnable{

        @Override
        public void run() {

            int sleepTime = (int)(Math.random()*100);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Set<InstanceIdentifier<?>> nodeSet = Sets.newSet(node4, node5);
            try
            {
                dTx3 = dTxProvider.newTx(nodeSet);
            }catch (Exception e)
            {
                Assert.assertTrue(e instanceof DTxException.DTxInitializationFailedException);
                System.out.println(e.getMessage());
            }
        }
    }


    public void addProvider(final TxProvider txProvider) {
        this.txProvider = txProvider;
    }

    @Test
    public void testOnSessionInitiated() {
        DTxProviderImpl provider = new DTxProviderImpl(txProvider);
    }

    @Test
    public void testNewTx()
    {

        dTxProvider = new DTxProviderImpl(new myTxProvider());
        ExecutorService threadPool = Executors.newCachedThreadPool();
        threadPool.execute(new CreateNewTx1());
        threadPool.execute(new CreateNewTx2());
        threadPool.execute(new CreateNewTx3());
        threadPool.shutdown();
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCancel()
    {
        Runnable createDtx1 = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    DTx dTx = dTxProvider.newTx((Set<InstanceIdentifier<?>>)Sets.newSet(node1, node2, node3));
                }catch (Exception e)
                {
                    System.out.println(e.getMessage());
                    return;
                }
                dTx1.cancel();

            }
        };

        Runnable createDtx2 = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    DTx dTx = dTxProvider.newTx((Set<InstanceIdentifier<?>>)Sets.newSet(node2, node3, node4));
                }catch (Exception e)
                {
                    System.out.println(e.getMessage());
                    fail();
                }
            }
        };

        ExecutorService threadPool = Executors.newCachedThreadPool();
        threadPool.execute(createDtx1);
        threadPool.execute(createDtx2);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testClose() throws Exception {
        DTxProviderImpl provider = new DTxProviderImpl(txProvider);

        // ensure no exceptions
        // currently this method is empty
        provider.close();
    }
}
