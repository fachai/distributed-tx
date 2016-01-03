/*
 * Copyright and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.distributed.tx.impl;

import org.eclipse.xtend.lib.annotations.Data;
import org.junit.Assert;
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

import static org.junit.Assert.fail;

public class DistributedTxProviderTest {
    private TxProvider txProvider;

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
        InstanceIdentifier<TestClassNode1> node1 = InstanceIdentifier.create(TestClassNode1.class);
        InstanceIdentifier<TestClassNode2> node2 = InstanceIdentifier.create(TestClassNode2.class);
        InstanceIdentifier<TestClassNode3> node3 = InstanceIdentifier.create(TestClassNode3.class);
        InstanceIdentifier<TestClassNode4> node4 = InstanceIdentifier.create(TestClassNode4.class);
        InstanceIdentifier<TestClassNode5> node5 = InstanceIdentifier.create(TestClassNode5.class);

        DTxProviderImpl dTxProvider = new DTxProviderImpl(new myTxProvider());

        Set<InstanceIdentifier<?>> nodeSet1 = Sets.newSet(node1, node2, node3);
        Set<InstanceIdentifier<?>> nodeSet2 = Sets.newSet(node3, node4, node5);

        DTx dTx1 = dTxProvider.newTx(nodeSet1);

        try
        {
            DTx dTx2 = dTxProvider.newTx(nodeSet2);
            fail();
        }catch (Exception e)
        {
            Assert.assertTrue(e instanceof DTxException.DTxInitializationFailedException);
            System.out.println(e.getMessage());
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
