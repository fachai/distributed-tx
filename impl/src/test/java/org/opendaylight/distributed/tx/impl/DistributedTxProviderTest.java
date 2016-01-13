/*
 * Copyright and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.distributed.tx.impl;

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
    private DTxProviderImpl dTxProvider;

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

    public void addProvider(final TxProvider txProvider) {
        this.txProvider = txProvider;
    }

    @Test
    public void testOnSessionInitiated() {
        DTxProviderImpl provider = new DTxProviderImpl(txProvider);
    }

    /**
     * use three node sets to get new Dtx from the provider
     * when use s3 to create the dtx from provider, node1, node3 and node5 are already in use so newTx will throw an exception
     */
    @Test
    public void testNewTx()
    {
        dTxProvider = new DTxProviderImpl(new myTxProvider());
        Set<InstanceIdentifier<?>> s1 = Sets.newSet(node1, node2, node3);
        Set<InstanceIdentifier<?>> s2 = Sets.newSet(node4, node5);
        Set<InstanceIdentifier<?>> s3 = Sets.newSet(node1, node3, node5);

        try
        {
            DTx dTx1 = dTxProvider.newTx(s1);
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        try
        {
            DTx dTx2 = dTxProvider.newTx(s2);
        }catch (Exception e)
        {
            fail("get unexpected exception");
        }

        try
        {
            DTx dTx3 = dTxProvider.newTx(s3);
        }catch (Exception e)
        {
            Assert.assertTrue("type of exception is wrong", e instanceof DTxException.DTxInitializationFailedException);
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
