/*
 * Copyright and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.distributed.tx.impl;

import org.junit.Test;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.impl.spi.DTxProviderImpl;
import org.opendaylight.distributed.tx.spi.TxProvider;

import java.util.HashMap;
import java.util.Map;

public class DistributedTxProviderTest {
    private TxProvider txProvider;
    Map<DTXLogicalTXProviderType, TxProvider> m = new HashMap<>();

    public void addProvider(final TxProvider txProvider) {
        this.txProvider = txProvider;
    }
    @Test
    public void testOnSessionInitiated() {
        m.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, txProvider);
        DTxProviderImpl provider = new DTxProviderImpl(m);
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
