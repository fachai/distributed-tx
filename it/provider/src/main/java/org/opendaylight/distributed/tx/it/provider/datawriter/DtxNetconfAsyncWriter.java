/*
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.distributed.tx.it.provider.datawriter;

import com.google.common.base.Function;
import com.google.common.util.concurrent.*;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.api.DTxProvider;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107.InterfaceActive;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfiguration;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfigurationBuilder;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfigurationKey;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.xr.types.rev150119.InterfaceName;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.OperationType;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NodeId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.NodeKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.KeyedInstanceIdentifier;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

public class DtxNetconfAsyncWriter extends AbstractNetconfWriter {
    private DTx dtx;
    private DTxProvider dTxProvider;

    public DtxNetconfAsyncWriter(BenchmarkTestInput input, DataBroker db, DTxProvider dTxProvider, Set nodeidset, Map<NodeId, List<InterfaceName>> nodeiflist) {
        super(input,db, nodeidset, nodeiflist);
        this.dTxProvider = dTxProvider;
    }

    @Override
    public void writeData() {
        long putsPerTx = input.getPutsPerTx();

        if (input.getOperation() == OperationType.DELETE) {
           configInterface();
        }

        List<ListenableFuture<Void>> putFutures = new ArrayList<ListenableFuture<Void>>((int) putsPerTx);
        List<NodeId> nodeIdList = new ArrayList(this.nodeIdSet);
        Set<InstanceIdentifier<?>> txIidSet = new HashSet<>();
        NodeId n = nodeIdList.get(0);
        InstanceIdentifier msNodeId = NETCONF_TOPO_IID.child(Node.class, new NodeKey(n));
        txIidSet.add(msNodeId);
        dtx = dTxProvider.newTx(txIidSet);
        int counter = 0;
        InterfaceName ifname = nodeIfList.get(n).get(0);
        startTime = System.nanoTime();
        for(int i=1;i<=input.getLoop();i++){
            KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                    = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), ifname));
            InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
            interfaceConfigurationBuilder.setInterfaceName(ifname);
            interfaceConfigurationBuilder.setDescription("Test description" + "-" + input.getOperation()+"-"+i);
            interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
            InterfaceConfiguration config = interfaceConfigurationBuilder.build();

            CheckedFuture<Void, DTxException> writeFuture = null;
            if (input.getOperation() == OperationType.PUT) {
                writeFuture = dtx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config, msNodeId);
            } else if (input.getOperation() == OperationType.MERGE) {
                writeFuture = dtx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config, msNodeId);
            } else {
                InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> subSpecificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                writeFuture = dtx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, subSpecificInterfaceCfgIid, msNodeId);
            }
            putFutures.add(writeFuture);
            counter++;

            if (counter == putsPerTx) {
                ListenableFuture<Void> aggregatePutFuture = Futures.transform(Futures.allAsList(putFutures), new Function<List<Void>, Void>() {
                    @Nullable
                    @Override
                    public Void apply(@Nullable List<Void> voids) {
                        return null;
                    }
                });
                try {
                    aggregatePutFuture.get();
                    CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dtx.submit();
                    try {
                        submitFuture.checkedGet();
                        txSucceed++;
                    } catch (TransactionCommitFailedException e) {
                        txError++;
                    }
                } catch (Exception e) {
                    txError++;
                    dtx.cancel();
                }

                counter = 0;
                dtx = dTxProvider.newTx(txIidSet);
                putFutures = new ArrayList<ListenableFuture<Void>>((int) putsPerTx);
            }
        }

        ListenableFuture<Void> aggregatePutFuture = Futures.transform(Futures.allAsList(putFutures), new Function<List<Void>, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable List<Void> voids) {
                return null;
            }
        });

        try{
            aggregatePutFuture.get();
            CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
            try {
                restSubmitFuture.checkedGet();
                txSucceed++;
            }catch (Exception e) {
                txError ++;
            }
        }catch (Exception e) {
            txError ++;
        }
        endTime = System.nanoTime();
    }
}
















