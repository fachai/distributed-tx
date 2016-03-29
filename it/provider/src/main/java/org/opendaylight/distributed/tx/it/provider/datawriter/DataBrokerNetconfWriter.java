package org.opendaylight.distributed.tx.it.provider.datawriter;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import com.google.common.util.concurrent.*;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107.InterfaceActive;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfiguration;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfigurationBuilder;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfigurationKey;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.xr.types.rev150119.InterfaceName;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.BenchmarkTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.OperationType;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NodeId;
import org.opendaylight.yangtools.yang.binding.KeyedInstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class DataBrokerNetconfWriter extends AbstractNetconfWriter {
    private static final Logger LOG = LoggerFactory.getLogger(DataBrokerNetconfWriter.class);

    public DataBrokerNetconfWriter(BenchmarkTestInput input, DataBroker db, Set nodeidset, Map<NodeId, List<InterfaceName>> nodeiflist) {
        super(input, db, nodeidset, nodeiflist);
    }

    @Override
    public void writeData() {
        long putsPerTx = input.getPutsPerTx();
        //if the operation is delete,we should create sub-interface first
        if (input.getOperation() ==OperationType.DELETE) {
            boolean buildTestConfig = configInterface();
            if (!buildTestConfig) {
                LOG.info("can't initialize the interface configuration");
                return;
            }
        }

        int counter = 0;
        List<NodeId> nodeIdList = new ArrayList(nodeIdSet);
        LOG.info("Native Netconf API {} test begin", input.getOperation());
        //create databroker writeTransactio
        WriteTransaction xrNodeWriteTx = xrNodeBroker.newWriteOnlyTransaction();
        startTime = System.nanoTime();
        //show the node we are going to operate
        NodeId n = nodeIdList.get(0);
        LOG.info("nodeIdList {}", nodeIdList);
        LOG.info("nodeIfList {}", nodeIfList);
        InterfaceName ifname =nodeIfList.get(n).get(0) ;
        for (int i=1;i<=input.getLoop();i++){
            final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                    = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), ifname));
            final InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
            interfaceConfigurationBuilder.setInterfaceName(ifname);
            interfaceConfigurationBuilder.setDescription("Test description" + "-" + input.getOperation()+"-"+i);
            interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
            InterfaceConfiguration config = interfaceConfigurationBuilder.build();

            if (input.getOperation() == OperationType.PUT) {
                xrNodeWriteTx.put(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config);
            } else if (input.getOperation() == OperationType.MERGE) {
                xrNodeWriteTx.merge(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config);
            } else {
                InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> subSpecificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                xrNodeWriteTx.delete(LogicalDatastoreType.CONFIGURATION, subSpecificInterfaceCfgIid);
            }
            counter++;

            if (counter == putsPerTx) {
                CheckedFuture<Void, TransactionCommitFailedException> submitFut = xrNodeWriteTx.submit();
                try {
                    submitFut.checkedGet();
                    txSucceed++;
                }catch (Exception e){
                    txError++;
                }

                counter = 0;
                xrNodeWriteTx=xrNodeBroker.newReadWriteTransaction();
            }
        }

        CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = xrNodeWriteTx.submit();
        try
        {
            restSubmitFuture.checkedGet();
            txSucceed++;
            LOG.info("Netconf native rest test submit success");
            endTime = System.nanoTime();
        }catch (Exception e)
        {
            txError++;
        }
    }
}






