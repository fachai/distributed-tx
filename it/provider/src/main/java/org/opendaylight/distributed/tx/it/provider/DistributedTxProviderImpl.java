package org.opendaylight.distributed.tx.it.provider;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.opendaylight.controller.md.sal.binding.api.*;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxProvider;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfiguration;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfigurationBuilder;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfigurationKey;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.oper.rev150107.InterfaceProperties;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.oper.rev150107._interface.properties.DataNodes;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.xr.types.rev150119.InterfaceName;
import org.opendaylight.yang.gen.v1.urn.opendaylight.netconf.node.topology.rev150114.NetconfNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.netconf.node.topology.rev150114.network.topology.topology.topology.types.TopologyNetconf;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.DistributedTxItModelService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.NaiveTestInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.NaiveTestOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.NaiveTestOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev130712.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev130712.NodeId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev130712.TopologyId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev130712.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev130712.network.topology.TopologyKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev130712.network.topology.topology.Node;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev130712.network.topology.topology.NodeKey;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.Identifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.KeyedInstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedTxProviderImpl implements DistributedTxItModelService, DataChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedTxProviderImpl.class);
    DTxProvider dTxProvider;
    private ListenerRegistration<DataChangeListener> dclReg;
    private DataBroker dataBroker;
    private MountPointService mountService;

    public static final InstanceIdentifier<Topology> NETCONF_TOPO_IID = InstanceIdentifier
            .create(NetworkTopology.class).child(
                    Topology.class,
                    new TopologyKey(new TopologyId(TopologyNetconf.QNAME
                            .getLocalName())));

    private void writeNetconfNode(NodeId nodeId, DataBroker db){
        final ReadWriteTransaction xrNodeReadTx = db.newReadWriteTransaction();

        InstanceIdentifier<InterfaceConfigurations> iid = InstanceIdentifier.create(InterfaceConfigurations.class);

        InterfaceName ifname = new InterfaceName("tmp");

        final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid = iid
                .child(InterfaceConfiguration.class,
                        new InterfaceConfigurationKey(
                                new InterfaceActive("act"),
                                ifname));

        final InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
        interfaceConfigurationBuilder.setInterfaceName(ifname);
        interfaceConfigurationBuilder.setDescription("Test description" + "-" + ifname.toString());
        interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));

        InterfaceConfiguration config_new = interfaceConfigurationBuilder.build();

        xrNodeReadTx.put(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config_new);

        final CheckedFuture<Void, TransactionCommitFailedException> submit = xrNodeReadTx
                .submit();

        Futures.addCallback(submit, new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
                final String des = "test des";
                LOG.info("Success to commit interface changes {} ", des);
            }

            @Override
            public void onFailure(final Throwable t) {
                LOG.info("Fail to commit interface changes");
            }
        });
    }

    private NodeId getNodeId(final InstanceIdentifier<?> path) {
        for (InstanceIdentifier.PathArgument pathArgument : path.getPathArguments()) {
            if (pathArgument instanceof InstanceIdentifier.IdentifiableItem<?, ?>) {

                final Identifier key = ((InstanceIdentifier.IdentifiableItem) pathArgument).getKey();
                if(key instanceof NodeKey) {
                    return ((NodeKey) key).getNodeId();
                }
            }
        }
        return null;
    }

    private boolean isNetconfInternalNode(String infname) {
        if (infname.contains("controller-config")) {
            return true;
        }
        return false;
    }

    private boolean isTestIntf(String itfname) {
        if (itfname.contains("Giga") || itfname.contains("TenGig")) {
            return true;
        }
        return false;
    }

    @Override
    public Future<RpcResult<NaiveTestOutput>> naiveTest(NaiveTestInput input) {
        Set<InstanceIdentifier<?>> iidSet = new HashSet<>();

        // NodeId nodeId = getNodeId(entry.getKey());
        NodeId nodeId = null;

        InstanceIdentifier msNodeId = NETCONF_TOPO_IID.child(Node.class, new NodeKey(nodeId));

        iidSet.add(msNodeId);

        DTx itDtx = this.dTxProvider.newTx(iidSet);

        InstanceIdentifier<InterfaceConfigurations> iid = InstanceIdentifier.create(InterfaceConfigurations.class);

        InterfaceName ifname = new InterfaceName("tmp");

        final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid = iid
                .child(InterfaceConfiguration.class,
                        new InterfaceConfigurationKey(
                                new InterfaceActive("act"),
                                ifname));

        InterfaceConfiguration ifcConfig = new InterfaceConfigurationBuilder()
                .setInterfaceName(ifname).setDescription("test description")
                .setActive(new InterfaceActive("act")).build();

        itDtx.putAndRollbackOnFailure(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, ifcConfig, msNodeId);

        CheckedFuture<Void, TransactionCommitFailedException> submitFuture = itDtx.submit();


        if(submitFuture.isDone()){}

        NaiveTestOutput output = new NaiveTestOutputBuilder().setResult("Bingo").build();

        return Futures.immediateFuture(RpcResultBuilder.success(output).build());
    }

    public DistributedTxProviderImpl(DTxProvider provider, DataBroker db, MountPointService ms){
        this.dTxProvider = provider;
        this.dTxProvider.test();
        this.dataBroker = db;
        this.mountService = ms;
        this.dclReg = dataBroker.registerDataChangeListener(
                LogicalDatastoreType.OPERATIONAL,
                NETCONF_TOPO_IID.child(Node.class), this,
                AsyncDataBroker.DataChangeScope.SUBTREE);
    }

    @Override
    public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
        LOG.info("NetconftestProvider onDataChange, change: ");

        for ( Map.Entry<InstanceIdentifier<?>,
                        DataObject> entry : change.getCreatedData().entrySet()) {
            if (entry.getKey().getTargetType() == NetconfNode.class) {
                NodeId nodeId = getNodeId(entry.getKey());
                LOG.info("NETCONF Node: {} was created", nodeId.getValue());

                // Not much can be done at this point, we need UPDATE event with
                // state set to connected
            }
        }

        for ( Map.Entry<InstanceIdentifier<?>,
                        DataObject> entry : change.getUpdatedData().entrySet()) {
            if (entry.getKey().getTargetType() == NetconfNode.class) {
                NodeId nodeId = getNodeId(entry.getKey());
                DataBroker xrNodeBroker;

                NetconfNode nnode = (NetconfNode)entry.getValue();
                LOG.info("NETCONF Node: {} is fully connected", nodeId.getValue());

                if (this.isNetconfInternalNode(nodeId.getValue())) {
                    LOG.info("NETCONF Node: {} is fully connected and skipped FB handling",
                                    nodeId.getValue());
                            break;
                }

                InstanceIdentifier<DataNodes> iid = InstanceIdentifier.create(
                             InterfaceProperties.class).child(DataNodes.class);

                final Optional<MountPoint> xrNodeOptional = mountService
                                .getMountPoint(NETCONF_TOPO_IID.child(Node.class,
                                        new NodeKey(nodeId)));
                final MountPoint xrNode = xrNodeOptional.get();
                xrNodeBroker = xrNode.getService(DataBroker.class).get();

                writeNetconfNode(nodeId, xrNodeBroker);
            }
        }
    }
}
