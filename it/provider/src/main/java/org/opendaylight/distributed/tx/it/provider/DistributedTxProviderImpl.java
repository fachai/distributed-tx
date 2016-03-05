package org.opendaylight.distributed.tx.it.provider;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.opendaylight.controller.md.sal.binding.api.*;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.common.api.data.*;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.api.DTxProvider;
import org.opendaylight.distributed.tx.it.provider.datawriter.AbstractDataWriter;
import org.opendaylight.distributed.tx.it.provider.datawriter.DataBrokerWrite;
import org.opendaylight.distributed.tx.it.provider.datawriter.DtxAsyncWrite;
import org.opendaylight.distributed.tx.it.provider.datawriter.DtxSyncWrite;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107.InterfaceActive;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107.InterfaceConfigurations;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfiguration;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfigurationBuilder;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.cfg.rev150107._interface.configurations.InterfaceConfigurationKey;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.oper.rev150107.InterfaceProperties;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.oper.rev150107._interface.properties.DataNodes;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.oper.rev150107._interface.properties.data.nodes.DataNode;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.oper.rev150107._interface.table.Interfaces;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.ios.xr.ifmgr.oper.rev150107._interface.table.interfaces.Interface;
import org.opendaylight.yang.gen.v1.http.cisco.com.ns.yang.cisco.xr.types.rev150119.InterfaceName;
import org.opendaylight.yang.gen.v1.urn.opendaylight.netconf.node.topology.rev150114.NetconfNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.netconf.node.topology.rev150114.network.topology.topology.topology.types.TopologyNetconf;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.*;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterListBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.OuterListKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerList;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerListBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.datastore.test.data.outer.list.InnerListKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.ds.naive.rollback.data.DsNaiveRollbackDataEntry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.ds.naive.rollback.data.DsNaiveRollbackDataEntryBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.ds.naive.rollback.data.DsNaiveRollbackDataEntryKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.ds.naive.test.data.DsNaiveTestDataEntry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.ds.naive.test.data.DsNaiveTestDataEntryBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.distributed.tx.it.model.rev150105.ds.naive.test.data.DsNaiveTestDataEntryKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NodeId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TopologyId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.NodeKey;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.Identifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.KeyedInstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class DistributedTxProviderImpl implements DistributedTxItModelService, DataChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedTxProviderImpl.class);
    DTxProvider dTxProvider;
    private ListenerRegistration<DataChangeListener> dclReg;
    private DataBroker dataBroker;
    private MountPointService mountService;
    Set<NodeId> nodeIdSet = new HashSet<>();
    Map<NodeId, List<InterfaceName>> nodeIfList = new HashMap<>();
    private DataBroker xrNodeBroker = null;
    private int couter = 0;
    //mark the status of the datastore test
    private final AtomicReference<TestStatus.ExecStatus> dsExecStatus = new AtomicReference<TestStatus.ExecStatus>( TestStatus.ExecStatus.Idle );
    //mark the status of the netconf test
    private final AtomicReference<TestStatus.ExecStatus> netconfExecStatus = new AtomicReference<TestStatus.ExecStatus>( TestStatus.ExecStatus.Idle );

    public static final InstanceIdentifier<Topology> NETCONF_TOPO_IID = InstanceIdentifier
            .create(NetworkTopology.class).child(
                    Topology.class,
                    new TopologyKey(new TopologyId(TopologyNetconf.QNAME
                            .getLocalName())));

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

    private boolean isTestIntf(String itfname) {
        if (itfname.contains("Giga") || itfname.contains("TenGig")) {
            return true;
        }
        return false;
    }

    private void initializeDsDataTree(){
        LOG.info("initilize ds data tree to data store");

        InstanceIdentifier<DsNaiveTestData> iid = InstanceIdentifier.create(DsNaiveTestData.class);

        if (this.dataBroker != null){
            dataBroker.registerDataChangeListener(LogicalDatastoreType.CONFIGURATION,
                    iid, new DsDataChangeListener(),
                    AsyncDataBroker.DataChangeScope.SUBTREE);
        }

        WriteTransaction transaction = dataBroker.newWriteOnlyTransaction();

        DsNaiveTestData dsTestData = new DsNaiveTestDataBuilder().build();

        transaction.put(LogicalDatastoreType.CONFIGURATION, iid, dsTestData);
        CheckedFuture<Void, TransactionCommitFailedException> cf = transaction.submit();

        Futures.addCallback(cf, new FutureCallback<Void>() {
            @Override
            public void onSuccess(@Nullable Void result) {
                LOG.info("initilize ds data tree to data store successfully");
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.info("initilize ds data tree to data store failure");
            }
        });
    }

    @Override
    public Future<RpcResult<DsNaiveTestOutput>> dsNaiveTest(DsNaiveTestInput input) {
        InstanceIdentifier<DsNaiveTestData> iid = InstanceIdentifier.create(DsNaiveTestData.class);

        Set<InstanceIdentifier<?>> iidSet = new HashSet<>();
        iidSet.add(iid);

        //int numberOfInterfaces = input.getNumberofinterfaces();
        int numberOfInterfaces = 1;
        boolean testRollback = false;
        //boolean testRollback = input.isRollback();
        String name = input.getName();
        iidSet.add(iid);

        DTx itDtx = this.dTxProvider.newTx(iidSet);

        for (int i = 0; i < numberOfInterfaces; i++) {
            DsNaiveTestDataEntryBuilder testEntryBuilder = new DsNaiveTestDataEntryBuilder();

            testEntryBuilder.setName(name + Integer.toString(i));

            DsNaiveTestDataEntry data = testEntryBuilder.build();

            InstanceIdentifier<DsNaiveTestDataEntry> entryIid = InstanceIdentifier.create(DsNaiveTestData.class)
                    .child(DsNaiveTestDataEntry.class, new DsNaiveTestDataEntryKey(input.getName() + Integer.toString(i)));

            CheckedFuture<Void, DTxException> cf = itDtx.putAndRollbackOnFailure(LogicalDatastoreType.CONFIGURATION, entryIid, data, iid);

            while (!cf.isDone()) ;
        }

        if (testRollback) {
            InstanceIdentifier<DsNaiveRollbackData> rollbackIid = InstanceIdentifier.create(DsNaiveRollbackData.class);

            DsNaiveRollbackDataEntryBuilder rollbackDataEntryBuilder = new DsNaiveRollbackDataEntryBuilder();
            DsNaiveRollbackDataEntry rolbackDataEntry = rollbackDataEntryBuilder.build();

            InstanceIdentifier<DsNaiveRollbackDataEntry> rollbackEntryIid = InstanceIdentifier.create(DsNaiveRollbackData.class)
                    .child(DsNaiveRollbackDataEntry.class, new DsNaiveRollbackDataEntryKey(input.getName()));

            CheckedFuture<Void, DTxException> cf = itDtx.putAndRollbackOnFailure(LogicalDatastoreType.CONFIGURATION, rollbackEntryIid, rolbackDataEntry, iid);
        }

        if (!testRollback)
            itDtx.submit();

        DsNaiveTestOutput output = new DsNaiveTestOutputBuilder().setResult("Bingo").build();

        return Futures.immediateFuture(RpcResultBuilder.success(output).build());
    }

    @Override
    public Future<RpcResult<MixedNaiveTestOutput>> mixedNaiveTest(MixedNaiveTestInput input) {
        List<NodeId> nodeIdList = new ArrayList(this.nodeIdSet);
        boolean testRollback = false;
        String name = input.getName();

        if(name.length() > 5)
            testRollback = true;

        int number = new Random().nextInt(100);
        int keyNumber = number;
        boolean doSumbit = true;

        // IID set for netconf
        Set<InstanceIdentifier<?>> txIidSet = new HashSet<>();

        for(NodeId n : nodeIdList) {
            InstanceIdentifier msNodeId = NETCONF_TOPO_IID.child(Node.class, new NodeKey(n));

            txIidSet.add(msNodeId);
        }

        // IID for data store
        InstanceIdentifier<DsNaiveTestData> dataStoreNodeId = InstanceIdentifier.create(DsNaiveTestData.class);

        Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> m = new HashMap<>();
        m.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, txIidSet);
        Set<InstanceIdentifier<?>> dataStoreSet = new HashSet<>();
        dataStoreSet.add(dataStoreNodeId);
        m.put(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreSet);

        // Initialize DTX
        DTx itDtx = this.dTxProvider.newTx(m);

        InstanceIdentifier<InterfaceConfigurations> netconfIid = InstanceIdentifier.create(InterfaceConfigurations.class);


        for(NodeId n : nodeIdList) {
            InstanceIdentifier msNodeId = NETCONF_TOPO_IID.child(Node.class, new NodeKey(n));

            InterfaceName ifname = this.nodeIfList.get(n).get(0);

            final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                    = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), ifname));

            final InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
            interfaceConfigurationBuilder.setInterfaceName(ifname);
            interfaceConfigurationBuilder.setDescription("Test description" + "-" + input.getName() + "-" + Integer.toString(number));
            interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));

            InterfaceConfiguration config = interfaceConfigurationBuilder.build();
            LOG.info("FM: writing to node {} ifc {} ", n.getValue(), ifname.getValue());

            CheckedFuture<Void, DTxException> done = null;
                 done = itDtx.putAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config, msNodeId);
             /*
            else if(this.couter % 3 == 1) {
                done = itDtx.mergeAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config, msNodeId);
            }else{
                 done = itDtx.deleteAndRollbackOnFailure(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, msNodeId);
            }
            */

            while (!done.isDone()) {
                Thread.yield();
            }

            try {
                done.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
                doSumbit = false;
            } catch (ExecutionException e) {
                e.printStackTrace();
                doSumbit = false;
            }
        }

        this.couter++;

        // Write to data store
        LOG.info("FM: now writing to data store");

        DsNaiveTestDataEntryBuilder testEntryBuilder = new DsNaiveTestDataEntryBuilder();
        testEntryBuilder.setName(name + Integer.toString(number));
        DsNaiveTestDataEntry data = testEntryBuilder.build();

        if (testRollback) {
            keyNumber = 101;
        }
        InstanceIdentifier<DsNaiveTestDataEntry> entryIid = InstanceIdentifier.create(DsNaiveTestData.class)
                .child(DsNaiveTestDataEntry.class, new DsNaiveTestDataEntryKey(input.getName() + Integer.toString(keyNumber)));

        CheckedFuture<Void, DTxException> cf = itDtx.putAndRollbackOnFailure(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                LogicalDatastoreType.CONFIGURATION, entryIid, data, dataStoreNodeId);

        while(!cf.isDone()){Thread.yield();}

        if(doSumbit) {
            try {
                cf.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
                doSumbit = false;
            } catch (ExecutionException e) {
                e.printStackTrace();
                doSumbit = false;
                LOG.info("FM: get 3");
            }
        }
        if(!testRollback) {
            if (doSumbit) {
                CheckedFuture<Void, TransactionCommitFailedException> submitFuture = itDtx.submit();
                LOG.info("submit done");
            } else {
                LOG.info("put failure. no submit");
            }
        }

        MixedNaiveTestOutput output = new MixedNaiveTestOutputBuilder().setResult("Bingo").build();

        return Futures.immediateFuture(RpcResultBuilder.success(output).build());
    }

    @Override
    public Future<RpcResult<NaiveTestOutput>> naiveTest(NaiveTestInput input) {
        List<NodeId> nodeIdList = new ArrayList(this.nodeIdSet);
        int numberOfInterfaces = 1;
        // int numberOfInterfaces = input.getNumberofinterfaces();
        boolean testRollback = false;
        // boolean testRollback = input.isRollback();
        String name = input.getName();

        InstanceIdentifier msNodeId = NETCONF_TOPO_IID.child(Node.class, new NodeKey(nodeIdList.get(0)));

        Set<InstanceIdentifier<?>> txIidSet = new HashSet<>();
        txIidSet.add(msNodeId);
        InstanceIdentifier<InterfaceConfigurations> iid = InstanceIdentifier.create(InterfaceConfigurations.class);

        for(int i = 0; i < numberOfInterfaces; i++) {
            InterfaceName ifname = null;
            for(NodeId n : nodeIdList) {
                ifname = this.nodeIfList.get(n).get(0);
                break;
            }

            final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                    = iid.child(InterfaceConfiguration.class,
                    new InterfaceConfigurationKey(new InterfaceActive("act"), ifname));

            final InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
            interfaceConfigurationBuilder.setInterfaceName(ifname);
            interfaceConfigurationBuilder.setDescription("Test description" + "-" + input.getName());
            interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));

            InterfaceConfiguration config = interfaceConfigurationBuilder.build();

            LOG.info("dtx ifc {}", ifname.toString());

            if (name.length() < 6) {
                final ReadWriteTransaction xrNodeReadTx = xrNodeBroker.newReadWriteTransaction();

                xrNodeReadTx.put(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config);

                final CheckedFuture<Void, TransactionCommitFailedException> submit = xrNodeReadTx
                        .submit();

                Futures.addCallback(submit, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(final Void result) {
                        LOG.info("Success to commit interface changes {} ");
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        LOG.info("Fail to commit interface changes");
                    }
                });
            } else {
                DTx itDtx = this.dTxProvider.newTx(txIidSet);

                CheckedFuture<Void, DTxException> done = itDtx.putAndRollbackOnFailure(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config, msNodeId);

                int cnt = 0;

                while (!done.isDone()) {
                    Thread.yield();
                }

                boolean doSumbit = true;

                try {
                    done.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    doSumbit = false;
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    doSumbit = false;
                }

                if (doSumbit) {
                    CheckedFuture<Void, TransactionCommitFailedException> submitFuture = itDtx.submit();
                    LOG.info("submit done");
                } else {
                    LOG.info("put failure. no submit");
                }
            }
        }

        NaiveTestOutput output = new NaiveTestOutputBuilder().setResult("Bingo").build();

        return Futures.immediateFuture(RpcResultBuilder.success(output).build());
    }

    public DistributedTxProviderImpl(DTxProvider provider, DataBroker db, MountPointService ms){
        this.dTxProvider = provider;
        this.dataBroker = db;
        this.mountService = ms;
        this.dclReg = dataBroker.registerDataChangeListener(
                LogicalDatastoreType.OPERATIONAL,
                NETCONF_TOPO_IID.child(Node.class), this,
                AsyncDataBroker.DataChangeScope.SUBTREE);

        this.initializeDsDataTree();
    }

    @Override
    public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
        DataObject dataObject = change.getUpdatedSubtree();

        LOG.info("NetconftestProvider onDataChange, change: {} ", dataObject);

        for ( Map.Entry<InstanceIdentifier<?>,
                        DataObject> entry : change.getCreatedData().entrySet()) {
            if (entry.getKey().getTargetType() == NetconfNode.class) {
                NodeId nodeId = getNodeId(entry.getKey());
                LOG.info("NETCONF Node: {} was created", nodeId.getValue());
            }
        }

        for ( Map.Entry<InstanceIdentifier<?>,
                        DataObject> entry : change.getUpdatedData().entrySet()) {
            if (entry.getKey().getTargetType() == NetconfNode.class) {
                NodeId nodeId = getNodeId(entry.getKey());

                LOG.info("NETCONF Node: {} is fully connected", nodeId.getValue());

                InstanceIdentifier<DataNodes> iid = InstanceIdentifier.create(
                             InterfaceProperties.class).child(DataNodes.class);

                final Optional<MountPoint> xrNodeOptional = mountService
                                .getMountPoint(NETCONF_TOPO_IID.child(Node.class,
                                        new NodeKey(nodeId)));
                final MountPoint xrNode = xrNodeOptional.get();

                if(xrNodeBroker == null) {
                    xrNodeBroker = xrNode.getService(DataBroker.class).get();
                }

                if(nodeId.getValue().contains("sdn")) {
                    this.nodeIdSet.add(nodeId);
                    if (!this.nodeIfList.containsKey(nodeId))
                        this.nodeIfList.put(nodeId, new ArrayList<InterfaceName>());
                }

                final ReadOnlyTransaction xrNodeReadTx = xrNodeBroker
                        .newReadOnlyTransaction();
                Optional<DataNodes> ldn;
                try {
                    ldn = xrNodeReadTx.read(LogicalDatastoreType.OPERATIONAL, iid).checkedGet();
                } catch (ReadFailedException e) {
                    throw new IllegalStateException(
                            "Unexpected error reading data from " + nodeId.getValue(), e);
                }
                if (ldn.isPresent()) {
                    LOG.info("interfaces: {}", ldn.get());
                    List<DataNode> dataNodes = ldn.get().getDataNode();
                    for (DataNode node : dataNodes) {

                        LOG.info("DataNode '{}'", node.getDataNodeName().getValue());

                        Interfaces ifc = node.getSystemView().getInterfaces();
                        List<Interface> ifList = ifc.getInterface();
                        for (Interface intf : ifList) {
                            if (isTestIntf(intf.getInterfaceName().toString())) {
                                LOG.info("add interface {}", intf.getInterfaceName().toString());

                                if(this.nodeIdSet.contains(nodeId)) {
                                    this.nodeIfList.get(nodeId).add(intf.getInterfaceName());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * this method is used to create the data structure for the benchmark test
     */
    public boolean initializeDataStoreForBenchmark(final int outerElements)
    {
        LOG.info("initialize the datastore data tree for benchmark");
        InstanceIdentifier<DatastoreTestData> iid = InstanceIdentifier.create(DatastoreTestData.class);

        WriteTransaction transaction = this.dataBroker.newWriteOnlyTransaction();
        DatastoreTestData datastoreTestData = new DatastoreTestDataBuilder().build();
        transaction.put(LogicalDatastoreType.CONFIGURATION, iid, datastoreTestData);
        CheckedFuture<Void, TransactionCommitFailedException> cf = transaction.submit();

        try{
            cf.checkedGet();
        }catch (Exception e)
        {
            return false;
        }

        for (OuterList outerList : buildOuterList(outerElements)) {
            transaction = this.dataBroker.newWriteOnlyTransaction();
            InstanceIdentifier<OuterList> outerIid = InstanceIdentifier.create(DatastoreTestData.class)
                    .child(OuterList.class, outerList.getKey());

            transaction.put(LogicalDatastoreType.CONFIGURATION, outerIid, outerList);
            CheckedFuture<Void, TransactionCommitFailedException> submitFut = transaction.submit();
            try {
                submitFut.checkedGet();
            } catch (Exception e)
            {
                return false;
            }
        }
        return true;
    }

    /*
    this method create the specific number of empty outerList
     */
    private List<OuterList> buildOuterList(int outerElements) {
        List<OuterList> outerList = new ArrayList<OuterList>(outerElements);
        for (int j = 0; j < outerElements; j++) {
            outerList.add(new OuterListBuilder()
                    .setId( j )
                    .setInnerList(Collections.<InnerList>emptyList())
                    .setKey(new OuterListKey( j ))
                    .build());
        }

        return outerList;
    }


    @Override
    public Future<RpcResult<BenchmarkTestOutput>> benchmarkTest(BenchmarkTestInput input) {
        if (input.getLogicalTxType() == BenchmarkTestInput.LogicalTxType.DATASTORE)
            return dsBenchmarkTest(input);
        else
            return netconfBenchmarkTest(input);
    }

    public Future<RpcResult<BenchmarkTestOutput>> dsBenchmarkTest(BenchmarkTestInput input) {
        LOG.info("Starting the datastore benchmark test for dtx and data broker");
//        Check if there is a test in progress
        if (dsExecStatus.compareAndSet(TestStatus.ExecStatus.Idle, TestStatus.ExecStatus.Executing) == false)
        {
            LOG.info("Test in progress");
            return RpcResultBuilder
                    .success(new BenchmarkTestOutputBuilder()
                    .setStatus(BenchmarkTestOutput.Status.TESTINPROGRESS)
                    .build()).buildFuture();

        }

        long dbTime = 0, dtxSyncTime = 0, dtxAsyncTime = 0; //test time for the corresponding test
        int outerElements = input.getOuterList(), innerElements = input.getInnerList();

        //create the nodeSet for the dtx
        Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> m = new HashMap<>();
        Set<InstanceIdentifier<?>> dsNodeSet = Sets.newHashSet();
        dsNodeSet.add(InstanceIdentifier.create(DatastoreTestData.class));
        m.put(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dsNodeSet);

        Long loopTime = input.getLoop();

        long dbOk = 0, dTxSyncOk = 0, dTxAyncOk = 0, errorCase = 0;
        for (int i = 0; i < loopTime ; i++) {

            DataBrokerWrite dbWrite = new DataBrokerWrite(input, dataBroker, outerElements, innerElements);
            DtxSyncWrite dTxSnycWrite = new DtxSyncWrite(input, dTxProvider, dataBroker, m, outerElements, innerElements);
            DtxAsyncWrite dTxAsyncWrite = new DtxAsyncWrite(input, dTxProvider, dataBroker, m, outerElements, innerElements);
            //initialize the data store
            if (!initializeDataStoreForBenchmark(outerElements))
            {
                LOG.info("can't initialize data store");
                errorCase ++;
                continue;
            }

            //dataBroker test
            try {
                dbWrite.writeData();
                dTxSnycWrite.writeData();
                dTxAsyncWrite.writeData();
            }catch (Exception e)
            {
                LOG.error( "Test error: {}", e.toString());
                errorCase++;
            }

            dbOk += dbWrite.getTxSucceed(); //number of databroker successful submit
            dTxSyncOk += dTxSnycWrite.getTxSucceed(); //number of dtx sync successful submit
            dTxAyncOk += dTxAsyncWrite.getTxSucceed(); //number of dtx async successful submit

            dbTime +=dbWrite.getExecTime()/(outerElements * innerElements);
            dtxSyncTime +=dTxSnycWrite.getExecTime()/(outerElements * innerElements);
            dtxAsyncTime += dTxAsyncWrite.getExecTime()/(outerElements * innerElements);

        }
        dsExecStatus.set(TestStatus.ExecStatus.Idle);
        LOG.info("Data store test success");
        if (loopTime != errorCase) {
            return RpcResultBuilder
                    .success(new BenchmarkTestOutputBuilder()
                            .setStatus(BenchmarkTestOutput.Status.OK)
                            .setExecTime(dbTime/(loopTime - errorCase))
                            .setDtxSyncExecTime(dtxSyncTime/(loopTime - errorCase))
                            .setDtxAsyncExecTime(dtxAsyncTime/(loopTime - errorCase))
                            .setDbOk(dbOk)
                            .setDTxSyncOk(dTxSyncOk)
                            .setDTxAyncOk(dTxAyncOk)
                            .build()).buildFuture();
        }else{
            return RpcResultBuilder
                    .success(new BenchmarkTestOutputBuilder()
                             .setStatus(BenchmarkTestOutput.Status.FAILED)
                             .build()).buildFuture();
        }
    }

    public Future<RpcResult<BenchmarkTestOutput>> netconfBenchmarkTest (BenchmarkTestInput input) {
        //netconf test
        return null;
    }
    private class DsDataChangeListener implements DataChangeListener{
        @Override
        public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> asyncDataChangeEvent) {
            DataObject dataObject = asyncDataChangeEvent.getUpdatedSubtree();

            LOG.info("DS DsDataChangeListenClass on changed called {}", dataObject);
        }
    }
}
