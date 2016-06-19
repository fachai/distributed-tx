/*
 * Copyright (c) 2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.distributed.tx.it.provider;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.CheckedFuture;
import org.opendaylight.controller.md.sal.binding.api.*;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.common.api.data.*;
import org.opendaylight.distributed.tx.api.DTXLogicalTXProviderType;
import org.opendaylight.distributed.tx.api.DTx;
import org.opendaylight.distributed.tx.api.DTxException;
import org.opendaylight.distributed.tx.api.DTxProvider;
import org.opendaylight.distributed.tx.it.provider.datawriter.*;
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
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class DistributedTxProviderImpl implements DistributedTxItModelService, DataChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedTxProviderImpl.class);
    DTxProvider dTxProvider;
    private ListenerRegistration<DataChangeListener> dclReg;
    private DataBroker dataBroker;
    private MountPointService mountService;
    Set<NodeId> nodeIdSet = new HashSet<>();
    Map<NodeId, List<InterfaceName>> nodeIfList = new HashMap<>();
    private Map<String, DataBroker> xrNodeBrokerMap = new HashMap<>();
    private final AtomicReference<TestStatus.ExecStatus> dsPerformanceTestStatus = new AtomicReference<>(TestStatus.ExecStatus.Idle);
    private final AtomicReference<TestStatus.ExecStatus> netConfTestStatus = new AtomicReference<>(TestStatus.ExecStatus.Idle);
    private final AtomicReference<TestStatus.ExecStatus> dsTestStatus = new AtomicReference<>(TestStatus.ExecStatus.Idle);
    private final AtomicReference<TestStatus.ExecStatus> mixedProviderTestStatus = new AtomicReference<>(TestStatus.ExecStatus.Idle);
    public static final InstanceIdentifier<Topology> NETCONF_TOPO_IID = InstanceIdentifier
            .create(NetworkTopology.class).child(
                    Topology.class,
                    new TopologyKey(new TopologyId(TopologyNetconf.QNAME
                            .getLocalName())));

    private NodeId getNodeId(final InstanceIdentifier<?> path) {
        for (InstanceIdentifier.PathArgument pathArgument : path.getPathArguments()) {
            if (pathArgument instanceof InstanceIdentifier.IdentifiableItem<?, ?>) {

                final Identifier key = ((InstanceIdentifier.IdentifiableItem) pathArgument).getKey();
                if (key instanceof NodeKey) {
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

    public DistributedTxProviderImpl(DTxProvider provider, DataBroker db, MountPointService ms) {
        this.dTxProvider = provider;
        this.dataBroker = db;
        this.mountService = ms;
        this.dclReg = dataBroker.registerDataChangeListener(
                LogicalDatastoreType.OPERATIONAL,
                NETCONF_TOPO_IID.child(Node.class), this,
                AsyncDataBroker.DataChangeScope.SUBTREE);
    }

    @Override
    public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
        DataObject dataObject = change.getUpdatedSubtree();

        LOG.info("NetConftestProvider onDataChange, change: {} ", dataObject);

        for (Map.Entry<InstanceIdentifier<?>,
                DataObject> entry : change.getCreatedData().entrySet()) {
            if (entry.getKey().getTargetType() == NetconfNode.class) {
                NodeId nodeId = getNodeId(entry.getKey());
                LOG.info("NETCONF Node: {} was created", nodeId.getValue());
            }
        }

        for (Map.Entry<InstanceIdentifier<?>,
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

                DataBroker xrNodeBroker = xrNode.getService(DataBroker.class).get();

                if (nodeId.getValue().contains("sdn")) {
                    xrNodeBrokerMap.put(nodeId.getValue(), xrNodeBroker);
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
                                LOG.info("Add interface {}", intf.getInterfaceName().toString());

                                if (this.nodeIdSet.contains(nodeId)) {
                                    this.nodeIfList.get(nodeId).add(intf.getInterfaceName());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public boolean initializeDataStoreTestData(int outerElements) {
        LOG.info("Initialize datastore data tree for performance test");
        InstanceIdentifier<DatastoreTestData> iid = InstanceIdentifier.create(DatastoreTestData.class);

        WriteTransaction transaction = this.dataBroker.newWriteOnlyTransaction();
        DatastoreTestData datastoreTestData = new DatastoreTestDataBuilder().build();
        transaction.put(LogicalDatastoreType.CONFIGURATION, iid, datastoreTestData);
        CheckedFuture<Void, TransactionCommitFailedException> cf = transaction.submit();

        try {
            cf.checkedGet();
        } catch (Exception e) {
            return false;
        }

        List<OuterList> outerLists = buildOuterList(outerElements);
        for (OuterList outerList : outerLists) {
            transaction = this.dataBroker.newWriteOnlyTransaction();
            InstanceIdentifier<OuterList> outerIid = InstanceIdentifier.create(DatastoreTestData.class)
                    .child(OuterList.class, outerList.getKey());

            transaction.put(LogicalDatastoreType.CONFIGURATION, outerIid, outerList);
            CheckedFuture<Void, TransactionCommitFailedException> submitFut = transaction.submit();
            try {
                submitFut.checkedGet();
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    private List<OuterList> buildOuterList(int outerElements) {
        List<OuterList> outerList = new ArrayList<OuterList>(outerElements);
        for (int j = 0; j < outerElements; j++) {
            outerList.add(new OuterListBuilder()
                    .setId(j)
                    .setInnerList(Collections.<InnerList>emptyList())
                    .setKey(new OuterListKey(j))
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
        LOG.info("DTx datastore performance test begin");

        if (dsPerformanceTestStatus.compareAndSet(TestStatus.ExecStatus.Idle, TestStatus.ExecStatus.Executing) == false) {
            return RpcResultBuilder
                    .success(new BenchmarkTestOutputBuilder()
                            .setStatus(StatusType.TESTINPROGRESS)
                            .build()).buildFuture();
        }

        long dbTime = 0, dtxSyncTime = 0, dtxAsyncTime = 0;
        int outerElements = input.getOuterList();

        Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap = new HashMap<>();
        Set<InstanceIdentifier<?>> dsNodeSet = Sets.newHashSet();
        dsNodeSet.add(InstanceIdentifier.create(DatastoreTestData.class));
        nodesMap.put(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dsNodeSet);

        int loopTime = input.getLoop();

        int dbOk = 0, dTxSyncOk = 0, dTxAyncOk = 0, errorCase = 0;
        for (int i = 0; i < loopTime; i++) {
            DataBrokerDataStoreWriter dbWrite = new DataBrokerDataStoreWriter(input, dataBroker);
            DtxDataStoreSyncWriter dTxSnycWrite = new DtxDataStoreSyncWriter(input, dTxProvider, dataBroker, nodesMap);
            DtxDataStoreAsyncWriter dTxAsyncWrite = new DtxDataStoreAsyncWriter(input, dTxProvider, dataBroker, nodesMap);

            if (!initializeDataStoreTestData(outerElements)) {
                LOG.info("Can't initialize data store for data broker test");
                errorCase++;
                continue;
            }
            try {
                dbWrite.writeData();
            } catch (Exception e) {
                LOG.error("Data broker test error: {}", e.toString());
                errorCase++;
            }

            if (!initializeDataStoreTestData(outerElements)) {
                LOG.info("can't initialize data store for dtx sync test");
                errorCase++;
                continue;
            }

            try {
                dTxSnycWrite.writeData();
            } catch (Exception e) {
                LOG.info("Dtx Sync test Error: {}", e.toString());
                errorCase++;
            }

            dbOk += dbWrite.getTxSucceed();
            dTxSyncOk += dTxSnycWrite.getTxSucceed();
            dTxAyncOk += dTxAsyncWrite.getTxSucceed();

            dbTime += dbWrite.getExecTime();
            dtxSyncTime += dTxSnycWrite.getExecTime();
            dtxAsyncTime += dTxAsyncWrite.getExecTime();

        }
        dsPerformanceTestStatus.set(TestStatus.ExecStatus.Idle);
        LOG.info("Datastore test finish");
        if (loopTime != errorCase) {
            return RpcResultBuilder
                    .success(new BenchmarkTestOutputBuilder()
                            .setStatus(StatusType.OK)
                            .setExecTime(dbTime / (loopTime - errorCase))
                            .setDtxSyncExecTime(dtxSyncTime / (loopTime - errorCase))
                            .setDtxAsyncExecTime(dtxAsyncTime / (loopTime - errorCase))
                            .setDbOk(dbOk)
                            .setDTxSyncOk(dTxSyncOk)
                            .setDTxAsyncOk(dTxAyncOk)
                            .build()).buildFuture();
        } else {
            return RpcResultBuilder
                    .success(new BenchmarkTestOutputBuilder()
                            .setStatus(StatusType.FAILED)
                            .build()).buildFuture();
        }
    }

    public Future<RpcResult<BenchmarkTestOutput>> netconfBenchmarkTest(BenchmarkTestInput input) {
        LOG.info("DTx netconf performance test begin");

        if (netConfTestStatus.compareAndSet(TestStatus.ExecStatus.Idle, TestStatus.ExecStatus.Executing) == false) {
            return RpcResultBuilder
                    .success(new BenchmarkTestOutputBuilder()
                            .setStatus(StatusType.TESTINPROGRESS).
                                    build()).buildFuture();
        }

        long nativeNetconfTime = 0, dtxNetconfSyncTime = 0, dtxNetconfAsyncTime = 0;
        long loopTimes = 1;
        int dbOk = 0, dTxSyncOk = 0, dTxAyncOk = 0, errorCase = 0;
        DataBroker xrNodeBroker = xrNodeBrokerMap.get("sdn-1");
        for (int i = 0; i < loopTimes; i++) {
            DataBrokerNetConfWriter databroker = new DataBrokerNetConfWriter(input, xrNodeBroker, nodeIdSet, nodeIfList);
            DtxNetConfSyncWriter dtxNetconfSync = new DtxNetConfSyncWriter(input, xrNodeBroker, dTxProvider, nodeIdSet, nodeIfList);
            DtxNetconfAsyncWriter dtxNetconfAsync = new DtxNetconfAsyncWriter(input, xrNodeBroker, dTxProvider, nodeIdSet, nodeIfList);

            try {
                databroker.writeData();
            } catch (Exception e) {
                LOG.error("Data broker test error: {}", e.toString());
                errorCase++;
                continue;
            }

            try {
                dtxNetconfSync.writeData();
            } catch (Exception e) {
                LOG.error("Dtx netconf sync test error: {}", e.toString());
                errorCase++;
                continue;
            }

            try {
                dtxNetconfAsync.writeData();
            } catch (Exception e) {
                LOG.error("Dtx netconf async test error: {}", e.toString());
                errorCase++;
                continue;
            }

            dbOk += databroker.getTxSucceed();
            dTxSyncOk += dtxNetconfSync.getTxSucceed();
            dTxAyncOk += dtxNetconfAsync.getTxSucceed();

            nativeNetconfTime += databroker.getExecTime();
            dtxNetconfSyncTime += dtxNetconfSync.getExecTime();
            dtxNetconfAsyncTime += dtxNetconfAsync.getExecTime();
        }

        netConfTestStatus.set(TestStatus.ExecStatus.Idle);
        LOG.info("Netconf test finish");
        if (loopTimes != errorCase) {
            return RpcResultBuilder
                    .success(new BenchmarkTestOutputBuilder()
                            .setStatus(StatusType.OK)
                            .setExecTime(nativeNetconfTime / (loopTimes - errorCase))
                            .setDtxSyncExecTime(dtxNetconfSyncTime / (loopTimes - errorCase))
                            .setDtxAsyncExecTime(dtxNetconfAsyncTime / (loopTimes - errorCase))
                            .setDbOk(dbOk)
                            .setDTxAsyncOk(dTxAyncOk)
                            .setDTxSyncOk(dTxSyncOk)
                            .build()).buildFuture();
        } else {
            return RpcResultBuilder.success(new BenchmarkTestOutputBuilder().setStatus(StatusType.FAILED)
                    .build()).buildFuture();
        }
    }

    @Override
    public Future<RpcResult<DatastoreTestOutput>> datastoreTest(DatastoreTestInput input) {
        LOG.info("DTx datastore test begin");
        if (dsTestStatus.compareAndSet(TestStatus.ExecStatus.Idle, TestStatus.ExecStatus.Executing) == false) {
            return RpcResultBuilder
                    .success(new DatastoreTestOutputBuilder()
                            .setStatus(StatusType.TESTINPROGRESS)
                            .build()).buildFuture();
        }

        int putsPerTx = input.getPutsPerTx();
        int outerElements = input.getOuterList(), innerElements = input.getInnerList();
        OperationType operation = input.getOperation();
        Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap = new HashMap<>();
        Set<InstanceIdentifier<?>> nodeIdSet = new HashSet<>();
        nodeIdSet.add(InstanceIdentifier.create(DatastoreTestData.class));
        nodesMap.put(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, nodeIdSet);

        if (!initializeDataStoreTestData(outerElements)) {
            LOG.info("Can't initialize test data for DTx datastore test");
            dsTestStatus.set(TestStatus.ExecStatus.Idle);
            return RpcResultBuilder
                    .success(new DatastoreTestOutputBuilder()
                            .setStatus(StatusType.FAILED)
                            .build()).buildFuture();
        }

        DTx dTx = dTxProvider.newTx(nodesMap);
        DataStoreListBuilder dataStoreListBuilder = new DataStoreListBuilder(dataBroker, outerElements, innerElements);

        if (operation == OperationType.DELETE)
            dataStoreListBuilder.buildTestInnerList();

        List<OuterList> outerLists = dataStoreListBuilder.buildOuterList();
        InstanceIdentifier<DatastoreTestData> nodeId = InstanceIdentifier.create(DatastoreTestData.class);
        long count = 0;
        boolean testSucceed = true;
        if (input.getType() != TestType.NORMAL) {
            if (input.getType() == TestType.ROLLBACKONFAILURE) {
                LOG.info("DTx datastore rollback on failure test begin");
                InstanceIdentifier<InnerList> errorInnerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, new OuterListKey(outerElements))
                        .child(InnerList.class, new InnerListKey(0));

                InnerList errorInnerlist = new InnerListBuilder()
                        .setKey(new InnerListKey(0))
                        .setValue("Error InnerList")
                        .setName(0)
                        .build();
                int errorOccur = (int) (putsPerTx * (Math.random())) + 1;//ensure the errorOccur not be zero

                for (OuterList outerList : outerLists) {
                    for (InnerList innerList : outerList.getInnerList()) {
                        InstanceIdentifier<InnerList> InnerIid = getInstanceIdentifier(outerList, innerList);
                        CheckedFuture<Void, DTxException> writeFuture = writeData(dTx, operation, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                LogicalDatastoreType.CONFIGURATION, InnerIid, nodeId, innerList);

                        try {
                            writeFuture.checkedGet();
                        } catch (DTxException e) {
                            LOG.info("Write failed for {}", e.toString());
                        }
                        count++;

                        if (count == errorOccur) {
                            writeFuture = writeData(dTx, operation, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                    LogicalDatastoreType.CONFIGURATION, errorInnerIid, nodeId, errorInnerlist);
                        }

                        try {
                            writeFuture.checkedGet();
                        } catch (DTxException e) {
                            LOG.info("Write failed for {}", e.toString());
                        }

                        if (count == putsPerTx) {
                            CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dTx.submit();
                            try {
                                submitFuture.checkedGet();
                                testSucceed = false;
                                break;
                            } catch (Exception e) {
                                LOG.info("Get submit exception {}", e.toString());
                            }
                            count = 0;
                            dTx = dTxProvider.newTx(nodesMap);
                        }
                    }
                }
            }else{
                LOG.info("DTx datastore rollback test begin");
                for (OuterList outerList : outerLists) {
                    for (InnerList innerList : outerList.getInnerList()) {
                        InstanceIdentifier<InnerList> InnerIid = getInstanceIdentifier(outerList, innerList);
                        CheckedFuture<Void, DTxException> writeFuture = writeData(dTx, operation, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                LogicalDatastoreType.CONFIGURATION, InnerIid, nodeId, innerList);

                        try {
                            writeFuture.checkedGet();
                        } catch (DTxException e) {
                            LOG.info("Write failed for {}", e.toString());
                        }
                        count++;

                        if (count == putsPerTx) {
                            CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = dTx.rollback();
                            try {
                                rollbackFuture.checkedGet();
                            } catch (Exception e) {
                                LOG.info("Get rollback exception {}", e.toString());
                                testSucceed = false;
                            }
                            count = 0;
                            dTx = dTxProvider.newTx(nodesMap);
                        }
                    }
                }
            }
            CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dTx.submit();
            try{
                restSubmitFuture.checkedGet();
            }catch (TransactionCommitFailedException e){
                LOG.info("DTx outstanding submit failed for {}", e.toString());
            }

            for (OuterList outerList : outerLists) {
                for (InnerList innerList : outerList.getInnerList()) {
                    InstanceIdentifier<InnerList> innerIid = getInstanceIdentifier(outerList, innerList);
                    ReadOnlyTransaction tx = dataBroker.newReadOnlyTransaction();
                    CheckedFuture<Optional<InnerList>, ReadFailedException> readFuture = tx.read(LogicalDatastoreType.CONFIGURATION,
                            innerIid);
                    Optional<InnerList> result = Optional.absent();
                    try {
                        result = readFuture.checkedGet();
                    } catch (ReadFailedException readException) {
                        testSucceed= false;
                    }
                    if (operation != OperationType.DELETE) {
                        if (result.isPresent()) {
                            testSucceed = false;
                        }
                    } else {
                        if (!result.isPresent()) {
                            testSucceed = false;
                        }
                    }
                }
            }

            dsTestStatus.set(TestStatus.ExecStatus.Idle);
        }else {
            LOG.info("DTx datastore normal test begin");
            for (OuterList outerList : outerLists) {
                for (InnerList innerList : outerList.getInnerList()) {
                    InstanceIdentifier<InnerList> InnerIid = getInstanceIdentifier(outerList, innerList);
                    CheckedFuture<Void, DTxException> writeFuture = writeData(dTx, operation, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, InnerIid, nodeId, innerList);
                    count++;

                    try {
                        writeFuture.checkedGet();
                    } catch (DTxException e) {
                        LOG.info("Write failed for {}", e.toString());
                    }

                    if (count == putsPerTx) {
                        CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dTx.submit();
                        try {
                            submitFuture.checkedGet();
                        } catch (TransactionCommitFailedException e) {
                            LOG.info("DTx transaction submit fail for {}", e.toString());
                        }
                        dTx = dTxProvider.newTx(nodesMap);
                        count = 0;
                    }
                }
            }

            CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dTx.submit();
            try {
                restSubmitFuture.checkedGet();
            } catch (TransactionCommitFailedException e) {
                LOG.info("DTx outstanding submit fail for {}", e.toString());
            }
            for (OuterList outerList : outerLists) {
                for (InnerList innerList : outerList.getInnerList()) {
                    InstanceIdentifier<InnerList> innerIid = getInstanceIdentifier(outerList, innerList);
                    ReadTransaction transaction = dataBroker.newReadOnlyTransaction();

                    CheckedFuture<Optional<InnerList>, ReadFailedException> readFuture = transaction
                            .read(LogicalDatastoreType.CONFIGURATION, innerIid);
                    Optional<InnerList> result = Optional.absent();
                    try {
                        result = readFuture.checkedGet();
                    } catch (ReadFailedException e) {
                        LOG.info("Can't read the data from the data store");
                        testSucceed = false;
                    }
                    if (operation != OperationType.DELETE) {
                        if (!result.isPresent()) {
                            LOG.info("DTx write data failed");
                            dsTestStatus.set(TestStatus.ExecStatus.Idle);
                            return RpcResultBuilder.success(new DatastoreTestOutputBuilder()
                                    .setStatus(StatusType.FAILED)
                                    .build()).buildFuture();
                        }
                    } else {
                        if ( result.isPresent()) {
                            LOG.info("DTx delete data failed");
                            testSucceed = false;
                        }
                    }
                }
            }
        }
        LOG.info("DTx datastore test finish");
        if (testSucceed == true){
            dsTestStatus.set(TestStatus.ExecStatus.Idle);
            return RpcResultBuilder.success(new DatastoreTestOutputBuilder()
                    .setStatus(StatusType.OK)
                    .build()).buildFuture();
        }
        else {
            dsTestStatus.set(TestStatus.ExecStatus.Idle);
            return RpcResultBuilder.success(new DatastoreTestOutputBuilder()
                    .setStatus(StatusType.FAILED)
                    .build()).buildFuture();
        }
    }

    private InstanceIdentifier<InnerList> getInstanceIdentifier(OuterList outerList, InnerList innerList) {
        return InstanceIdentifier.create(DatastoreTestData.class)
                .child(OuterList.class, outerList.getKey())
                .child(InnerList.class, innerList.getKey());
    }

    private <T extends DataObject> CheckedFuture<Void, DTxException> writeData(DTx dTx, OperationType operation, DTXLogicalTXProviderType dtxTxType, LogicalDatastoreType dsType,
                                                                               InstanceIdentifier<T> Iid, InstanceIdentifier<?> nodeId, T data) {
        CheckedFuture<Void, DTxException> writeFuture = null;
        if (operation == OperationType.PUT) {
            writeFuture = dTx.putAndRollbackOnFailure(dtxTxType, dsType, Iid, data, nodeId);
        } else if (operation == OperationType.MERGE) {
            writeFuture = dTx.mergeAndRollbackOnFailure(dtxTxType, dsType, Iid, data, nodeId);
        } else {
            writeFuture = dTx.deleteAndRollbackOnFailure(dtxTxType, dsType, Iid, nodeId);
        }
        return writeFuture;
    }

    @Override
    public Future<RpcResult<MixedProviderTestOutput>> mixedProviderTest(MixedProviderTestInput input) {
        LOG.info("DTx mixed providers test begin");
        if (mixedProviderTestStatus.compareAndSet(TestStatus.ExecStatus.Idle, TestStatus.ExecStatus.Executing) == false) {
            return RpcResultBuilder.success(new MixedProviderTestOutputBuilder()
                    .setStatus(StatusType.TESTINPROGRESS)
                    .build()).buildFuture();
        }

        int putsPerTx = input.getPutsPerTx();
        int innerElements = input.getNumberOfTxs(), outerElements = 1;
        OperationType operation = input.getOperation();
        Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodesMap = new HashMap<>();
        DataStoreListBuilder dsListBuilder = new DataStoreListBuilder(dataBroker, outerElements, innerElements);
        List<OuterList> outerLists = dsListBuilder.buildOuterList();
        List<NodeId> nodeIdList = new ArrayList(nodeIdSet);
        Set<InstanceIdentifier<?>> netconfNodeIdSet = new HashSet<>();
        NodeId nodeId = nodeIdList.get(0);
        InstanceIdentifier netconfNodeId = NETCONF_TOPO_IID.child(Node.class, new NodeKey(nodeId));
        InstanceIdentifier<InterfaceConfigurations> netconfIid = InstanceIdentifier.create(InterfaceConfigurations.class);
        netconfNodeIdSet.add(netconfNodeId);
        DataBroker xrNodeBroker = xrNodeBrokerMap.get("sdn-1");

        Set<InstanceIdentifier<?>> dataStoreNodeIdSet = new HashSet<>();
        InstanceIdentifier<DatastoreTestData> dsNodeId = InstanceIdentifier.create(DatastoreTestData.class);
        dataStoreNodeIdSet.add(dsNodeId);

        nodesMap.put(DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER, dataStoreNodeIdSet);
        nodesMap.put(DTXLogicalTXProviderType.NETCONF_TX_PROVIDER, netconfNodeIdSet);

        if (!initializeDataStoreTestData(outerElements)) {
            LOG.info("Can't initialize datastore test data");
            mixedProviderTestStatus.set(TestStatus.ExecStatus.Idle);
            return RpcResultBuilder.success(new MixedProviderTestOutputBuilder()
                    .setStatus(StatusType.FAILED)
                    .build()).buildFuture();
        }

        deleteInterfaces(xrNodeBroker, input.getNumberOfTxs());

        if (input.getOperation() == OperationType.DELETE) {
            if (!buildTestInterfaces(xrNodeBroker, input.getNumberOfTxs())) {
                LOG.info("Can't build test data for DTx MixedProvider NetConf delete test");
                mixedProviderTestStatus.set(TestStatus.ExecStatus.Idle);
                return RpcResultBuilder
                        .success(new MixedProviderTestOutputBuilder()
                                .setStatus(StatusType.FAILED)
                                .build()).buildFuture();
            }
            if (!dsListBuilder.buildTestInnerList()) {
                LOG.info("Can't initialize test data for Dtx MixedProvider DataStore delete test");
                mixedProviderTestStatus.set(TestStatus.ExecStatus.Idle);
                return RpcResultBuilder
                        .success(new MixedProviderTestOutputBuilder()
                                .setStatus(StatusType.FAILED)
                                .build()).buildFuture();
            }
        }

        DTx dtx = dTxProvider.newTx(nodesMap);
        OuterList outerList = outerLists.get(0);
        boolean testSucceed = true;
        int counter = 0;
        if (input.getType() != TestType.NORMAL) {
            if (input.getType() == TestType.ROLLBACKONFAILURE) {
                LOG.info("DTx Mixed Provider RollbackOnFailure test begin");
                int errorOccur = (int) (putsPerTx * Math.random()) + 1;

                InstanceIdentifier<InnerList> errorInnerIid = InstanceIdentifier.create(DatastoreTestData.class)
                        .child(OuterList.class, new OuterListKey(outerElements))
                        .child(InnerList.class, new InnerListKey(0));

                InnerList errorInnerlist = new InnerListBuilder()
                        .setKey(new InnerListKey(0))
                        .setValue("Error InnerList")
                        .setName(0)
                        .build();

                for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                    InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                    KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                            = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                    InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
                    interfaceConfigurationBuilder.setInterfaceName(subIfname);
                    interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
                    InterfaceConfiguration config = interfaceConfigurationBuilder.build();

                    InnerList innerList = outerList.getInnerList().get(i - 1);
                    InstanceIdentifier<InnerList> innerIid = getInstanceIdentifier(outerList, innerList);

                    CheckedFuture<Void, DTxException> netconfWriteFuture = writeData(dtx, operation, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, netconfNodeId, config);
                    CheckedFuture<Void, DTxException> dataStoreWriteFuture = writeData(dtx, operation, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, innerIid, dsNodeId, innerList);

                    try {
                        netconfWriteFuture.checkedGet();
                    } catch (Exception e) {
                        LOG.info("Netconf write failed for {}", e.toString());
                    }

                    try {
                        dataStoreWriteFuture.checkedGet();
                    }catch (Exception e){
                        LOG.info("Datastore write failed for {}", e.toString());
                    }

                    counter++;
                    if (counter == errorOccur) {
                        dataStoreWriteFuture = writeData(dtx, operation, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                                LogicalDatastoreType.CONFIGURATION, errorInnerIid, dsNodeId, errorInnerlist);
                        try {
                            dataStoreWriteFuture.checkedGet();
                        } catch (Exception e) {
                            LOG.info("Datastore write failed for {}", e.toString());
                        }
                    }

                    if (counter == putsPerTx) {
                        CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dtx.submit();
                        try {
                            submitFuture.checkedGet();
                        } catch (TransactionCommitFailedException e) {
                            LOG.info("Get submit exception {}", e.toString());
                        }
                        counter = 0;
                        dtx = dTxProvider.newTx(nodesMap);
                    }
                }
            } else {
                LOG.info("DTx Mixed Provider rollback test begin");
                for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                    InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                    KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                            = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                    InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
                    interfaceConfigurationBuilder.setInterfaceName(subIfname);
                    interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
                    InterfaceConfiguration config = interfaceConfigurationBuilder.build();

                    InnerList innerList = outerList.getInnerList().get(i - 1);
                    InstanceIdentifier<InnerList> innerIid = getInstanceIdentifier(outerList, innerList);

                    CheckedFuture<Void, DTxException> netconfWriteFuture = writeData(dtx, operation, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, netconfNodeId, config);
                    CheckedFuture<Void, DTxException> dataStoreWriteFuture = writeData(dtx, operation, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, innerIid, dsNodeId, innerList);

                    try {
                        netconfWriteFuture.checkedGet();
                    } catch (Exception e) {
                        LOG.info("Netconf write failed for {}", e.toString());
                    }

                    try {
                        dataStoreWriteFuture.checkedGet();
                    }catch (Exception e){
                        LOG.info("Datastore write failed for {}", e.toString());
                    }

                    counter++;

                    if (counter == putsPerTx) {
                        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = dtx.rollback();
                        try {
                            rollbackFuture.checkedGet();
                        } catch (DTxException.RollbackFailedException e) {
                            LOG.info("Mixed provider rollback test failed for {}", e.toString());
                            testSucceed = false;
                        }
                        counter = 0;
                        dtx = dTxProvider.newTx(nodesMap);
                    }
                }
            }

            CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
            try {
                restSubmitFuture.checkedGet();
            } catch (TransactionCommitFailedException e) {
                LOG.info("Get outstanding submit exception {}", e.toString());
            }

            for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                InstanceIdentifier<InnerList> innerIid = getInstanceIdentifier(outerList, outerList.getInnerList().get(i - 1));

                ReadOnlyTransaction netconfTransaction = xrNodeBroker.newReadOnlyTransaction(),
                        datastoreTransaction = dataBroker.newReadOnlyTransaction();

                CheckedFuture<Optional<InterfaceConfiguration>, ReadFailedException> netconfReadFuture = netconfTransaction.read(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid);
                CheckedFuture<Optional<InnerList>, ReadFailedException> datastoreReadFuture = datastoreTransaction.read(LogicalDatastoreType.CONFIGURATION, innerIid);
                Optional<InterfaceConfiguration> netconfResult = Optional.absent();
                Optional<InnerList> datastoreResult = Optional.absent();

                try {
                    netconfResult = netconfReadFuture.checkedGet();
                } catch (ReadFailedException e) {
                    LOG.info("Can't read the data from netconf device");
                    testSucceed = false;
                }

                try{
                    datastoreResult = datastoreReadFuture.checkedGet();
                }catch (ReadFailedException e){
                    LOG.info("Can't read the data from datastore node");
                    testSucceed = false;
                }

                if (input.getOperation() != OperationType.DELETE) {
                    if (netconfResult.isPresent() || datastoreResult.isPresent()) {

                        testSucceed = false;
                    }
                } else {
                    if (!netconfResult.isPresent() || !datastoreResult.isPresent()) {
                        testSucceed = false;
                    }
                }
            }
        }else{
            LOG.info("DTx Mixed Provider normal test begin");
            for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
                interfaceConfigurationBuilder.setInterfaceName(subIfname);
                interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
                InterfaceConfiguration config = interfaceConfigurationBuilder.build();

                InnerList innerList = outerList.getInnerList().get(i - 1);
                InstanceIdentifier<InnerList> innerIid = getInstanceIdentifier(outerList, innerList);
                CheckedFuture<Void, DTxException> netconfWriteFuture = writeData(dtx, operation, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, netconfNodeId, config);
                CheckedFuture<Void, DTxException> datastoreWriteFuture = writeData(dtx, operation, DTXLogicalTXProviderType.DATASTORE_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, innerIid, dsNodeId, innerList);
                counter++;

                try {
                    netconfWriteFuture.checkedGet();
                } catch (Exception e) {
                    LOG.info("Netconf write failed for {}", e.toString());
                }

                try {
                    datastoreWriteFuture.checkedGet();
                }catch (Exception e){
                    LOG.info("Datastore write failed for {}", e.toString());
                }

                if (counter == putsPerTx) {
                    CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dtx.submit();
                    try {
                        submitFuture.checkedGet();
                    } catch (TransactionCommitFailedException e) {
                        LOG.info("Get submit exception {}", e.toString());
                    }
                    counter = 0;
                    dtx = dTxProvider.newTx(nodesMap);
                }
            }

            CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
            try {
                restSubmitFuture.checkedGet();
            } catch (TransactionCommitFailedException e) {
                LOG.info("Get outstanding submit exception {}", e.toString());
            }

            for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                InstanceIdentifier<InnerList> innerIid = getInstanceIdentifier(outerList, outerList.getInnerList().get(i - 1));
                ReadOnlyTransaction netconfTx = xrNodeBroker.newReadOnlyTransaction(),
                        dataStoreTx = dataBroker.newReadOnlyTransaction();

                CheckedFuture<Optional<InterfaceConfiguration>, ReadFailedException> netconfReadFuture = netconfTx.read(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid);
                CheckedFuture<Optional<InnerList>, ReadFailedException> datastoreReadFuture = dataStoreTx.read(LogicalDatastoreType.CONFIGURATION, innerIid);
                Optional<InterfaceConfiguration> netconfResult = Optional.absent();
                Optional<InnerList> datastoreResult = Optional.absent();

                try {
                    netconfResult = netconfReadFuture.checkedGet();
                } catch (ReadFailedException e) {
                    LOG.info("Can't read the data from netconf device");
                    testSucceed = false;
                }

                try{
                    datastoreResult = datastoreReadFuture.checkedGet();
                }catch (ReadFailedException e){
                    LOG.info("Can't read the data from datastore node");
                    testSucceed = false;
                }

                if (operation != OperationType.DELETE) {
                    if (!netconfResult.isPresent() && !datastoreResult.isPresent()) {
                        testSucceed = false;
                    }
                } else {
                    if (netconfResult.isPresent() && datastoreResult.isPresent()) {
                        testSucceed = false;
                    }
                }
            }
        }

        mixedProviderTestStatus.set(TestStatus.ExecStatus.Idle);
        if (testSucceed) {
            return RpcResultBuilder.success(new MixedProviderTestOutputBuilder()
                    .setStatus(StatusType.OK)
                    .build()).buildFuture();
        } else {
            return RpcResultBuilder.success(new MixedProviderTestOutputBuilder()
                    .setStatus(StatusType.FAILED)
                    .build()).buildFuture();
        }
    }

    /**
     * this method is used to initialize netconf configurations
     * it will check whether the subInterfaces exit and delete the subInterfaces
     * @param numberOfTxs number of test subInterfaces
     */
    public void deleteInterfaces( DataBroker xrNodeBroker, long numberOfTxs){
        InstanceIdentifier<InterfaceConfigurations> netconfIid = InstanceIdentifier.create(InterfaceConfigurations.class);

        for (int i = 1; i <= numberOfTxs; i++) {
            ReadWriteTransaction writeTx = xrNodeBroker.newReadWriteTransaction();
            InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
            KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                    = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
            CheckedFuture<Optional<InterfaceConfiguration>, ReadFailedException> readFuture = writeTx.read(
                    LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid);
            Optional<InterfaceConfiguration> readResult = Optional.absent();
            try{
                readResult = readFuture.checkedGet();
            }catch (ReadFailedException e){
                LOG.info("Can't read the original config of the {} subInterface", subIfname.toString());
            }

            if (readResult.isPresent()) {
                writeTx.delete(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid);
            }

            CheckedFuture<Void, TransactionCommitFailedException> submitFuture = writeTx.submit();
            try {
                submitFuture.checkedGet();
            } catch (TransactionCommitFailedException e) {
                LOG.info("Delete fail");
            }
        }
    }

    public boolean buildTestInterfaces(DataBroker xrNodeBroker, long numberOfIfs)
    {
        WriteTransaction xrNodeWriteTx = null;
        InstanceIdentifier<InterfaceConfigurations> netconfIid = InstanceIdentifier.create(InterfaceConfigurations.class);
        for (int i = 1; i <= numberOfIfs; i++) {
            xrNodeWriteTx = xrNodeBroker.newWriteOnlyTransaction();
            InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
            final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                    = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
            final InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
            interfaceConfigurationBuilder.setInterfaceName(subIfname);
            interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
            InterfaceConfiguration config = interfaceConfigurationBuilder.build();

            xrNodeWriteTx.put(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, config);
            CheckedFuture<Void, TransactionCommitFailedException> submitFut = xrNodeWriteTx.submit();
            try {
                submitFut.checkedGet();
            } catch (TransactionCommitFailedException e) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Future<RpcResult<NetconfTestOutput>> netconfTest(NetconfTestInput input) {
        LOG.info("DTx netconf test begin");
        if (netConfTestStatus.compareAndSet(TestStatus.ExecStatus.Idle, TestStatus.ExecStatus.Executing) == false) {
            return RpcResultBuilder.success(new NetconfTestOutputBuilder()
                    .setStatus(StatusType.TESTINPROGRESS)
                    .build()).buildFuture();
        }
        int putsPerTx=input.getPutsPerTx();
        OperationType operation=input.getOperation();

        List<NodeId> nodeIdList = new ArrayList(nodeIdSet);
        Set<InstanceIdentifier<?>> txIidSet = new HashSet<>();
        NodeId n1 =nodeIdList.get(0);
        InstanceIdentifier nodeId1 = NETCONF_TOPO_IID.child(Node.class, new NodeKey(n1));
        txIidSet.add(nodeId1);

        NodeId n2 =nodeIdList.get(1);
        InstanceIdentifier nodeId2 = NETCONF_TOPO_IID.child(Node.class, new NodeKey(n2));
        txIidSet.add(nodeId2);

        InstanceIdentifier<InterfaceConfigurations> netconfIid = InstanceIdentifier.create(InterfaceConfigurations.class);

        DataBroker xrNodeBroker1 = xrNodeBrokerMap.get("sdn-1");
        DataBroker xrNodeBroker2 = xrNodeBrokerMap.get("sdn-2");
        deleteInterfaces(xrNodeBroker1, input.getNumberOfTxs());
        deleteInterfaces(xrNodeBroker2, input.getNumberOfTxs());
        if(input.getOperation()==OperationType.DELETE){
            if (!buildTestInterfaces(xrNodeBroker1, input.getNumberOfTxs())){
                LOG.info("Can't build test subInterfaces for DTx netconf node1");
                netConfTestStatus.set(TestStatus.ExecStatus.Idle);
                return RpcResultBuilder
                        .success(new NetconfTestOutputBuilder()
                                .setStatus(StatusType.FAILED)
                                .build()).buildFuture();
            }

        if (!buildTestInterfaces(xrNodeBroker2, input.getNumberOfTxs())){
                LOG.info("Can't build test subInterfaces for DTx netconf node2");
                netConfTestStatus.set(TestStatus.ExecStatus.Idle);
                return RpcResultBuilder
                        .success(new NetconfTestOutputBuilder()
                                .setStatus(StatusType.FAILED)
                                .build()).buildFuture();
            }
        }

        int counter=0;
        boolean testSucceed = true;
        if(input.getType() != TestType.NORMAL){
            if (input.getType() == TestType.ROLLBACKONFAILURE) {
                LOG.info("DTx netconf rollback on failure test begin");
                deleteInterfaces(xrNodeBroker2, 1);
                DTx dtx = dTxProvider.newTx(txIidSet);
                InterfaceName errorIfName = new InterfaceName("GigabitEthernet0/0/0/1.1");
                KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> errorSpecificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), errorIfName));
                InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
                interfaceConfigurationBuilder.setInterfaceName(errorIfName);
                interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
                InterfaceConfiguration errorConfig = interfaceConfigurationBuilder.build();

                for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                    InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                    final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                            = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                    final InterfaceConfigurationBuilder interfaceConfigurationBuilder2 = new InterfaceConfigurationBuilder();
                    interfaceConfigurationBuilder2.setInterfaceName(subIfname);
                    interfaceConfigurationBuilder2.setActive(new InterfaceActive("act"));
                    InterfaceConfiguration config = interfaceConfigurationBuilder2.build();

                    CheckedFuture<Void, DTxException> writeFuture = writeData(dtx, operation, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, nodeId1, config);
                    counter++;

                    try {
                        writeFuture.checkedGet();
                    } catch (Exception e) {
                        LOG.info("put failed for {}", e.toString());
                    }

                    if (counter == putsPerTx) {
                        writeFuture = writeData(dtx, OperationType.DELETE, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                                LogicalDatastoreType.CONFIGURATION, errorSpecificInterfaceCfgIid, nodeId2, errorConfig);

                        try {
                            writeFuture.checkedGet();
                            testSucceed = false;
                        } catch (Exception e) {
                            LOG.info("Write failed for {}", e.toString());
                        }
                        counter = 0;
                        dtx = dTxProvider.newTx(txIidSet);
                    }
                }
                CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
                try{
                    restSubmitFuture.checkedGet();
                }catch (Exception e){
                    LOG.info("Outstanding submit failed for {}", e.toString());
                    testSucceed = false;
                }
            }else{
                LOG.info("DTx netconf rollback test begin");
                DTx dtx = dTxProvider.newTx(txIidSet);
                for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                    InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                    KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                            = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                    InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
                    interfaceConfigurationBuilder.setInterfaceName(subIfname);
                    interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
                    InterfaceConfiguration config = interfaceConfigurationBuilder.build();

                    CheckedFuture<Void, DTxException> writeFuture = writeData(dtx, operation, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                            LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, nodeId1, config);
                    counter++;

                    try {
                        writeFuture.checkedGet();
                    } catch (Exception e) {
                        LOG.info("Write failed for {}", e.toString());
                    }

                    if (counter == putsPerTx) {
                        CheckedFuture<Void, DTxException.RollbackFailedException> rollbackFuture = dtx.rollback();
                        try {
                            rollbackFuture.checkedGet();
                        } catch (Exception e) {
                            LOG.info("DTx netconf rollback failed for {}", e.toString());
                            testSucceed = false;
                        }
                        counter = 0;
                        dtx = dTxProvider.newTx(txIidSet);
                    }
                }
                CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
                try{
                    restSubmitFuture.checkedGet();
                }catch (Exception e){
                    LOG.info("Outstanding submit failed for {}", e.toString());
                    testSucceed = false;
                }
            }
            for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                ReadOnlyTransaction netconfTx1 = xrNodeBroker1.newReadOnlyTransaction();

                CheckedFuture<Optional<InterfaceConfiguration>, ReadFailedException> netconfReadFuture1 = netconfTx1.read(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid);
                Optional<InterfaceConfiguration> netconfResult1 = Optional.absent();

                try {
                    netconfResult1 = netconfReadFuture1.checkedGet();
                } catch (ReadFailedException e) {
                    LOG.info("Can't read the data from the device");
                    break;
                }
                if (operation != OperationType.DELETE) {
                    if (netconfResult1.isPresent()) {
                        LOG.info("DTx Rollback failed");
                        testSucceed = false;
                    }
                } else {
                    if (!netconfResult1.isPresent()) {
                        LOG.info("DTx Rollback failed");
                        testSucceed = false;
                    }
                }
            }
        }else {
            LOG.info("DTx Netconf normal test begin");
            DTx dtx = dTxProvider.newTx(txIidSet);
            for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                InterfaceName subIfname = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                final KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfname));
                final InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder();
                interfaceConfigurationBuilder.setInterfaceName(subIfname);
                interfaceConfigurationBuilder.setActive(new InterfaceActive("act"));
                InterfaceConfiguration config = interfaceConfigurationBuilder.build();

                CheckedFuture<Void, DTxException> writeFuture1 = writeData(dtx, operation, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, nodeId1, config);
                CheckedFuture<Void, DTxException> writeFuture2 = writeData(dtx, operation, DTXLogicalTXProviderType.NETCONF_TX_PROVIDER,
                        LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid, nodeId2, config);
                counter++;

                try {
                    writeFuture1.checkedGet();
                } catch (Exception e) {
                    testSucceed = false;
                    LOG.info("Write to netConf node1 failed for {}", e.toString());
                }

                try {
                    writeFuture2.checkedGet();
                }catch (Exception e){
                    testSucceed = false;
                    LOG.info("Write to netConf node2 failed for {}", e.toString());
                }

                if (counter == putsPerTx) {
                    CheckedFuture<Void, TransactionCommitFailedException> submitFuture = dtx.submit();
                    try {
                        submitFuture.checkedGet();
                    } catch (Exception e) {
                        LOG.info("Submit failed for {}", e.toString());
                        testSucceed = false;
                    }
                    counter = 0;
                    dtx = dTxProvider.newTx(txIidSet);
                }
            }
            CheckedFuture<Void, TransactionCommitFailedException> restSubmitFuture = dtx.submit();
            try {
                restSubmitFuture.checkedGet();
            } catch (TransactionCommitFailedException e) {
                LOG.info("Get outstanding submit exception {}", e.toString());
            }

            for (int i = 1; i <= input.getNumberOfTxs(); i++) {
                InterfaceName subIfName = new InterfaceName("GigabitEthernet0/0/0/1." + i);
                KeyedInstanceIdentifier<InterfaceConfiguration, InterfaceConfigurationKey> specificInterfaceCfgIid
                        = netconfIid.child(InterfaceConfiguration.class, new InterfaceConfigurationKey(new InterfaceActive("act"), subIfName));
                ReadOnlyTransaction netconfTx1 = xrNodeBroker1.newReadOnlyTransaction();
                ReadOnlyTransaction netconfTx2 = xrNodeBroker2.newReadOnlyTransaction();

                CheckedFuture<Optional<InterfaceConfiguration>, ReadFailedException> netconfReadFuture1 = netconfTx1.read(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid);
                CheckedFuture<Optional<InterfaceConfiguration>, ReadFailedException> netconfReadFuture2 = netconfTx2.read(LogicalDatastoreType.CONFIGURATION, specificInterfaceCfgIid);
                Optional<InterfaceConfiguration> netonfResult1 = Optional.absent();
                Optional<InterfaceConfiguration> netconfResult2 = Optional.absent();

                try {
                    netonfResult1 = netconfReadFuture1.checkedGet();
                } catch (ReadFailedException e) {
                    LOG.info("Can't read data from netconf node1");
                    testSucceed = false;
                }

                try{
                    netconfResult2 = netconfReadFuture2.checkedGet();
                }catch (ReadFailedException e){
                    LOG.info("Can't read data from netconf node2");
                }
                if (operation != OperationType.DELETE) {
                    if (!netonfResult1.isPresent() && !netconfResult2.isPresent()) {
                        testSucceed = false;
                    }
                } else {
                    if (netonfResult1.isPresent() && netconfResult2.isPresent()) {
                        testSucceed = false;
                    }
                }
            }
        }
        netConfTestStatus.set(TestStatus.ExecStatus.Idle);
        if (testSucceed) {
            return RpcResultBuilder.success(new NetconfTestOutputBuilder()
                    .setStatus(StatusType.OK)
                    .build()).buildFuture();
        } else {
            return RpcResultBuilder.success(new NetconfTestOutputBuilder()
                    .setStatus(StatusType.FAILED)
                    .build()).buildFuture();
        }
    }
}
