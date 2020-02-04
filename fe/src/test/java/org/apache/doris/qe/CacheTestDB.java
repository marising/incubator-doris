// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.qe;

import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.SetPassVar;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.alter.RollupHandler;
import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.easymock.EasyMock;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CacheTestDB {
    public static String clusterName = "testCluster";
    public static String dbName = "testDb";
    public static String fullDbName = "testCluster:testDb";
    public static String tableName = "testTbl";
    public static String userName = "testUser";
    public PaloAuth auth;
    public SystemInfoService clusterInfo;
    public Database db;
    public Catalog catalog;
    public ConnectContext ctx;
    public Analyzer analyzer;
    public Planner planner;
    public boolean isInit = false;

    public void init() {
        if (!isInit) {
            initSystemInfoService();
            initAdminAccess();
            initDb();
            initFullCatalog();
            initContext();
            isInit = true;
        }
    }

    public void initSystemInfoService() {
        clusterInfo = EasyMock.createMock(SystemInfoService.class);
        EasyMock.replay(clusterInfo);
    }

    public void initAdminAccess() {
        auth = EasyMock.createMock(PaloAuth.class);
        EasyMock.expect(auth.checkGlobalPriv(EasyMock.isA(ConnectContext.class),
                EasyMock.isA(PrivPredicate.class))).andReturn(true).anyTimes();
        EasyMock.expect(auth.checkDbPriv(EasyMock.isA(ConnectContext.class), EasyMock.anyString(),
                EasyMock.isA(PrivPredicate.class))).andReturn(true).anyTimes();
        EasyMock.expect(auth.checkTblPriv(EasyMock.isA(ConnectContext.class), EasyMock.anyString(),
                EasyMock.anyString(), EasyMock.isA(PrivPredicate.class)))
                .andReturn(true).anyTimes();
        try {
            auth.setPassword(EasyMock.isA(SetPassVar.class));
        } catch (DdlException e) {
            e.printStackTrace();
        }
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(auth);
    }

    public void initDb() {
        db = new Database(1L, fullDbName);
    }

    /*
    public OlapTable createTestTable() {
        OlapTable table = EasyMock.createMock(OlapTable.class);
        Partition partition = EasyMock.createMock(Partition.class);
        MaterializedIndex index = EasyMock.createMock(MaterializedIndex.class);
        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);
        EasyMock.expect(table.getBaseSchema()).andReturn(Lists.newArrayList(column1, column2)).anyTimes();
        EasyMock.expect(table.getPartition(40000L)).andReturn(partition).anyTimes();
        EasyMock.expect(partition.getBaseIndex()).andReturn(index).anyTimes();
        EasyMock.expect(partition.getIndex(30000L)).andReturn(index).anyTimes();
        EasyMock.expect(index.getId()).andReturn(30000L).anyTimes();
        EasyMock.replay(index);
        EasyMock.replay(partition);
        return table;
    }*/

    public OlapTable createProfileTable() {
        Column column1 = new Column("date", ScalarType.INT);
        Column column2 = new Column("userid", ScalarType.INT);
        Column column3 = new Column("country", ScalarType.INT);
        List<Column> columns = Lists.newArrayList(column1, column2, column3);

        MaterializedIndex baseIndex = new MaterializedIndex(20001, IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(10);

        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column1));

        Partition part12 = new Partition(2020112, "p20200112", baseIndex, distInfo);
        part12.SetVisibleVersion(1,1,1578762000000L);     //2020-01-12 1:00:00
        Partition part13 = new Partition(2020113, "p20200113", baseIndex, distInfo);
        part13.SetVisibleVersion(1,1,1578848400000L);     //2020-01-13 1:00:00
        Partition part14 = new Partition(2020114, "p20200114", baseIndex, distInfo);
        part14.SetVisibleVersion(1,1,1578934800000L);     //2020-01-14 1:00:00
        Partition part15 = new Partition(2020115, "p20200115", baseIndex, distInfo);
        part15.SetVisibleVersion(2,2,1579021200000L);     //2020-01-15 1:00:00

        OlapTable table = new OlapTable(20000L, "userprofile", columns,KeysType.DUP_KEYS, partInfo, distInfo);

        table.addPartition(part12);
        table.addPartition(part13);
        table.addPartition(part14);
        table.addPartition(part15);

        table.setIndexSchemaInfo(baseIndex.getId(), "userprofile", columns, 0, 1, (short) 1);
        table.setBaseIndexId(baseIndex.getId());

        //EasyMock.expect(table.getRowCount()).andReturn(0L).anyTimes();
        //EasyMock.replay(table);

        return table;
    }

    public ScanNode createProfileScanNode(){
        OlapTable table = createProfileTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(20004));
        desc.setTable(table);
        ScanNode node = new OlapScanNode(new PlanNodeId(20008), desc, "userprofilenode");
        return node;
    }

    /**
     * table appevent(date(pk), userid, eventid, eventtime), stream load every 5 miniutes
     * @param scanNodes
     */
    public OlapTable createEventTable() {
        Column column1 = new Column("date", ScalarType.INT);
        Column column2 = new Column("userid", ScalarType.INT);
        Column column3 = new Column("eventid", ScalarType.INT);
        Column column4 = new Column("eventtime", ScalarType.DATETIME);
        List<Column> columns = Lists.newArrayList(column1, column2, column3);
        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column1));
        MaterializedIndex baseIndex = new MaterializedIndex(30001, IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(10);

        Partition part12 = new Partition(2020112, "p20200112", baseIndex, distInfo);
        part12.SetVisibleVersion(1,1,1578762000000L);     //2020-01-12 1:00:00
        Partition part13 = new Partition(2020113, "p20200113", baseIndex, distInfo);
        part13.SetVisibleVersion(1,1,1578848400000L);     //2020-01-13 1:00:00
        Partition part14 = new Partition(2020114, "p20200114", baseIndex, distInfo);
        part14.SetVisibleVersion(1,1,1578934800000L);     //2020-01-14 1:00:00
        Partition part15 = new Partition(2020115, "p20200115", baseIndex, distInfo);
        part15.SetVisibleVersion(2,2,1579053661000L);     //2020-01-15 10:01:01

        OlapTable table = new OlapTable(30000L, "appevent", columns,KeysType.DUP_KEYS, partInfo, distInfo);
        table.addPartition(part12);
        table.addPartition(part13);
        table.addPartition(part14);
        table.addPartition(part15);

        table.setIndexSchemaInfo(baseIndex.getId(), "appevent", columns, 0, 1, (short) 1);
        table.setBaseIndexId(baseIndex.getId());

        return table;
    }

    public ScanNode createEventScanNode(){
        OlapTable table = createEventTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(30002));
        desc.setTable(table);
        ScanNode node = new OlapScanNode(new PlanNodeId(30004), desc, "appeventnode");
        return node;
    }

    public Catalog initFullCatalog() {
        try {
            catalog = EasyMock.createMock(Catalog.class);
            EasyMock.expect(catalog.getAuth()).andReturn(auth).anyTimes();
            //OlapTable tbl1 = createTestTable();
            OlapTable tbl2 = createProfileTable();
            OlapTable tbl3 = createEventTable();
            //db.createTable(tbl1);
            db.createTable(tbl2);
            db.createTable(tbl3);
            EasyMock.expect(catalog.getDb(fullDbName)).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb(dbName)).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb(db.getId())).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDbNames()).andReturn(Lists.newArrayList(fullDbName)).anyTimes();
            EasyMock.expect(catalog.getLoadInstance()).andReturn(new Load()).anyTimes();
            EasyMock.expect(catalog.getSchemaChangeHandler()).andReturn(new SchemaChangeHandler()).anyTimes();
            EasyMock.expect(catalog.getRollupHandler()).andReturn(new RollupHandler()).anyTimes();
            EasyMock.expect(catalog.getEditLog()).andReturn(EasyMock.createMock(EditLog.class)).anyTimes();
            EasyMock.expect(catalog.getClusterDbNames(clusterName)).andReturn(Lists.newArrayList(fullDbName)).anyTimes();
            catalog.changeDb(EasyMock.isA(ConnectContext.class), EasyMock.eq("blockDb"));
            EasyMock.expectLastCall().andThrow(new DdlException("failed.")).anyTimes();
            catalog.changeDb(EasyMock.isA(ConnectContext.class), EasyMock.isA(String.class));
            EasyMock.expectLastCall().anyTimes();
            EasyMock.expect(catalog.getBrokerMgr()).andReturn(new BrokerMgr()).anyTimes();
            EasyMock.replay(catalog);
            return catalog;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    public void initContext(){
        QueryState state = new QueryState();
        MysqlChannel channel = EasyMock.createMock(MysqlChannel.class);
        try{
            channel.sendOnePacket(EasyMock.isA(ByteBuffer.class));
        } catch (IOException e) {
            return; 
        }
        EasyMock.expectLastCall().anyTimes();
        channel.reset();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(channel);
        ConnectScheduler scheduler = EasyMock.createMock(ConnectScheduler.class);
        ctx = EasyMock.createMock(ConnectContext.class);
        EasyMock.expect(ctx.getMysqlChannel()).andReturn(channel).anyTimes();
        EasyMock.expect(ctx.getClusterName()).andReturn(clusterName).anyTimes();
        EasyMock.expect(ctx.getSerializer()).andReturn(MysqlSerializer.newInstance()).anyTimes();
        EasyMock.expect(ctx.getCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(ctx.getState()).andReturn(state).anyTimes();
        EasyMock.expect(ctx.getConnectScheduler()).andReturn(scheduler).anyTimes();
        EasyMock.expect(ctx.getConnectionId()).andReturn(1).anyTimes();
        EasyMock.expect(ctx.getQualifiedUser()).andReturn(userName).anyTimes();
        EasyMock.expect(ctx.getForwardedStmtId()).andReturn(123L).anyTimes();
        ctx.setKilled();
        EasyMock.expectLastCall().anyTimes();
        ctx.updateReturnRows(EasyMock.anyInt());
        EasyMock.expectLastCall().anyTimes();
        ctx.setQueryId(EasyMock.isA(TUniqueId.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ctx.queryId()).andReturn(new TUniqueId()).anyTimes();
        EasyMock.expect(ctx.getStartTime()).andReturn(0L).anyTimes();
        EasyMock.expect(ctx.getDatabase()).andReturn(dbName).anyTimes();
        SessionVariable sessionVariable = new SessionVariable();
        EasyMock.expect(ctx.getSessionVariable()).andReturn(sessionVariable).anyTimes();
        ctx.setStmtId(EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ctx.getStmtId()).andReturn(1L).anyTimes();
        EasyMock.replay(ctx);
    }

    public Analyzer createAnalyzer() {
        init();
        analyzer = new Analyzer(catalog, ctx);
        return analyzer;
    }
}
