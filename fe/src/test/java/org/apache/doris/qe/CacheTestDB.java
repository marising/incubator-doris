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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.easymock.EasyMock;

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
    public Database db
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
        return auth;
    }

    public void initDb() {
        db = new Database(1L, fullDbName);
    }

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
    }

    public OlapTable createProfileTable() {
        Column column1 = new Column("date", ScalarType.INT);
        Column column2 = new Column("userid", ScalarType.INT);
        Column column3 = new Column("country", ScalarType.INT);
        List<Column> columns = Lists.newArrayList(column1, column2, column3);
        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column1));
        Partition part1 = new Partition(2020114, "p20200114", null, null);
        part1.SetVisibleVersion(1,1,1578934800000L);     //2020-01-14 1:00:00
        Partition part2 = new Partition(2020115, "p20200115", null, null);
        part2.SetVisibleVersion(2,2,1579021200000L);     //2020-01-15 1:00:00

        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        OlapTable table = new OlapTable(20000L, "userprofile", columns,KeysType.DUP_KEYS, partInfo, distributionInfo);

        table.addPartition(part1);
        table.addPartition(part2);

        MaterializedIndex baseIndex = new MaterializedIndex(20001, IndexState.NORMAL);
        table.setIndexSchemaInfo(baseIndex.getId(), "userprofile", baseSchema, 0, 1, (short) 1);
        table.setBaseIndexId(baseIndex.getId());
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

        Partition part1 = new Partition(2020114, "p20200114", null, null);
        part1.SetVisibleVersion(1,1,1578934800000L);     //2020-01-14 1:00:00
        Partition part2 = new Partition(2020115, "p20200115", null, null);
        part2.SetVisibleVersion(2,2,1579053661000L);     //2020-01-15 10:01:01

        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);

        OlapTable table = new OlapTable(30000L, "appevent", columns,KeysType.DUP_KEYS, partInfo, distributionInfo);
        table.addPartition(part1);
        table.addPartition(part2);

        MaterializedIndex baseIndex = new MaterializedIndex(30001, IndexState.NORMAL);
        table.setIndexSchemaInfo(baseIndex.getId(), "appevent", baseSchema, 0, 1, (short) 1);
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
            OlapTable tbl1 = createTestTable();
            OlapTable tbl2 = createProfileTable();
            OlapTable3 tbl3 = createEventTable();
            db.createTable(tbl1);
            db.createTable(tbl2);
            db.createTable(tbl3);
            EasyMock.expect(catalog.getDb(fullDbName)).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb("testCluster:emptyDb")).andReturn(null).anyTimes();
            EasyMock.expect(catalog.getDb(db.getId())).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb(EasyMock.isA(String.class))).andReturn(new Database()).anyTimes();
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

    public Catalog createTestCatalog() {
        try {
            Catalog catalog = EasyMock.createMock(Catalog.class);
            EasyMock.expect(catalog.getAuth()).andReturn(fetchAdminAccess()).anyTimes();
            Database db = new Database(50000L, "testCluster:testDb");
            MaterializedIndex baseIndex = new MaterializedIndex(30001, IndexState.NORMAL);

            RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);

            Partition partition = new Partition(20000L, "testTbl", baseIndex, distributionInfo);
            List<Column> baseSchema = new LinkedList<Column>();
            OlapTable table = new OlapTable(30000, "testTbl", baseSchema,
                    KeysType.AGG_KEYS, new SinglePartitionInfo(), distributionInfo);
            table.setIndexSchemaInfo(baseIndex.getId(), "testTbl", baseSchema, 0, 1, (short) 1);
            table.addPartition(partition);
            table.setBaseIndexId(baseIndex.getId());
            db.createTable(table);

            EasyMock.expect(catalog.getDb("testCluster:testDb")).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb("testCluster:emptyDb")).andReturn(null).anyTimes();
            EasyMock.expect(catalog.getDb(db.getId())).andReturn(db).anyTimes();
            EasyMock.expect(catalog.getDb(EasyMock.isA(String.class))).andReturn(new Database()).anyTimes();
            EasyMock.expect(catalog.getDbNames()).andReturn(Lists.newArrayList("testCluster:testDb")).anyTimes();
            EasyMock.expect(catalog.getLoadInstance()).andReturn(new Load()).anyTimes();
            EasyMock.expect(catalog.getSchemaChangeHandler()).andReturn(new SchemaChangeHandler()).anyTimes();
            EasyMock.expect(catalog.getRollupHandler()).andReturn(new RollupHandler()).anyTimes();
            EasyMock.expect(catalog.getEditLog()).andReturn(EasyMock.createMock(EditLog.class)).anyTimes();
            EasyMock.expect(catalog.getClusterDbNames("testCluster")).andReturn(Lists.newArrayList("testCluster:testDb")).anyTimes();
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
        channel.sendOnePacket(EasyMock.isA(ByteBuffer.class));
        EasyMock.expectLastCall().anyTimes();
        channel.reset();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(channel);
        scheduler = EasyMock.createMock(ConnectScheduler.class);
        ctx = EasyMock.createMock(ConnectContext.class);
        EasyMock.expect(ctx.getMysqlChannel()).andReturn(channel).anyTimes();
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

    public Analyzer createAdminAnalyzer(boolean withCluster) {
        init();
        String prefix = "";
        if (withCluster) {
            prefix = clusterName+":";
        }
        //Analyzer analyzer = EasyMock.createMock(Analyzer.class);
        analyzer = new Analyzer(catalog, ctx);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn(prefix + dbName).anyTimes();
        EasyMock.expect(analyzer.getQualifiedUser()).andReturn(prefix + userName).anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn(clusterName).anyTimes();
        EasyMock.expect(analyzer.incrementCallDepth()).andReturn(1).anyTimes();
        EasyMock.expect(analyzer.decrementCallDepth()).andReturn(0).anyTimes();
        EasyMock.expect(analyzer.getCallDepth()).andReturn(1).anyTimes();
        EasyMock.replay(analyzer);
        return analyzer;
    }
}
