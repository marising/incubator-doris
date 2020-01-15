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

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CachePartition;
import org.apache.doris.qe.cache.CacheProxy;
import org.apache.doris.qe.cache.PartitionRange;
import org.apache.doris.qe.cache.MySqlRowBuffer;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.planner.Planner;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

import java_cup.runtime.Symbol;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.log4j.*", "javax.management.*"})
@PrepareForTest({CacheAnalyzer.class, DdlExecutor.class, Catalog.class})
public class PartitionTest {
    private ConnectContext ctx;
    private Analyzer analyzer;
    private Planner planner;
    private QueryState state;
    private ConnectScheduler scheduler;

    @BeforeClass
    public static void start() {
        MetricRepo.init();
        try {
            FrontendOptions.init();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws IOException {
        state = new QueryState();
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
        EasyMock.expect(ctx.getCatalog()).andReturn(AccessTestUtil.fetchAdminCatalog()).anyTimes();
        EasyMock.expect(ctx.getState()).andReturn(state).anyTimes();
        EasyMock.expect(ctx.getConnectScheduler()).andReturn(scheduler).anyTimes();
        EasyMock.expect(ctx.getConnectionId()).andReturn(1).anyTimes();
        EasyMock.expect(ctx.getQualifiedUser()).andReturn("testUser").anyTimes();
        EasyMock.expect(ctx.getForwardedStmtId()).andReturn(123L).anyTimes();
        ctx.setKilled();
        EasyMock.expectLastCall().anyTimes();
        ctx.updateReturnRows(EasyMock.anyInt());
        EasyMock.expectLastCall().anyTimes();
        ctx.setQueryId(EasyMock.isA(TUniqueId.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ctx.queryId()).andReturn(new TUniqueId()).anyTimes();
        EasyMock.expect(ctx.getStartTime()).andReturn(0L).anyTimes();
        EasyMock.expect(ctx.getDatabase()).andReturn("testDb").anyTimes();
        SessionVariable sessionVariable = new SessionVariable();
        EasyMock.expect(ctx.getSessionVariable()).andReturn(sessionVariable).anyTimes();
        ctx.setStmtId(EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ctx.getStmtId()).andReturn(1L).anyTimes();
        EasyMock.replay(ctx);
    }
    @Test
    public void testCachePartition throws Exception {
        CachePartition cp = CachePartition.getInstance();
        Backend bd1 = new Backend(1, "", 1000);
        Backend bd2 = new Backend(2, "", 2000);
        Backend bd3 = new Backend(3, "", 3000);
        cp.addBackend(bd1);
        cp.addBackend(bd2);
        cp.addBackend(bd3);
        PUniqueId sqlKey;
        sql_key.hi = 1;
        sql_key.lo = 1;
        EasyMock.expect(cp.findBackend(sqlKey)).andReturn(Backend.class).anlytimes();
    }

    /**
    * table UserProfile(date(pk), userid, country), load data at 1:00 every day
    * @param scanNodes
    */
    void createUserProfileScanNodes(List<ScanNode> scanNodes){
        List<Column> columns = new List<>();
        Column column1 = new Column("date", ScalarType.INT, false, AggregateType.SUM, "", "");
        Column column2 = new Column("userid", ScalarType.INT true, AggregateType.REPLACE, "", "");
        Column column3 = new Column("country", ScalarType.INT, false, AggregateType.SUM, "", "");
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        PartitionInfo partInfo = new PartitionInfo(PartitionType.RANGE);
        OlapTable table = new OlapTable(1, columns,KeysType.DUP_KEYS, partInfo, new DistributionInfo());
        ScanNode node = new OlapScanNode(new PlanNodeId(1), new TupleDescriptor(new TupleId(10)), "null scanNode");
        //Partition part1 = new Partition(2020114,null,null);
        //part1.SetVisibleVersion(1,1,1578934800000);     //2020-01-14 1:00:00
        Partition part1 = new Partition(2020115,null,null);
        part2.SetVisibleVersion(2,2,1579021200000);     //2020-01-15 1:00:00
        node.addPartition(part1);
        scanNodes.add(node);
    }

    /**
     * table MusicEvent(date(pk), userid, eventname, eventtime), stream load every 5 miniutes
     * @param scanNodes
     */
    void createMusicEventScanNodes(List<ScanNode> scanNodes) {

    }

    @Test
    public void testCacheModelSimple throws Exception {
        //2020-01-15 11:50:31
        //EasyMock.expect(System.currentTimeMillis()).andReturn(1579060231000).anyTimes();
        String stmt = new String("SELECT country，COUNT( userid ) FROM userprofile GROUP BY country;");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        SelectStmt selectStmt = null;
        try {
            selectStmt = (SelectStmt) parser.parse().value;
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        List<ScanNode> scanNodes;
        createUserProfileScanNodes(scanNodes);
        CacheAnalytics ca = new CacheAnalytics(selectStmt, scanNodes);
        Assert.assertEquals(ca.checkCacheModel(), CacheModel.Table);
    }

    /*
    @Test
    public void testSelect() throws Exception {
        QueryStmt queryStmt = EasyMock.createMock(QueryStmt.class);
        queryStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(queryStmt.getColLabels()).andReturn(Lists.<String>newArrayList()).anyTimes();
        EasyMock.expect(queryStmt.getResultExprs()).andReturn(Lists.<Expr>newArrayList()).anyTimes();
        EasyMock.expect(queryStmt.isExplain()).andReturn(false).anyTimes();
        queryStmt.getDbs(EasyMock.isA(Analyzer.class), EasyMock.isA(SortedMap.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(queryStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        queryStmt.rewriteExprs(EasyMock.isA(ExprRewriter.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(queryStmt);

        Symbol symbol = new Symbol(0, queryStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // mock planner
        Planner planner = EasyMock.createMock(Planner.class);
        planner.plan(EasyMock.isA(QueryStmt.class), EasyMock.isA(Analyzer.class), EasyMock.isA(TQueryOptions.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(planner);

        PowerMock.expectNew(Planner.class).andReturn(planner).anyTimes();
        PowerMock.replay(Planner.class);

        // mock coordinator
        Coordinator cood = EasyMock.createMock(Coordinator.class);
        cood.exec();
        EasyMock.expectLastCall().anyTimes();
        cood.endProfile();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(cood.getQueryProfile()).andReturn(new RuntimeProfile()).anyTimes();
        EasyMock.expect(cood.getNext()).andReturn(new RowBatch()).anyTimes();
        EasyMock.expect(cood.getJobId()).andReturn(-1L).anyTimes();
        EasyMock.replay(cood);
        PowerMock.expectNew(Coordinator.class, EasyMock.isA(ConnectContext.class),
                EasyMock.isA(Analyzer.class), EasyMock.isA(Planner.class))
                .andReturn(cood).anyTimes();
        PowerMock.replay(Coordinator.class);

        Catalog catalog = Catalog.getInstance();
        Field field = catalog.getClass().getDeclaredField("canRead");
        field.setAccessible(true);
        field.set(catalog, new AtomicBoolean(true));

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.EOF, state.getStateType());
    }
     */

}

