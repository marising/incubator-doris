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

import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CacheAnalyzer.CacheModel;
import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CachePartition;
import org.apache.doris.qe.cache.CacheProxy;
import org.apache.doris.qe.cache.PartitionRange;
import org.apache.doris.qe.cache.MySqlRowBuffer;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.planner.Planner;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.proto.PUniqueId;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.system.SystemInfoService;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

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

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
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
public class PartitionCacheTest {
    private static final Logger LOG = LogManager.getLogger(PartitionCacheTest.class);
    private Catalog catalog;
    private Analyzer analyzer;
    private ConnectContext ctx;
    private Planner planner;
    private SystemInfoService systemInfo;
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

        catalog = AccessTestUtil.fetchAdminCatalog();
//        systemInfo = new SystemInfoService();
        systemInfo = AccessTestUtil.fetchSystemInfoService();

        /*
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfo).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalog()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);
        */

        /*
        TableRef tblRef = new TableRef(new TableName("testDb","testTbl"),"l");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        try{
            EasyMock.expect(analyzer.resolveTableRef(tblRef)).andReturn(tblRef).anyTimes();
        } catch (AnalysisException e) {
            LOG.warn("an_ex={}", e);
            Assert.fail(e.getMessage());
        } catch (UserException e) {            
            LOG.warn("ue_ex={}", e);
            Assert.fail(e.getMessage());
        } catch (Exception e) {
            LOG.warn("cm_ex={}", e);
            Assert.fail(e.getMessage());
        }
        */

        /*
        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("testDb").anyTimes();
        EasyMock.expect(analyzer.getQualifiedUser()).andReturn("testUser").anyTimes();
        EasyMock.expect(analyzer.getCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.replay(analyzer);
        */
    }
    
    @Test
    public void testCachePartition() throws Exception {
        CachePartition cp = CachePartition.getInstance();
        Backend bd1 = new Backend(1, "", 1000);
        bd1.updateOnce(0,0,0);
        Backend bd2 = new Backend(2, "", 2000);
        bd2.updateOnce(0,0,0);
        Backend bd3 = new Backend(3, "", 3000);
        bd3.updateOnce(0,0,0);
        cp.addBackend(bd1);
        cp.addBackend(bd2);
        cp.addBackend(bd3);
        PUniqueId key1 = new PUniqueId();
        key1.hi = 1L;
        key1.lo = 1L;
        Backend bk = cp.findBackend(key1);
        Assert.assertNotNull(bk);
        Assert.assertEquals(bk.getId(),3);
        
        key1.hi = 669560558156283345L;
        key1.lo = 1L; 
        bk = cp.findBackend(key1);
        Assert.assertNotNull(bk);
        Assert.assertEquals(bk.getId(),1);
    }

    /**
    * table userprofile(date(pk), userid, country), load data at 1:00 every day
    * @param scanNodes
    */
    void createUserProfileScanNodes(List<ScanNode> scanNodes){
        List<Column> columns = Lists.newArrayList();
        Column column1 = new Column("date", ScalarType.INT);
        Column column2 = new Column("userid", ScalarType.INT);
        Column column3 = new Column("country", ScalarType.INT);
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column1));

        Partition part1 = new Partition(2020114, "p20200114", null, null);
        part1.SetVisibleVersion(1,1,1578934800000L);     //2020-01-14 1:00:00
        Partition part2 = new Partition(2020115, "p20200115", null, null);
        part2.SetVisibleVersion(2,2,1579021200000L);     //2020-01-15 1:00:00

        OlapTable table = new OlapTable(1, "userprofile", columns,KeysType.DUP_KEYS, partInfo, null);      
        table.addPartition(part1);
        table.addPartition(part2);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(1)); 
        desc.setTable(table);
        ScanNode node = new OlapScanNode(new PlanNodeId(1), desc, "UserProfileNode");
        scanNodes.add(node);
    }

    /**
     * table MusicEvent(date(pk), userid, eventid, eventtime), stream load every 5 miniutes
     * @param scanNodes
     */
    void createMusicEventScanNodes(List<ScanNode> scanNodes) {
        List<Column> columns = Lists.newArrayList();
        Column column1 = new Column("date", ScalarType.INT);
        Column column2 = new Column("userid", ScalarType.INT);
        Column column3 = new Column("eventid", ScalarType.INT);
        Column column4 = new Column("eventtime", ScalarType.DATETIME);
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column1));

        Partition part1 = new Partition(2020114, "p20200114", null, null);
        part1.SetVisibleVersion(1,1,1578934800000L);     //2020-01-14 1:00:00
        Partition part2 = new Partition(2020115, "p20200115", null, null);
        part2.SetVisibleVersion(2,2,1579053661000L);     //2020-01-15 10:01:01

        OlapTable table = new OlapTable(1, "testTbl", columns,KeysType.DUP_KEYS, partInfo, null); 
        table.addPartition(part1);
        table.addPartition(part2);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(1)); 
        desc.setTable(table);
        ScanNode node = new OlapScanNode(new PlanNodeId(1), desc, "testTblNode");
        scanNodes.add(node);
    }

    @Test
    public void testCacheModelSimple() throws Exception {
        //2020-01-15 11:50:31
        //EasyMock.expect(System.currentTimeMillis()).andReturn(1579060231000).anyTimes();
        String stmt = new String("SELECT country, COUNT(userid) FROM testDb.userprofile GROUP BY country");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        StatementBase parseStmt = null;
        try {
            parseStmt = (StatementBase) parser.parse().value;
        } catch (Exception e) {
            LOG.warn("SQL={},msg={}", stmt, e.getMessage());
            Assert.fail(e.getMessage());
        }
        List<ScanNode> scanNodes = Lists.newArrayList();
        createUserProfileScanNodes(scanNodes);
        if(parseStmt == null){
            LOG.warn("parse stmt failed.");
        }
        LOG.warn("scan node size {}.", scanNodes.size());
        
        CacheAnalyzer ca = new CacheAnalyzer(parseStmt, scanNodes);
        CacheModel cm = ca.checkCacheModel(0);
        LOG.warn("SQL={}",stmt);
        LOG.warn("CheckModel={}",cm);
        Assert.assertEquals(cm, CacheModel.Table);
    }
    
    @Test
    public void testWithinMinTime() throws Exception {
        String stmt = new String("SELECT country, COUNT(userid) FROM testDb.testTbl GROUP BY country");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        StatementBase parseStmt = null;
        try {
            parseStmt = (StatementBase) parser.parse().value;
        } catch (Exception e) {
            LOG.warn("SQL={},msg={}", stmt, e.getMessage());
            Assert.fail(e.getMessage());
        }
        List<ScanNode> scanNodes = Lists.newArrayList();
        createUserProfileScanNodes(scanNodes);
        CacheAnalyzer ca = new CacheAnalyzer(parseStmt, scanNodes);
        CacheModel cm = ca.checkCacheModel(1579024800000L); //2020-1-15 10:01:01
        LOG.warn("SQL={}",stmt);
        LOG.warn("CheckModel={}",cm);
        Assert.assertEquals(cm, CacheModel.None);
    }
    
    @Test
    public void testPartitionCache() throws Exception {
        String stmt = new String("SELECT date, COUNT(userid) FROM testTbl GROUP BY date");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        StatementBase parseStmt = null;
        try {
            parseStmt = (StatementBase) parser.parse().value;
        } catch (Exception e) {
            LOG.warn("SQL={},ps_ex={}", stmt, e);
            Assert.fail(e.getMessage());
        }

        if (parseStmt==null) {
            LOG.warn("smtm is null");
        }

        try {            
            parseStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            LOG.warn("SQL={},an_ex={}", stmt, e);
            //Assert.fail(e.getMessage());
        } catch (UserException e) {            
            LOG.warn("SQL={},ue_ex={}", stmt, e);
            //Assert.fail(e.getMessage());
        } catch (Exception e) {
            LOG.warn("SQL={},cm_ex={}", stmt, e);
            //Assert.fail(e.getMessage());
        }

        List<ScanNode> scanNodes = Lists.newArrayList();
        createMusicEventScanNodes(scanNodes);
        CacheAnalyzer ca = new CacheAnalyzer(parseStmt, scanNodes);
        CacheModel cm = ca.checkCacheModel(1579053661000L); //2020-1-15 10:01:01
        LOG.warn("SQL={}",stmt);
        LOG.warn("CheckModel={}",cm);
        Assert.assertEquals(cm, CacheModel.Partition);
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

