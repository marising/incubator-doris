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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.CacheCoordinator;
import org.apache.doris.qe.cache.PartitionCache;
import org.apache.doris.qe.cache.PartitionRange;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CacheAnalyzer.CacheMode;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.qe.cache.CacheCoordinator;
import org.apache.doris.qe.cache.RowBatchBuilder;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.system.Backend;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.Planner;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.proto.PUniqueId;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.log4j.*", "javax.management.*"})
@PrepareForTest({CacheAnalyzer.class, CacheTestDB.class})
public class PartitionCacheTest {
    private static final Logger LOG = LogManager.getLogger(PartitionCacheTest.class);
    private Planner planner;
    private ConnectScheduler scheduler;
    private CacheTestDB testDB;
    private static ConnectContext context;

    List<PartitionRange.PartitionSingle> newRangeList;
    Cache.HitRange hitRange;
    @BeforeClass
    public static void start() {
        MetricRepo.init();
        try {
            FrontendOptions.init();
            context = new ConnectContext(null);
            Config.enable_sql_cache = true;
            Config.enable_partition_cache = true;
            context.getSessionVariable().setEnableSqlCache(true);
            context.getSessionVariable().setEnablePartitionCache(true);
            Config.last_version_interval_second = 7200;
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws IOException {
        testDB = new CacheTestDB();
        try{
            testDB.init();
            newRangeList = Lists.newArrayList();
        } catch (Exception e){
            LOG.warn("Init error={}",e);
        }
    }
    
    private StatementBase parseSql(String sql){
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        StatementBase parseStmt = null;
        try {
            parseStmt = (StatementBase) parser.parse().value;
            parseStmt.analyze(testDB.createAnalyzer());
        } catch (AnalysisException e) {
            LOG.warn("Part,an_ex={}", e);
            Assert.fail(e.getMessage());
        } catch (UserException e) {
            LOG.warn("Part,ue_ex={}", e);
            Assert.fail(e.getMessage());
        } catch (Exception e) {
            LOG.warn("Part,cm_ex={}", e);
            Assert.fail(e.getMessage());
        }
        return parseStmt;
    }
    
    @Test
    public void testCacheNode() throws Exception {
        CacheCoordinator cp = CacheCoordinator.getInstance();
        cp.DebugModel = true;
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

    @Test
    public void testCacheModeNone() throws Exception {
        StatementBase parseStmt = parseSql("select @@version_comment limit 1");
        List<ScanNode> scanNodes = Lists.newArrayList();
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(0);
        Assert.assertEquals(ca.getCacheMode(), CacheMode.NoNeed);
    }

    @Test
    public void testCacheModeTable() throws Exception {
        StatementBase parseStmt = parseSql(
            "SELECT country, COUNT(userid) FROM userprofile GROUP BY country"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createProfileScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(0);
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);
    }
    
    @Test
    public void testWithinMinTime() throws Exception {
        StatementBase parseStmt = parseSql(
            "SELECT country, COUNT(userid) FROM userprofile GROUP BY country"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createProfileScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579024800000L); //2020-1-15 02:00:00
        Assert.assertEquals(ca.getCacheMode(), CacheMode.None);
    }

    @Test
    public void testPartitionModel() throws Exception {
        StatementBase parseStmt = parseSql(
            "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-15\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);
    }

    @Test
    public void testParseByte() throws Exception {
        RowBatchBuilder sb = new RowBatchBuilder(CacheMode.Partition);
        byte[] buffer = new byte[]{10, 50, 48, 50, 48, 45, 48, 51, 45, 49, 48, 1, 51, 2, 67, 78};
        PartitionRange.PartitionKeyType key1 = sb.getKeyFromRow(buffer, 0, Type.DATE);
        LOG.info("real value key1 {}",key1.realValue());
        Assert.assertEquals(key1.realValue(), 20200310);
        PartitionRange.PartitionKeyType key2 = sb.getKeyFromRow(buffer, 1, Type.INT);
        LOG.info("real value key2 {}",key2.realValue());
        Assert.assertEquals(key2.realValue(), 3);
    }

    @Test
    public void testPartitionIntTypeSql() throws Exception {
        StatementBase parseStmt = parseSql(
                "SELECT `date`, COUNT(id) FROM `order` WHERE `date`>=20200112 and `date`<=20200115 GROUP BY date"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createOrderScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L);                         //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);    //assert cache model first
        try {
            PartitionCache cache = (PartitionCache) ca.getCache();
            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(), null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag, true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 4);

            String sql;
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            hitRange = range.diskPartitionRange(newRangeList);
            Assert.assertEquals(hitRange, Cache.HitRange.Left);
            Assert.assertEquals(newRangeList.size(), 2);
            Assert.assertEquals(newRangeList.get(0).getCacheKey().realValue(), 20200114);
            Assert.assertEquals(newRangeList.get(1).getCacheKey().realValue(), 20200115);

            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().getWhereClause().toSql();
            Assert.assertEquals(sql, "(`date` >= 20200114) AND (`date` <= 20200115)");
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSimpleCacheSql() throws Exception {
        StatementBase parseStmt = parseSql(
            "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-15\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first
        SelectStmt selectStmt = (SelectStmt) parseStmt;

        try{
            PartitionCache cache = (PartitionCache) ca.getCache();
            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(),null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag,true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 4);
            
            String sql;        
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            hitRange = range.diskPartitionRange(newRangeList);
            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().getWhereClause().toSql();
            Assert.assertEquals(sql,"(`eventdate` >= '2020-01-14') AND (`eventdate` <= '2020-01-15')");
        } catch(Exception e){
            LOG.warn("ex={}",e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testHitPartPartition() throws Exception {
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-14\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first

        try {
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(), null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag, true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 3);

            String sql;
            range.setCacheFlag(20200113);
            range.setCacheFlag(20200114);

            hitRange = range.diskPartitionRange(newRangeList);
            Assert.assertEquals(hitRange,Cache.HitRange.Right);
            Assert.assertEquals(newRangeList.size(), 2);
            Assert.assertEquals(newRangeList.get(0).getCacheKey().realValue(), 20200112);
            Assert.assertEquals(newRangeList.get(1).getCacheKey().realValue(), 20200112);

            List<PartitionRange.PartitionSingle> updateRangeList = range.updatePartitionRange();
            Assert.assertEquals(updateRangeList.size(), 1);
            Assert.assertEquals(updateRangeList.get(0).getCacheKey().realValue(), 20200112);
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNoUpdatePartition() throws Exception {
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-14\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first

        try {
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(), null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag, true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 3);

            String sql;
            range.setCacheFlag(20200112);    //get data from cache
            range.setCacheFlag(20200113);
            range.setCacheFlag(20200114);

            hitRange = range.diskPartitionRange(newRangeList);
            Assert.assertEquals(hitRange, Cache.HitRange.Full);
            Assert.assertEquals(newRangeList.size(), 0);
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testUpdatePartition() throws Exception {
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-15\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first

        try {
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(), null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag, true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 4);

            String sql;
            range.setCacheFlag(20200112L);    //get data from cache
            range.setTooNewByKey(20200115);

            range.diskPartitionRange(newRangeList);
            Assert.assertEquals(newRangeList.size(), 2);
            cache.rewriteSelectStmt(newRangeList);

            sql = ca.getRewriteStmt().getWhereClause().toSql();
            Assert.assertEquals(sql, "(`eventdate` >= '2020-01-13') AND (`eventdate` <= '2020-01-15')");

            List<PartitionRange.PartitionSingle> updateRangeList = range.updatePartitionRange();
            Assert.assertEquals(updateRangeList.size(), 2);
            Assert.assertEquals(updateRangeList.get(0).getCacheKey().realValue(), 20200113);
            Assert.assertEquals(updateRangeList.get(1).getCacheKey().realValue(), 20200114);
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }
   
    @Test
    public void testRewriteMultiPredicate1() throws Exception {
        StatementBase parseStmt = parseSql(
            "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>\"2020-01-11\" and eventdate<\"2020-01-16\"" +
                    " and eventid=1 GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first
        try{
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            LOG.warn("Nokey multi={}", cache.getNokeyStmt().getWhereClause().toSql());
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause().toSql(),"`eventid` = 1");

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag,true);

            int size = range.getPartitionSingleList().size();
            Assert.assertEquals(size, 4);
            
            String sql;        
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            range.diskPartitionRange(newRangeList);

            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().getWhereClause().toSql();
            LOG.warn("MultiPredicate={}", sql);                
            Assert.assertEquals(sql,"((`eventdate` > '2020-01-13') AND (`eventdate` < '2020-01-16')) AND (`eventid` = 1)");
        } catch(Exception e){
            LOG.warn("multi ex={}",e);
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testRewriteJoin() throws Exception {
        StatementBase parseStmt = parseSql(
            "SELECT appevent.eventdate, country, COUNT(appevent.userid) FROM appevent" +
                    " INNER JOIN userprofile ON appevent.userid = userprofile.userid" +
            " WHERE appevent.eventdate>=\"2020-01-12\" and appevent.eventdate<=\"2020-01-15\"" +
                    " and eventid=1 GROUP BY appevent.eventdate, country"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first
        try{
            PartitionCache cache = (PartitionCache) ca.getCache();
            cache.rewriteSelectStmt(null);
            LOG.warn("Join nokey={}", cache.getNokeyStmt().getWhereClause().toSql());
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause().toSql(),"`eventid` = 1");

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag,true);

            int size = range.getPartitionSingleList().size();
            Assert.assertEquals(size, 4);
            
            String sql;        
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            range.diskPartitionRange(newRangeList);

            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().getWhereClause().toSql();
            LOG.warn("Join rewrite={}", sql);                
            Assert.assertEquals(sql,"((`appevent`.`eventdate` >= '2020-01-14')" +
                    " AND (`appevent`.`eventdate` <= '2020-01-15')) AND (`eventid` = 1)");
        } catch(Exception e){
            LOG.warn("Join ex={}",e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSubSelect() throws Exception {
        StatementBase parseStmt = parseSql(
            "SELECT eventdate, sum(pv) FROM (SELECT eventdate, COUNT(userid) AS pv FROM appevent WHERE eventdate>\"2020-01-11\" AND eventdate<\"2020-01-16\"" +
                " AND eventid=1 GROUP BY eventdate) tbl GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L);                           //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition); //assert cache model first
        try{
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            LOG.warn("Sub nokey={}", cache.getNokeyStmt().toSql());
            Assert.assertEquals(cache.getNokeyStmt().toSql(),"SELECT <slot 7> `eventdate` AS `eventdate`, <slot 8> sum(`pv`) AS `sum(``pv``)` FROM (" +
                "SELECT <slot 3> `eventdate` AS `eventdate`, <slot 4> count(`userid`) AS `pv` FROM `testCluster:testDb`.`appevent` WHERE `eventid` = 1" +
                " GROUP BY `eventdate`) tbl GROUP BY `eventdate`");

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag,true);

            int size = range.getPartitionSingleList().size();
            Assert.assertEquals(size, 4);
            
            String sql;        
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            range.diskPartitionRange(newRangeList);

            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().toSql();
            LOG.warn("Sub rewrite={}", sql);                
            Assert.assertEquals(sql,"SELECT <slot 7> `eventdate` AS `eventdate`, <slot 8> sum(`pv`) AS `sum(``pv``)` FROM (" + 
                "SELECT <slot 3> `eventdate` AS `eventdate`, <slot 4> count(`userid`) AS `pv` FROM `testCluster:testDb`.`appevent` WHERE " + 
                "((`eventdate` > '2020-01-13') AND (`eventdate` < '2020-01-16')) AND (`eventid` = 1) GROUP BY `eventdate`) tbl GROUP BY `eventdate`");
        } catch(Exception e){
            LOG.warn("sub ex={}",e);
            Assert.fail(e.getMessage());
        }
    }
}


