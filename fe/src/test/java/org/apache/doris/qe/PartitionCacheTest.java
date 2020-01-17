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

import org.apache.doris.qe.CacheTestDB;
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
import org.apache.doris.catalog.Table;
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
@PrepareForTest({CacheAnalyzer.class, CacheTestDB.class})
public class PartitionCacheTest {
    private static final Logger LOG = LogManager.getLogger(PartitionCacheTest.class);
    private Planner planner;
    private ConnectScheduler scheduler;
    private CacheTestDB testDB;
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
        testDB = new CacheTestDB();
        try{
            testDB.init();
        } catch (Exception e){
            LOG.warn("Init error={}",e);
        }
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

    @Test
    public void testCacheModelSimple() throws Exception {
        String stmt = new String("SELECT country, COUNT(userid) FROM userprofile GROUP BY country");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        StatementBase parseStmt = null;
        try {
            parseStmt = (StatementBase) parser.parse().value;
        } catch (Exception e) {
            LOG.warn("Simple SQL={},msg={}", stmt, e.getMessage());
            Assert.fail(e.getMessage());
        }
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createProfileScanNode());
        
        CacheAnalyzer ca = new CacheAnalyzer(parseStmt, scanNodes);
        CacheModel cm = ca.checkCacheModel(0);
        LOG.warn("Simple SQL={}",stmt);
        LOG.warn("Simple CheckModel={}",cm);
        Assert.assertEquals(cm, CacheModel.Table);
    }
    
    @Test
    public void testWithinMinTime() throws Exception {
        String stmt = new String("SELECT country, COUNT(userid) FROM userprofile GROUP BY country");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        StatementBase parseStmt = null;
        try {
            parseStmt = (StatementBase) parser.parse().value;
        } catch (Exception e) {
            LOG.warn("MinTime SQL={},msg={}", stmt, e.getMessage());
            Assert.fail(e.getMessage());
        }
        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createProfileScanNode());

        CacheAnalyzer ca = new CacheAnalyzer(parseStmt, scanNodes);
        CacheModel cm = ca.checkCacheModel(1579024800000L); //2020-1-15 10:01:01
        LOG.warn("MinTime SQL={}",stmt);
        LOG.warn("MinTime CheckModel={}",cm);
        Assert.assertEquals(cm, CacheModel.None);
    }
    
    @Test
    public void testPartitionCache() throws Exception {
        String stmt = new String("SELECT date, COUNT(userid) FROM appevent WHERE date>=202014 and date<=202015 GROUP BY date");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        StatementBase parseStmt = null;
        try {
            parseStmt = (StatementBase) parser.parse().value;
        } catch (Exception e) {
            LOG.warn("Part SQL={},ps_ex={}", stmt, e);
            Assert.fail(e.getMessage());
        }

        try {
            parseStmt.analyze(testDB.createAnalyzer());
        } catch (AnalysisException e) {
            LOG.warn("Part,an_ex={}", e);
            //Assert.fail(e.getMessage());
        } catch (UserException e) {            
            LOG.warn("Part,ue_ex={}", e);
            //Assert.fail(e.getMessage());
        } catch (Exception e) {
            LOG.warn("Part,cm_ex={}", e);
            //Assert.fail(e.getMessage());
        }

        List<ScanNode> scanNodes = Lists.newArrayList(testDB.createEventScanNode());

        CacheAnalyzer ca = new CacheAnalyzer(parseStmt, scanNodes);
        CacheModel cm = ca.checkCacheModel(1579053661000L); //2020-1-15 10:01:01
        LOG.warn("Part SQL={}",stmt);
        LOG.warn("Part CheckModel={}",cm);
        Assert.assertEquals(cm, CacheModel.Partition);
    }
}

