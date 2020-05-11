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

package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TRoutineLoadTask;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.easymock.EasyMock;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({ Catalog.class })
public class RoutineLoadTaskSchedulerTest {

    @Test
    public void testSingleThreadSchedule() throws Exception {
        check(1, 2);
    }

    @Test
    public void testMultiThreadSchedule() throws Exception {
        check(4, 8);
    }

    private synchronized void check(int threadNumber, int taskNumber) throws Exception {
        long timeoutMs = 10000;
        String clusterName = "default-cluster";
        final Catalog catalog = mockCatalog();

        final AtomicInteger count = new AtomicInteger(0);
        List<RoutineLoadTaskInfo> taskInfos = new ArrayList<>(taskNumber);

        List<RoutineLoadTaskSchedulerImpl> slots = new ArrayList<>();
        Set<Long> s = new ConcurrentSkipListSet<>();
        for (int i = 0; i < taskNumber; ++i) {
            taskInfos.add(new RoutineLoadTaskInfo(UUID.randomUUID(), i, clusterName, timeoutMs) {
                @Override
                TRoutineLoadTask createRoutineLoadTask() throws UserException {
                    return null;
                }

                @Override
                String getTaskDataSourceProperties() {
                    return null;
                }

                @Override
                public boolean beginTxn() {
                    return true;
                }
            });
        }
        for (int i = 0; i < threadNumber; ++i) {
            slots.add(mockRoutineLoadTaskSchedlerImpl(catalog.getRoutineLoadManager(), i, count, s));
        }

        final RoutineLoadTaskScheduler scheduler = new RoutineLoadTaskScheduler(slots);

        scheduler.start();
        scheduler.addTasksInQueue(taskInfos);
        Thread.sleep(100);
        System.out.println(s.size() + " " + count.get());
        assert s.size() == threadNumber;
        Thread.sleep(300);
        assert count.get() == taskNumber;
    }

    private Catalog mockCatalog() {
        Catalog catalog = EasyMock.createMock(Catalog.class);

        RoutineLoadManager routineLoadManager = mockRoutineLoadManager();
        EasyMock.expect(catalog.isReady()).andReturn(true).anyTimes();
        EasyMock.expect(catalog.getRoutineLoadManager()).andReturn(routineLoadManager).anyTimes();
        EasyMock.replay(catalog);

        SystemInfoService systemInfoService = new SystemInfoService();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService);
        PowerMock.replay(Catalog.class);
        return catalog;
    }

    private RoutineLoadManager mockRoutineLoadManager() {
        RoutineLoadManager manager = Mockito.mock(RoutineLoadManager.class);
        // just cover this method.
        Mockito.doAnswer(invocation -> null).when(manager).updateBeIdToMaxConcurrentTasks();
        Mockito.doAnswer(invocation -> 1).when(manager).getClusterIdleSlotNum();
        Mockito.doAnswer(invocation -> true)
                .when(manager).checkTaskInJob(Mockito.any(UUID.class), Mockito.anyLong());
        return manager;
    }

    private RoutineLoadTaskSchedulerImpl mockRoutineLoadTaskSchedlerImpl(RoutineLoadManager manager,
            int id, final AtomicInteger count, Set<Long> s) throws Exception {
        final RoutineLoadTaskSchedulerImpl impl =
                Mockito.spy(new RoutineLoadTaskSchedulerImpl(manager, id));
        Mockito.doAnswer(invocation -> {
            s.add(Thread.currentThread().getId());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // need do nothing.
            }
            count.getAndIncrement();
            return null;
        }).when(impl).scheduleOneTask(Mockito.any(RoutineLoadTaskInfo.class));

        return impl;
    }
}
