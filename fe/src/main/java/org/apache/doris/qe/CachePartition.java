package org.apache.doris.qe;

import org.apache.doris.common.Status;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.PFetchDataRequest;
import org.apache.doris.system.Backend;

import java.util.*;

public class CachePartition {
    private static final int VIRTUAL_NODES = 10;
    private List<Long> realNodes = new LinkedList<Long>();
    private SortedMap<Integer, Backend> virtualNodes = new TreeMap<Integer, Backend>();
    private static CachePartition cachePartition;

    public class CacheResult {
        RowBatch rowBatch;
        List<Integer> cacheRange;
    }

    public static CachePartition getInstance() {
        if (cachePartition == null) {
            cachePartition = new CachePartition();
        }
        return cachePartition;
    }

    protected CachePartition() {
    }

    public CacheResult getCacheData(String query) {
        CacheResult result = new CacheResult();
        Backend backend = findBackend(query);
        TNetworkAddress address = TNetworkAddress(backend.getHost(), backend.getBePort());
        final RowBatch rowBatch = new RowBatch();
        try {
            PFetchDataRequest request = new PFetchDataRequest(finstId);
            Future<PFetchDataResult> future = BackendServiceProxy.getInstance().getCacheData(address, request);
            PFetchDataResult pResult = null;
            while (pResult == null) {
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    throw new TimeoutException("query timeout");
                }
                try {
                    pResult = future.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.info("future get interrupted Exception");
                }
            }
            TStatusCode code = TStatusCode.findByValue(pResult.status.status_code);
            if (code != TStatusCode.OK) {
                status.setPstatus(pResult.status);
                return null;
            }
            rowBatch.setQueryStatistics(pResult.query_statistics);
            if (packetIdx != pResult.packet_seq) {
                LOG.warn("receive packet failed, expect={}, receive={}", packetIdx, pResult.packet_seq);
                status.setRpcStatus("receive error packet");
                return null;
            }

            byte[] serialResult = request.getSerializedResult();
            if (serialResult != null && serialResult.length > 0) {
                TResultBatch resultBatch = new TResultBatch();
                TDeserializer deserializer = new TDeserializer();
                deserializer.deserialize(resultBatch, serialResult);
                rowBatch.setBatch(resultBatch);
                rowBatch.setEos(pResult.eos);
                result.rowBatch = rowBatch;
                return result;
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception, finstId={}", finstId, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backendId);
        } catch (
                ExecutionException e) {
            LOG.warn("fetch result execution exception, finstId={}", finstId, e);
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.setStatus(new Status(TStatusCode.TIMEOUT, e.getMessage()));
            } else {
                status.setRpcStatus(e.getMessage());
                SimpleScheduler.addToBlacklist(backendId);
            }
        } catch (
                TimeoutException e) {
            LOG.warn("fetch result timeout, finstId={}", finstId, e);
            status.setStatus("query timeout");
        } finally {
            synchronized (this) {
                currentThread = null;
            }
        }
        result.rowBatch = rowBatch;
        return result;
    }

    public Backend findBackend(String query) {
        int hash = getHash(query);
        SortedMap<Integer, Backend> headMap = virtualNodes.headMap(hash);
        SortedMap<Integer, Backend> subMap = virtualNodes.tailMap(hash);
        int switchMap = 0;
        while (true) {
            if (subMap == null || subMap.size() == 0) {
                subMap = headMap;
                switchMap += 1;
                if (switchMap == 2) {
                    return null;
                }
            }
            Integer i = subMap.firstKey();
            Backend virtualNode = subMap.get(i);
            if (SimpleScheduler.isAlive(virtualNode)) {
                return virtualNode;
            }
            subMap = subMap.tailMap(i + 1);
        }
    }

    public void addBackends(ImmutableMap<Long, Backend> backends) {
        for (Map.Entry<Long, Backend> entry : backends.entrySet()) {
            Long backendID = entry.getKey();
            Backend backend = entry.getValue();
            addBackend(backendID, backend);
        }
    }

    public void addBackend(Long backendID, Backend backend) {
        realNodes.add(backendID);
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            String virtualNodeName = backendID.toString() + "::" + String.valueOf(i);
            int hash = getHash(virtualNodeName);
            virtualNodes.put(hash, backend);
        }
    }

    private int getHash(String str) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < str.length(); i++)
            hash = (hash ^ str.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        if (hash < 0)
            hash = Math.abs(hash);
        return hash;
    }
}
