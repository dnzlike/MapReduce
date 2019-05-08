package sjtu.sdic.mapreduce.rpc;

import com.alipay.sofa.rpc.config.ConsumerConfig;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Cachhe on 2019/4/22.
 */
public class Call {
    /**
     * get RPC service of the master with specified address
     *
     * @param address master address
     * @return the master's RPC service, null if no service runs at given address
     */
    public static MasterRpcService getMasterRpcService(String address) {
        return Master.getMasterRpcService(address);
    }

    /**
     * get RPC service of the worker with specified address
     *
     * @param address worker address
     * @return the worker's RPC service, null if no service runs at given address
     */
    public static WorkerRpcService getWorkerRpcService(String address) {
        return Worker.getWorkerRpcService(address);
    }
}
