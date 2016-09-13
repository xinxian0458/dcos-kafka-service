package com.mesosphere.dcos.kafka.state;

import com.mesosphere.dcos.kafka.config.ZookeeperConfiguration;
import com.mesosphere.dcos.kafka.offer.OfferUtils;
import org.apache.mesos.Protos.*;
import org.apache.mesos.state.StateStoreException;
import org.apache.mesos.state.SchedulerState;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;


/**
 * Read/write interface for storing and retrieving information about Framework tasks. The underlying data is stored
 * against Executor IDs of "broker-0", "broker-1", etc.
 */
public class KafkaSchedulerState extends SchedulerState {
    private static final Logger log = LoggerFactory.getLogger(KafkaSchedulerState.class);

    public KafkaSchedulerState(ZookeeperConfiguration zkConfig) {
        super(zkConfig.getFrameworkName(), zkConfig.getMesosZkUri());
    }

    public int getRunningBrokersCount() throws StateStoreException {
        int count = 0;

        for (TaskStatus taskStatus : getTaskStatuses()) {
            if (taskStatus.getState().equals(TaskState.TASK_RUNNING)) {
                count++;
            }
        }

        return count;
    }

    /**
     * Returns the full Task ID (including UUID) for the provided Broker index, or {@code null} if none is found.
     */
    public TaskID getTaskIdForBroker(Integer brokerId) throws Exception {
        TaskInfo taskInfo = getTaskInfoForBroker(brokerId);
        return (taskInfo != null) ? taskInfo.getTaskId() : null;
    }

    /**
     * Returns the TaskInfo for the provided Broker index, or {@code null} if none is found.
     */
    public TaskInfo getTaskInfoForBroker(Integer brokerId) throws Exception {
        try {
            return fetchTask(OfferUtils.brokerIdToTaskName(brokerId)).orElse(null);
        } catch (StateStoreException e) {
            log.warn(String.format(
                    "Failed to get TaskInfo for broker %d. This is expected when the service is "
                            + "starting for the first time.", brokerId), e);
            return null;
        }
    }

    /**
     * Returns the TaskStatus for the provided Broker index, or {@code null} if none is found.
     */
    public TaskStatus getTaskStatusForBroker(Integer brokerId) throws Exception {
        try {
            return fetchStatus(OfferUtils.brokerIdToTaskName(brokerId));
        } catch (StateStoreException e) {
            log.warn(String.format(
                    "Failed to get TaskStatus for broker %d. This is expected when the service is "
                            + "starting for the first time.", brokerId), e);
            return null;
        }
    }
}
