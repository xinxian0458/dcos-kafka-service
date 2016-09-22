package com.mesosphere.dcos.kafka.plan;

import com.mesosphere.dcos.kafka.config.KafkaConfigState;
import com.mesosphere.dcos.kafka.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.kafka.state.ClusterState;
import com.mesosphere.dcos.kafka.state.KafkaSchedulerState;
import com.mesosphere.dcos.kafka.test.ConfigTestUtils;
import com.mesosphere.dcos.kafka.test.KafkaTestUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.offer.OfferRequirement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * This class tests the KafkaUpdateBlock class.
 */
public class KafkaUpdateBlockTest {
    @Mock private KafkaSchedulerState schedulerState;
    @Mock private KafkaConfigState configState;
    @Mock private ClusterState clusterState;
    @Mock private Capabilities capabilities;
    private PersistentOfferRequirementProvider offerRequirementProvider;
    private KafkaUpdateBlock updateBlock;

    @Before
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(schedulerState.getFrameworkId()).thenReturn(KafkaTestUtils.testFrameworkId);
        when(configState.fetch(UUID.fromString(KafkaTestUtils.testConfigName))).thenReturn(
                ConfigTestUtils.getTestKafkaSchedulerConfiguration());
        when(capabilities.supportsNamedVips()).thenReturn(true);
        when(clusterState.getCapabilities()).thenReturn(capabilities);
        offerRequirementProvider = new PersistentOfferRequirementProvider(schedulerState, configState, clusterState);
        updateBlock =
                new KafkaUpdateBlock(
                        schedulerState,
                        offerRequirementProvider,
                        KafkaTestUtils.testConfigName,
                        0);
    }

    @Test
    public void testKafkaUpdateBlockConstruction() {
        Assert.assertNotNull(updateBlock);
    }

    @Test
    public void testStart() {
        Optional<OfferRequirement> offerRequirement = updateBlock.start();
        Assert.assertTrue(offerRequirement.isPresent());
        Assert.assertEquals(1, offerRequirement.get().getTaskRequirements().size());
        Assert.assertNotNull(offerRequirement.get().getExecutorRequirement());
    }

    @Test
    public void testUpdateWhilePending() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.update(getRunningTaskStatus("bad-task-id"));
        Assert.assertTrue(updateBlock.isPending());
    }

    @Test
    public void testUpdateUnknownTaskId() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.start();
        updateBlock.updateOfferStatus(Optional.of(Collections.<Protos.Offer.Operation>singleton(null)));
        Assert.assertTrue(updateBlock.isInProgress());
        updateBlock.update(getRunningTaskStatus("bad-task-id"));
        Assert.assertTrue(updateBlock.isInProgress());
    }

    @Test
    public void testReconciliationUpdate() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.start();
        updateBlock.updateOfferStatus(Optional.of(Collections.<Protos.Offer.Operation>singleton(null)));
        Assert.assertTrue(updateBlock.isInProgress());
        Protos.TaskID taskId = updateBlock.getPendingTaskIds().get(0);
        Protos.TaskStatus reconciliationTaskStatus = getRunningTaskStatus(taskId.getValue());
        reconciliationTaskStatus = Protos.TaskStatus.newBuilder(reconciliationTaskStatus)
                .setReason(Protos.TaskStatus.Reason.REASON_RECONCILIATION)
                .build();
        updateBlock.update(reconciliationTaskStatus);
        Assert.assertTrue(updateBlock.isInProgress());
    }

    @Test
    public void testUpdateExpectedTaskIdRunning() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.start();
        updateBlock.updateOfferStatus(Optional.of(Collections.<Protos.Offer.Operation>singleton(null)));
        Assert.assertTrue(updateBlock.isInProgress());
        Protos.TaskID taskId = updateBlock.getPendingTaskIds().get(0);
        updateBlock.update(getRunningTaskStatus(taskId.getValue()));
        Assert.assertTrue(updateBlock.isComplete());
    }

    @Test
    public void testUpdateExpectedTaskIdTerminated() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.start();
        updateBlock.updateOfferStatus(Optional.of(Collections.<Protos.Offer.Operation>singleton(null)));
        Assert.assertTrue(updateBlock.isInProgress());
        Protos.TaskID taskId = updateBlock.getPendingTaskIds().get(0);
        updateBlock.update(getFailedTaskStatus(taskId.getValue()));
        Assert.assertTrue(updateBlock.isPending());
    }

    private Protos.TaskStatus getRunningTaskStatus(String taskId) {
        return getTaskStatus(taskId, Protos.TaskState.TASK_RUNNING);
    }

    private Protos.TaskStatus getFailedTaskStatus(String taskId) {
        return getTaskStatus(taskId, Protos.TaskState.TASK_FAILED);
    }

    private Protos.TaskStatus getTaskStatus(String taskId, Protos.TaskState state) {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId))
                .setState(state)
                .build();
    }
}
