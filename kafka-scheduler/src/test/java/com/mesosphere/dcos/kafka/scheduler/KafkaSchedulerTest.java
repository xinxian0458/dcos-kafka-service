package com.mesosphere.dcos.kafka.scheduler;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mesosphere.dcos.kafka.config.DropwizardConfiguration;
import com.mesosphere.dcos.kafka.config.KafkaSchedulerConfiguration;
import com.mesosphere.dcos.kafka.offer.KafkaOfferRequirementProvider;
import com.mesosphere.dcos.kafka.test.KafkaDropwizardAppRule;
import io.dropwizard.setup.Environment;
import io.dropwizard.testing.ResourceHelpers;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.scheduler.plan.PhaseStrategyFactory;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.PlanManager;
import org.apache.mesos.scheduler.recovery.DefaultRecoveryScheduler;
import org.apache.mesos.state.StateStore;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.Mockito.*;

/**
 * This class tests the KafkaScheduler class.
 */
public class KafkaSchedulerTest {
    private static KafkaSchedulerConfiguration kafkaSchedulerConfiguration;
    private static Environment environment;

    private static final String testFrameworkName = "kafka";
    private static final String testZkConnectionString = "localhost:40000";
    private static final String testTaskName = "test-task-name";
    private static final String testFrameworkId = "test-framework-id";

    private static final Protos.TaskID testTaskId = Protos.TaskID.newBuilder()
            .setValue(testTaskName + "__" + UUID.randomUUID())
            .build();
    private static final Protos.TaskInfo testTaskInfo = Protos.TaskInfo.newBuilder()
            .setName(testTaskName)
            .setTaskId(testTaskId)
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("test-agent-id"))
            .build();
    private static final Protos.TaskStatus testTaskStatus = Protos.TaskStatus.newBuilder()
            .setTaskId(testTaskId)
            .setState(Protos.TaskState.TASK_RUNNING)
            .build();
    private KafkaScheduler kafkaScheduler;

    public static Injector injector;

    @Mock private SchedulerDriver driver;

    @ClassRule
    public static KafkaDropwizardAppRule<DropwizardConfiguration> RULE =
            new KafkaDropwizardAppRule<>(Main.class, ResourceHelpers.resourceFilePath("scheduler.yml"));

    @Before
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
        kafkaScheduler = getTestKafkaScheduler();
    }

    @BeforeClass
    public static void before() {
        final Main main = (Main) RULE.getApplication();
        kafkaSchedulerConfiguration = main.getKafkaSchedulerConfiguration();

        environment = main.getEnvironment();
        injector = Guice.createInjector(new TestModule(kafkaSchedulerConfiguration, environment));
    }

    @Test
    public void testResourceOffersEmpty() {
        kafkaScheduler.resourceOffers(driver, Collections.emptyList());
        verify(driver, times(1)).reconcileTasks(anyObject());
    }

    @Test
    public void testRestartEmptyTasks() {
        KafkaScheduler.restartTasks(null);
        kafkaScheduler.resourceOffers(driver, Collections.emptyList());
        verify(driver, times(0)).killTask(anyObject());
    }

    @Test
    public void testRestartOneTask() {
        StateStore stateStore = new CuratorStateStore(testFrameworkName, testZkConnectionString);
        stateStore.storeTasks(Arrays.asList(testTaskInfo));
        stateStore.storeStatus(testTaskStatus);
        KafkaScheduler.restartTasks(testTaskInfo);
        kafkaScheduler.resourceOffers(driver, Collections.emptyList());
        verify(driver, times(1)).killTask(anyObject());
    }

    @Test
    public void testRescheduleOneTask() {
        StateStore stateStore = new CuratorStateStore(testFrameworkName, testZkConnectionString);
        stateStore.storeTasks(Arrays.asList(testTaskInfo));
        stateStore.storeStatus(testTaskStatus);
        KafkaScheduler.rescheduleTask(testTaskInfo);
        kafkaScheduler.resourceOffers(driver, Collections.emptyList());
        verify(driver, times(1)).killTask(anyObject());
    }

    @Test
    public void testResourceOffersSuppress() throws Exception {
        kafkaScheduler = new KafkaScheduler(kafkaSchedulerConfiguration, environment) {
            // Create install StageManager with no operations
            @Override
            protected PlanManager createDeploymentPlanManager(Plan deploymentPlan, PhaseStrategyFactory strategyFactory) {
                PlanManager planManager = super.createDeploymentPlanManager(deploymentPlan, strategyFactory);
                PlanManager planManagerSpy = spy(planManager);
                Plan stage = planManagerSpy.getPlan();
                Plan stageSpy = spy(stage);
                when(stageSpy.isComplete()).thenReturn(true);
                when(planManagerSpy.getPlan()).thenReturn(stageSpy);
                return planManagerSpy;
            }

            // Create RecoveryScheduler with no operations
            @Override
            protected DefaultRecoveryScheduler createRecoveryScheduler(KafkaOfferRequirementProvider offerRequirementProvider) {
                DefaultRecoveryScheduler scheduler = super.createRecoveryScheduler(offerRequirementProvider);
                DefaultRecoveryScheduler spy = spy(scheduler);
                when(spy.hasOperations(any())).thenReturn(false);
                return spy;
            }
        };

        kafkaScheduler.resourceOffers(driver, Collections.emptyList());
        verify(driver, times(1)).suppressOffers();
    }


    @Test
    public void testStatusUpdateRevive() throws Exception {
        kafkaScheduler.statusUpdate(driver, getTestTaskStatus());
        verify(driver, times(1)).reviveOffers();
    }

    @Test
    public void testRegistered() {
        Assert.assertNull(kafkaScheduler.getFrameworkState().getFrameworkId());
        kafkaScheduler.registered(driver, getTestFrameworkId(), null);
        Assert.assertEquals(getTestFrameworkId(), kafkaScheduler.getFrameworkState().getFrameworkId());
    }

    @Test
    public void testStatusUpdate() {
        kafkaScheduler.statusUpdate(driver, getTestTaskStatus());
    }

    @Test
    public void testReconciled() {
        kafkaScheduler.reregistered(driver, null);
    }

    private KafkaScheduler getTestKafkaScheduler() throws Exception {
        return new KafkaScheduler(kafkaSchedulerConfiguration, environment);
    }

    private Protos.FrameworkID getTestFrameworkId() {
        return Protos.FrameworkID.newBuilder()
                .setValue(testFrameworkId)
                .build();
    }

    private Protos.TaskStatus getTestTaskStatus() {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(testTaskInfo.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
    }
}
