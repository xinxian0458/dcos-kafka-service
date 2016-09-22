package com.mesosphere.dcos.kafka.scheduler;

import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.kafka.commons.state.KafkaState;
import com.mesosphere.dcos.kafka.config.ConfigStateUpdater;
import com.mesosphere.dcos.kafka.config.ConfigStateValidator.ValidationError;
import com.mesosphere.dcos.kafka.config.ConfigStateValidator.ValidationException;
import com.mesosphere.dcos.kafka.config.KafkaConfigState;
import com.mesosphere.dcos.kafka.config.KafkaSchedulerConfiguration;
import com.mesosphere.dcos.kafka.offer.KafkaOfferRequirementProvider;
import com.mesosphere.dcos.kafka.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.kafka.offer.PersistentOperationRecorder;
import com.mesosphere.dcos.kafka.plan.KafkaUpdatePhase;
import com.mesosphere.dcos.kafka.repair.KafkaFailureMonitor;
import com.mesosphere.dcos.kafka.repair.KafkaRecoveryRequirementProvider;
import com.mesosphere.dcos.kafka.repair.KafkaTaskFailureListener;
import com.mesosphere.dcos.kafka.state.ClusterState;
import com.mesosphere.dcos.kafka.state.KafkaSchedulerState;
import io.dropwizard.setup.Environment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.RecoveryConfiguration;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.offer.ResourceCleaner;
import org.apache.mesos.offer.ResourceCleanerScheduler;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.DefaultTaskKiller;
import org.apache.mesos.scheduler.SchedulerDriverFactory;
import org.apache.mesos.scheduler.plan.*;
import org.apache.mesos.scheduler.recovery.DefaultRecoveryScheduler;
import org.apache.mesos.scheduler.recovery.RecoveryRequirementProvider;
import org.apache.mesos.scheduler.recovery.RecoveryStatus;
import org.apache.mesos.scheduler.recovery.constrain.LaunchConstrainer;
import org.apache.mesos.scheduler.recovery.constrain.TimedLaunchConstrainer;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Kafka Framework Scheduler.
 */
public class KafkaScheduler implements Scheduler, Runnable {
  private static final Log log = LogFactory.getLog(KafkaScheduler.class);

  private static final int TWO_WEEK_SEC = 2 * 7 * 24 * 60 * 60;

  private final KafkaConfigState configState;
  private final KafkaSchedulerConfiguration config;
  private final KafkaSchedulerState schedulerState;
  private final KafkaState kafkaState;
  private final ClusterState clusterState;

  private final DefaultPlanScheduler stageScheduler;
  private final DefaultRecoveryScheduler recoveryScheduler;
  private final KafkaTaskFailureListener kafkaTaskFailureListener;

  private final OfferAccepter offerAccepter;
  private final Reconciler reconciler;
  private final PlanManager installPlanManager;
  private final AtomicReference<RecoveryStatus> recoveryStatusRef;
  private SchedulerDriver driver;
  private static final Integer restartLock = 0;
  private static List<TaskInfo> tasksToRestart = new ArrayList<>();
  private static final Integer rescheduleLock = 0;
  private static List<TaskInfo> tasksToReschedule = new ArrayList<>();

  private boolean isRegistered = false;

  // Possibly inconsistent with master.
  private boolean isSuppressed = false;

  public KafkaScheduler(KafkaSchedulerConfiguration configuration, Environment environment)
          throws ConfigStoreException, URISyntaxException {
    ConfigStateUpdater configStateUpdater = new ConfigStateUpdater(configuration);
    List<String> stageErrors = new ArrayList<>();
    KafkaSchedulerConfiguration targetConfigToUse;

    try {
      targetConfigToUse = configStateUpdater.getTargetConfig();
    } catch (ValidationException e) {
      // New target config failed to validate and was not used. Fall back to previous target config.
      log.error("Got " + e.getValidationErrors().size() +
              " errors from new config. Falling back to last valid config.");
      targetConfigToUse = configStateUpdater.getConfigState().getTargetConfig();
      for (ValidationError err : e.getValidationErrors()) {
        stageErrors.add(err.toString());
      }
    }

    configState = configStateUpdater.getConfigState();
    schedulerState = configStateUpdater.getSchedulerState();
    kafkaState = configStateUpdater.getKafkaState();

    config = targetConfigToUse;
    reconciler = new DefaultReconciler(schedulerState);
    clusterState = new ClusterState();

    offerAccepter =
      new OfferAccepter(Arrays.asList(new PersistentOperationRecorder(schedulerState)));

    KafkaOfferRequirementProvider offerRequirementProvider =
      new PersistentOfferRequirementProvider(schedulerState, configState, clusterState);

    List<Phase> phases = Arrays.asList(
        ReconciliationPhase.create(reconciler),
        new KafkaUpdatePhase(
                configState.getTargetName().toString(),
                config,
                schedulerState,
                offerRequirementProvider));
    // If config validation had errors, expose them via the Stage.
    Plan installPlan = stageErrors.isEmpty()
        ? DefaultPlan.fromList(phases)
        : DefaultPlan.withErrors(phases, stageErrors);

    recoveryStatusRef = new AtomicReference<>(new RecoveryStatus(Collections.emptyList(), Collections.emptyList()));
    kafkaTaskFailureListener = new KafkaTaskFailureListener(schedulerState);
    stageScheduler = new DefaultPlanScheduler(
            offerAccepter,
            new DefaultTaskKiller(schedulerState, kafkaTaskFailureListener));

    installPlanManager = createInstallStageManager(installPlan, getPhaseStrategyFactory(config));
    recoveryScheduler = createRecoveryScheduler(offerRequirementProvider);
  }

  protected DefaultRecoveryScheduler createRecoveryScheduler(KafkaOfferRequirementProvider offerRequirementProvider) {
    RecoveryConfiguration recoveryConfiguration = config.getRecoveryConfiguration();
    LaunchConstrainer constrainer = new TimedLaunchConstrainer(
            Duration.ofSeconds(recoveryConfiguration.getRepairDelaySecs()));
    RecoveryRequirementProvider recoveryRequirementProvider =
            new KafkaRecoveryRequirementProvider(
                    offerRequirementProvider,
                    configState.getConfigStore());
    return new DefaultRecoveryScheduler(
            schedulerState,
            kafkaTaskFailureListener,
            recoveryRequirementProvider,
            offerAccepter,
            constrainer,
            new KafkaFailureMonitor(recoveryConfiguration),
            recoveryStatusRef);
  }

  protected PlanManager createInstallStageManager(Plan installStage, PhaseStrategyFactory strategyFactory) {
    return new DefaultPlanManager(installStage, strategyFactory);
  }

  public KafkaSchedulerConfiguration getKafkaSchedulerConfiguration() {
    return config;
  }

  private static PhaseStrategyFactory getPhaseStrategyFactory(KafkaSchedulerConfiguration config) {
    String strategy = config.getServiceConfiguration().getPhaseStrategy();

    switch (strategy) {
      case "INSTALL":
        return new DefaultStrategyFactory();
      case "STAGE":
        return new StageStrategyFactory();
      default:
        log.warn("Unknown strategy: " + strategy);
        return new StageStrategyFactory();
    }
  }

  public static void restartTasks(TaskInfo taskInfo) {
    synchronized (restartLock) {
      tasksToRestart.add(taskInfo);
    }
  }

  public static void rescheduleTask(TaskInfo taskInfo) {
    synchronized (rescheduleLock) {
      tasksToReschedule.add(taskInfo);
    }
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID, int status) {
    log.info("Executor lost: executorId: " + executorID.getValue()
        + " slaveId: " + slaveID.getValue() + " status: " + status);
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    log.info("Slave lost slaveId: " + slaveId.getValue());
  }

  @Override
  public void frameworkMessage(
      SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID, byte[] data) {
    log.info("Framework message: executorId: " + executorID.getValue() + " slaveId: "
        + slaveID.getValue() + " data: '" + Arrays.toString(data) + "'");
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    log.info("Offer rescinded: offerId: " + offerId.getValue());
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    log.info("Registered framework with frameworkId: " + frameworkId.getValue());
    try {
      schedulerState.setFrameworkId(frameworkId);
      isRegistered = true;
    } catch (Exception e) {
      isRegistered = false;
      log.error(String.format(
          "Unable to store registered framework ID '%s'", frameworkId.getValue()), e);
      //TODO(nick): exit process?
    }
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework.");
    try {
      isRegistered = true;
      reconcile();
    } catch (Exception e) {
      isRegistered = false;
      log.error("Unable to trigger reconciliation after re-registration", e);
      //TODO(nick): exit process?
    }
  }

  public boolean isRegistered() {
    return isRegistered;
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    log.info(String.format(
        "Received status update for taskId=%s state=%s message='%s'",
        status.getTaskId().getValue(),
        status.getState().toString(),
        status.getMessage()));

    // Store status, then pass status to StageManager => Plan => Blocks
    try {
      schedulerState.updateStatus(status);
      installPlanManager.update(status);
    } catch (Exception e) {
      log.warn("Failed to update TaskStatus received from Mesos. "
          + "This may be expected if Mesos sent stale status information: " + status, e);
    }

    if (hasOperations()) {
      reviveOffers(driver);
    }
  }

  private boolean hasOperations() {
    return !installPlanManager.getPlan().isComplete() ||
            recoveryScheduler.hasOperations(null);
  }

  private void reviveOffers(SchedulerDriver driver) {
    log.info("Reviving offers.");
    driver.reviveOffers();
    schedulerState.setSuppressed(false);
  }

  private void suppressOffers(SchedulerDriver driver) {
    log.info("Suppressing offers.");
    driver.suppressOffers();
    schedulerState.setSuppressed(true);
  }


  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    try {
      logOffers(offers);
      reconciler.reconcile(driver);
      processTaskOperations(driver);

      List<OfferID> acceptedOffers = new ArrayList<>();

      if (!reconciler.isReconciled()) {
        log.info("Accepting no offers: Reconciler is still in progress");
      } else {
        Optional<Block> block = installPlanManager.getCurrentBlock();
        if (block.isPresent()) {
            acceptedOffers = stageScheduler.resourceOffers(driver, offers, block.get());
        }
        List<Offer> unacceptedOffers = filterAcceptedOffers(offers, acceptedOffers);

        try {
          acceptedOffers.addAll(recoveryScheduler.resourceOffers(driver, unacceptedOffers, block));
        } catch (InvalidRequirementException e) {
          log.error("Error recovering block: " + block + " Reason: " + e);
        }

        ResourceCleanerScheduler cleanerScheduler = getCleanerScheduler();
        if (cleanerScheduler != null) {
          acceptedOffers.addAll(getCleanerScheduler().resourceOffers(driver, offers));
        }
      }

      log.info(String.format("Accepted %d of %d offers: %s",
              acceptedOffers.size(), offers.size(), acceptedOffers));
      declineOffers(driver, acceptedOffers, offers);

      if (!hasOperations()) {
        suppressOffers(driver);
      }
    } catch (Exception ex) {
      log.error("Unexpected exception encountered when processing offers", ex);
    }
  }

  private ResourceCleanerScheduler getCleanerScheduler() {
    try {
      ResourceCleaner cleaner = new ResourceCleaner(schedulerState);
      return new ResourceCleanerScheduler(cleaner, offerAccepter);
    } catch (Exception ex) {
      log.error("Failed to construct ResourceCleaner", ex);
      return null;
    }
  }

  private List<Offer> filterAcceptedOffers(List<Offer> offers, List<OfferID> acceptedOfferIds) {
    List<Offer> filteredOffers = new ArrayList<Offer>();

    for (Offer offer : offers) {
      if (!offerAccepted(offer, acceptedOfferIds)) {
        filteredOffers.add(offer);
      }
    }

    return filteredOffers;
  }

  private boolean offerAccepted(Offer offer, List<OfferID> acceptedOfferIds) {
    for (OfferID acceptedOfferId: acceptedOfferIds) {
      if(acceptedOfferId.equals(offer.getId())) {
        return true;
      }
    }

    return false;
  }

  private void processTaskOperations(SchedulerDriver driver) {
    processTasksToRestart(driver);
    processTasksToReschedule(driver);
  }

  private void processTasksToRestart(SchedulerDriver driver) {
    synchronized (restartLock) {
      for (TaskInfo taskInfo : tasksToRestart) {
        if (taskInfo != null) {
          log.info("Restarting task: " + taskInfo.getTaskId().getValue());
          driver.killTask(taskInfo.getTaskId());
        } else {
          log.warn("Asked to restart null task.");
        }
      }

      tasksToRestart = new ArrayList<>();
    }
  }

  private void processTasksToReschedule(SchedulerDriver driver) {
    synchronized (rescheduleLock) {
      for (TaskInfo taskInfo : tasksToReschedule) {
        if (taskInfo != null) {
          log.info("Rescheduling task: " + taskInfo.getTaskId().getValue());
          kafkaTaskFailureListener.taskFailed(taskInfo.getTaskId());
          driver.killTask(taskInfo.getTaskId());
        } else {
          log.warn("Asked to reschedule null task.");
        }
      }

      tasksToReschedule = new ArrayList<>();
    }
  }

  @Override
  public void run() {
    Thread.currentThread().setName("KafkaScheduler");
    Thread.currentThread().setUncaughtExceptionHandler(getUncaughtExceptionHandler());

    String zkPath = "zk://" + config.getKafkaConfiguration().getMesosZkUri() + "/mesos";
    FrameworkInfo fwkInfo = getFrameworkInfo();

    registerFramework(this, fwkInfo, zkPath);
  }

  private void reconcile() {
    Block recBlock = getReconciliationBlock();

    if (recBlock != null) {
      recBlock.restart();
    } else {
      log.error("Failed to reconcile because unable to find the Reconciliation Block");
    }
  }

  private Block getReconciliationBlock() {
    Plan stage = installPlanManager.getPlan();

    for (Phase phase : stage.getPhases()) {
      for (Block block : phase.getBlocks()) {
        if (block instanceof ReconciliationBlock) {
          return block;
        }
      }
    }

    return null;
  }

  private FrameworkInfo getFrameworkInfo() {
    FrameworkInfo.Builder fwkInfoBuilder = FrameworkInfo.newBuilder()
      .setName(config.getServiceConfiguration().getName())
      .setFailoverTimeout(TWO_WEEK_SEC)
      .setUser(config.getServiceConfiguration().getUser())
      .setRole(config.getServiceConfiguration().getRole())
      .setPrincipal(config.getServiceConfiguration().getPrincipal())
      .setCheckpoint(true);

    FrameworkID fwkId = schedulerState.getFrameworkId();
    if (fwkId != null) {
      fwkInfoBuilder.setId(fwkId);
    }

    return fwkInfoBuilder.build();
  }

 private void logOffers(List<Offer> offers) {
    if (offers == null) {
      return;
    }

    log.info(String.format("Received %d offers:", offers.size()));
    for (int i = 0; i < offers.size(); ++i) {
      // offer protos are very long. print each as a single line:
      log.info(String.format("- Offer %d: %s", i + 1, TextFormat.shortDebugString(offers.get(i))));
    }
  }

  private void declineOffers(SchedulerDriver driver, List<OfferID> acceptedOffers, List<Offer> offers) {
    for (Offer offer : offers) {
      OfferID offerId = offer.getId();
      if (!acceptedOffers.contains(offerId)) {
        log.info("Declining offer: " + offerId.getValue());
        driver.declineOffer(offerId);
      }
    }
  }

  private void registerFramework(KafkaScheduler sched, FrameworkInfo frameworkInfo, String masterUri) {
    log.info("Registering without authentication.  Framework Info: " + frameworkInfo);
    driver = new SchedulerDriverFactory().create(sched, frameworkInfo, masterUri);
    driver.run();
  }

  private Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {

    return new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        final String msg = "Scheduler exiting due to uncaught exception";
        log.error(msg, e);
        log.fatal(msg, e);
        System.exit(2);
      }
    };
  }

  public KafkaConfigState getConfigState() {
    return configState;
  }

  public KafkaSchedulerState getSchedulerState() {
    return schedulerState;
  }

  public KafkaState getKafkaState() {
    return kafkaState;
  }

  public PlanManager getInstallPlanManager() {
    return installPlanManager;
  }

  public AtomicReference<RecoveryStatus> getRecoveryStatusRef() {
    return recoveryStatusRef;
  }
}
