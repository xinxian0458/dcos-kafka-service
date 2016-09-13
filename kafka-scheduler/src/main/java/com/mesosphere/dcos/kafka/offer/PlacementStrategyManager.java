package com.mesosphere.dcos.kafka.offer;

import com.mesosphere.dcos.kafka.state.KafkaSchedulerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mesosphere.dcos.kafka.config.KafkaSchedulerConfiguration;
import org.apache.mesos.offer.AnyPlacementStrategy;
import org.apache.mesos.offer.PlacementStrategy;

class PlacementStrategyManager {
  private static final Log log = LogFactory.getLog(PlacementStrategy.class);

  private final KafkaSchedulerState schedulerState;

  PlacementStrategyManager(KafkaSchedulerState schedulerState) {
    this.schedulerState = schedulerState;
  }

  public PlacementStrategy getPlacementStrategy(KafkaSchedulerConfiguration config) {
    String placementStrategy = config.getServiceConfiguration().getPlacementStrategy();

    log.info("Using placement strategy: " + placementStrategy);

    switch (placementStrategy) {
      case "ANY":
        log.info("Returning ANY strategy");
        return new AnyPlacementStrategy();
      case "NODE":
        log.info("Returning NODE strategy");
        return new NodePlacementStrategy(schedulerState);
      default:
        log.info("Returning DEFAULT strategy");
        return new AnyPlacementStrategy();
    }
  }
}
