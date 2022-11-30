package com.capitalone.dashboard.controller;

import com.capitalone.dashboard.collector.DefaultArtifactoryClient;
import com.capitalone.dashboard.misc.HygieiaException;
import com.capitalone.dashboard.model.ArtifactItem;
import com.capitalone.dashboard.model.ArtifactSyncRequest;
import com.capitalone.dashboard.model.ArtifactVersionRequest;
import com.capitalone.dashboard.model.BinaryArtifact;
import com.capitalone.dashboard.model.Collector;
import com.capitalone.dashboard.model.CollectorItem;
import com.capitalone.dashboard.model.CollectorType;
import com.capitalone.dashboard.model.Component;
import com.capitalone.dashboard.model.Dashboard;
import com.capitalone.dashboard.repository.ArtifactItemRepository;
import com.capitalone.dashboard.repository.BinaryArtifactRepository;
import com.capitalone.dashboard.repository.CollectorRepository;
import com.capitalone.dashboard.repository.ComponentRepository;
import com.capitalone.dashboard.repository.DashboardRepository;
import com.capitalone.dashboard.util.ArtifactUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@RestController
@Validated
@RequestMapping("/artifactory")
public class ArtifactoryController {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArtifactoryController.class);
  private final DefaultArtifactoryClient artifactoryClient;
  private final ArtifactItemRepository artifactItemRepository;
  private final ComponentRepository componentRepository;
  private final DashboardRepository dashboardRepository;
  private final BinaryArtifactRepository binaryArtifactRepository;
  private final CollectorRepository collectorRepository;


  @Autowired
  public ArtifactoryController(DefaultArtifactoryClient artifactoryClient,
                               ArtifactItemRepository artifactItemRepository,
                               ComponentRepository componentRepository,
                               DashboardRepository dashboardRepository,
                               BinaryArtifactRepository binaryArtifactRepository,
                               CollectorRepository collectorRepository) {
    this.artifactoryClient = artifactoryClient;
    this.artifactItemRepository = artifactItemRepository;
    this.componentRepository = componentRepository;
    this.dashboardRepository = dashboardRepository;
    this.binaryArtifactRepository = binaryArtifactRepository;
    this.collectorRepository = collectorRepository;
  }

  @RequestMapping(value = "/refresh", method = POST, consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
  public ResponseEntity<String> refresh(@RequestBody ArtifactSyncRequest request) throws HygieiaException {
    List<String> dashboards = request.getDashboards();
    if (Objects.isNull(request.getStartTime()) || Objects.isNull(request.getEndTime())) return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body("StartTime or EndTime is null");
    long startTime = request.getStartTime();
    long endTime = request.getEndTime();
    int dashboardCount = 0;
    int collectorItemsCount = 0;
    int actualCollectorItemsDataPresent = 0;
    List<String> dashboardNotPresent = new ArrayList<>();
    List<String> dashboardNotConfiguredWithArtifactory = new ArrayList<>();
    List<String> collectorItemDataNotFound = new ArrayList<>();
    for (String dashboard : dashboards) {
      List<Dashboard> ds = dashboardRepository.findByTitle(dashboard);
      if(CollectionUtils.isEmpty(ds)){
        dashboardNotPresent.add(dashboard);
      }
      dashboardCount = dashboardCount + ds.size();
      for (Dashboard d : ds) {
        Component component = (Component) componentRepository.findAllById(Collections.singleton(d.getApplication().getComponents().get(0).getId()));
        List<CollectorItem> collectorItems = component.getCollectorItems().get(CollectorType.Artifact);
        if(CollectionUtils.isEmpty(collectorItems)) {
          dashboardNotConfiguredWithArtifactory.add(dashboard);
          continue;
        }
        collectorItemsCount = collectorItemsCount + collectorItems.size();
        LOGGER.info("dashboard name=" + dashboard + ", artifact collector-Items=" + collectorItems.size());
        for (CollectorItem c : collectorItems) {
          ArtifactItem artifactItem = new ArtifactItem((String)c.getOptions().get("repoName"),(String)c.getOptions().get("artifactName"),(String)c.getOptions().get("path"),(String)c.getOptions().get("instanceUrl"));
          artifactItem.setId(c.getId());
          artifactItem.setDescription(c.getDescription());
          ArtifactUtil.normalize(artifactItem);
          artifactItem.setEnabled(true);
          artifactItem.setPushed(c.isPushed());
          artifactItem.setCollectorId(c.getCollectorId());
          artifactItem.setLastUpdated(c.getLastUpdated());
          if(!request.isMetrics()) {
            List<BinaryArtifact> updated = artifactoryClient.getArtifacts(artifactItem, artifactoryClient.getPattern(artifactItem.getRepoName()));
            if (CollectionUtils.isEmpty(updated)) {
              String logger = "repoName=" + artifactItem.getRepoName() + ", artifactName=" + artifactItem.getArtifactName() + ", path=" + artifactItem.getPath() + " has no data";
              collectorItemDataNotFound.add(logger);
            }
          }
          List<BinaryArtifact> existing = binaryArtifactRepository.findByCollectorItemIdAndTimestampIsBetweenOrderByTimestampDesc(c.getId(),startTime, endTime);
          if(!CollectionUtils.isEmpty(existing)){
            actualCollectorItemsDataPresent = actualCollectorItemsDataPresent + 1;
          }
          LOGGER.info("artifact name=" + c.getOptions().get("artifactName") + ", artifactRepo=" + c.getOptions().get("repoName") + ", artifactPath=" + c.getOptions().get("path") + ", isBinaryData=" + CollectionUtils.isEmpty(existing));
          artifactItemRepository.save(artifactItem);
        }
      }
    }

    LOGGER.info("==================== DASHBOARDS NOT PRESENT ==========================");
    dashboardNotPresent.forEach(dashNotPresent -> LOGGER.info(dashNotPresent));

    LOGGER.info("==================== DASHBOARDS NOT CONFIGURED ==========================");
    dashboardNotConfiguredWithArtifactory.forEach(dashNotConfigured -> LOGGER.info(dashNotConfigured));

    LOGGER.info("==================== COLLECTORITEMS NO DATA FOUND ==========================");
    collectorItemDataNotFound.forEach(collectorItem -> LOGGER.info(collectorItem));
    return ResponseEntity
      .status(HttpStatus.OK)
      .body("Total dashboards="+dashboardCount+", total collectorItems="+collectorItemsCount+", actualCollectorItemsDataCount="+actualCollectorItemsDataPresent);
  }

  @RequestMapping(value = "/artifactByVersion", method = POST, consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
  public ResponseEntity<List<BinaryArtifact>> findArtifactByVersion(@RequestBody ArtifactVersionRequest request) throws HygieiaException{
    Collector collector = collectorRepository.findByName("Artifactory");
    List<BinaryArtifact> bas = new ArrayList<>();
    if(Objects.nonNull(collector)){
     List<ArtifactItem> matchedArtifactItems=  artifactItemRepository.findArtifactItemByOptions(collector.getId(),request.getArtifactName(),request.getRepoName(),request.getPath(),request.getInstanceUrl());
      for (ArtifactItem artifactItem: matchedArtifactItems) {
        bas.addAll(artifactoryClient.getArtifactsForVersion(artifactItem,request.getArtifactVersion(), request.getStartTime(),  artifactoryClient.getPattern(artifactItem.getRepoName())));
      }
    }
    return ResponseEntity
            .status(HttpStatus.OK)
            .body(bas);
  }
}
