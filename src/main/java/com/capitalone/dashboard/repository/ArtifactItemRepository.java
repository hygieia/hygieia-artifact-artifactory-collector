package com.capitalone.dashboard.repository;

import com.capitalone.dashboard.model.ArtifactItem;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.Query;

import java.util.List;
import java.util.Set;


public interface ArtifactItemRepository extends BaseCollectorItemRepository<ArtifactItem>{

    @Query(value="{ 'collectorId' : ?0 }")
    Set<ArtifactItem> findByCollectorIdInSet(ObjectId collectorId);

    @Query(value="{ 'collectorId' : ?0, options.artifactName : ?1, options.repoName : ?2, options.path : ?3, options.instanceUrl : ?4}")
    List<ArtifactItem> findArtifactItemByOptions(ObjectId collectorId, String artifactName, String repoName, String path, String instanceUrl);

    @Query(value="{ 'collectorId' : ?0, enabled: true}")
    List<ArtifactItem> findEnabledArtifactItems(ObjectId collectorId);

}