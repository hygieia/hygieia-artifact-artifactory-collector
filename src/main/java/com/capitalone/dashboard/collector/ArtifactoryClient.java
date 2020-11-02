package com.capitalone.dashboard.collector;

import java.util.List;
import java.util.Map;

import com.capitalone.dashboard.model.ArtifactItem;
import com.capitalone.dashboard.model.ArtifactoryRepo;
import com.capitalone.dashboard.model.BaseArtifact;
import com.capitalone.dashboard.model.BinaryArtifact;
import com.capitalone.dashboard.model.Collector;

/**
 * Client for fetching artifacts information from Artifactory
 */
public interface ArtifactoryClient {

    /**
     * Obtain list of repos in the given artifactory
     *
     * @param instanceUrl         server url
     * @return
     */
    List<ArtifactoryRepo> getRepos(String instanceUrl);

    /**
     * Obtain all the artifacts in the given artifactory repo
     *
     * @param instanceUrl server url
     * @param repoName    repo name
     * @param lastUpdated timestamp when the repo was last updated
     * @return
     */
    List<BinaryArtifact> getArtifacts(String instanceUrl, String repoName, long lastUpdated);

    List<BaseArtifact> getArtifactItems(String instanceUrl, String repoName,String pattern, long lastUpdated);

    List<BinaryArtifact> getArtifacts(ArtifactItem artifactItem, List<String> pattern);

    Map<ArtifactItem,List<BinaryArtifact>> getLatestBinaryArtifacts(Collector collector, List<String> patterns, String instanceUrl, String repo);

    List<String> getPattern(String repoName);

    ArtifactItem normalize(ArtifactItem artifactItem);

}