package com.capitalone.dashboard.collector;


import com.capitalone.dashboard.event.constants.sync.Reason;
import com.capitalone.dashboard.model.ArtifactItem;
import com.capitalone.dashboard.model.ArtifactoryCollector;
import com.capitalone.dashboard.model.ArtifactoryRepo;
import com.capitalone.dashboard.model.BaseArtifact;
import com.capitalone.dashboard.model.BinaryArtifact;
import com.capitalone.dashboard.model.Build;
import com.capitalone.dashboard.model.Collector;
import com.capitalone.dashboard.model.CollectorItem;
import com.capitalone.dashboard.model.GenericCollectorItem;
import com.capitalone.dashboard.model.RepoAndPattern;
import com.capitalone.dashboard.model.relation.RelatedCollectorItem;
import com.capitalone.dashboard.repository.ArtifactItemRepository;
import com.capitalone.dashboard.repository.ArtifactoryCollectorRepository;
import com.capitalone.dashboard.repository.ArtifactoryRepoRepository;
import com.capitalone.dashboard.repository.BaseCollectorRepository;
import com.capitalone.dashboard.repository.BinaryArtifactRepository;
import com.capitalone.dashboard.repository.BuildRepository;
import com.capitalone.dashboard.repository.CollectorItemRepository;
import com.capitalone.dashboard.repository.GenericCollectorItemRepository;
import com.capitalone.dashboard.repository.RelatedCollectorItemRepository;
import com.google.common.collect.Iterables;
import org.apache.commons.collections.map.HashedMap;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class ArtifactoryCollectorTask extends CollectorTaskWithGenericItem<ArtifactoryCollector> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArtifactoryCollectorTask.class);
    private static final int ARTIFACT_GROUP = 1;
    private static final int ARTIFACT_NAME = 2;
    public static final String SLASH = "/";
    private final ArtifactoryCollectorRepository artifactoryCollectorRepository;
    private final ArtifactoryRepoRepository artifactoryRepoRepository;
    private final ArtifactItemRepository artifactItemRepository;
    private final ArtifactoryClient artifactoryClient;
    private final ArtifactorySettings artifactorySettings;
    private final BinaryArtifactRepository binaryArtifactRepository;
    private final CollectorItemRepository collectorItemRepository;
    private final GenericCollectorItemRepository genericCollectorItemRepository;
    private final RelatedCollectorItemRepository relatedCollectorItemRepository;
    private final BuildRepository buildRepository;
    private AtomicInteger count = new AtomicInteger(0);

    @SuppressWarnings("PMD.ExcessiveParameterList")
    @Autowired
    public ArtifactoryCollectorTask(TaskScheduler taskScheduler,
                                    ArtifactoryCollectorRepository artifactoryCollectorRepository,
                                    ArtifactoryRepoRepository artifactoryRepoRepository,
                                    ArtifactItemRepository artifactItemRepository, BinaryArtifactRepository binaryArtifactRepository,
                                    ArtifactoryClient artifactoryClient,
                                    ArtifactorySettings artifactorySettings,
                                    CollectorItemRepository collectorItemRepository,
                                    GenericCollectorItemRepository genericCollectorItemRepository,
                                    RelatedCollectorItemRepository relatedCollectorItemRepository,
                                    BuildRepository buildRepository
    ) {
        super(taskScheduler, "Artifactory", collectorItemRepository, genericCollectorItemRepository, relatedCollectorItemRepository);
        this.artifactoryCollectorRepository = artifactoryCollectorRepository;
        this.artifactoryRepoRepository = artifactoryRepoRepository;
        this.artifactItemRepository = artifactItemRepository;
        this.binaryArtifactRepository = binaryArtifactRepository;
        this.artifactoryClient = artifactoryClient;
        this.artifactorySettings = artifactorySettings;
        this.collectorItemRepository = collectorItemRepository;
        this.genericCollectorItemRepository = genericCollectorItemRepository;
        this.relatedCollectorItemRepository = relatedCollectorItemRepository;
        this.buildRepository = buildRepository;
    }

    @Override
    public ArtifactoryCollector getCollector() {
        return ArtifactoryCollector.prototype(artifactorySettings);
    }

    @Override
    public BaseCollectorRepository<ArtifactoryCollector> getCollectorRepository() {
        return artifactoryCollectorRepository;
    }

    @Override
    public String getCron() {
        return artifactorySettings.getCron();
    }

    @Override
    public void collect(ArtifactoryCollector collector) {
        this.count.set(0);
        LOGGER.info("COLLECTION MODE= " + artifactorySettings.getMode());
        switch (artifactorySettings.getMode()) {
            case REPO_BASED:
                collectRepoBased(collector);
                break;
            case ARTIFACT_BASED:
                collectArtifactBased(collector);
                break;
            case HYBRID_MODE:
                collectHybridMode(collector);
                break;
            default:
                LOGGER.error("Error with collection mode. Valid modes are REPO_BASED, ARTIFACT_BASED, or HYBRID_MODE to be set as properties.");
                break;
        }

    }

    protected void collectRepoBased(ArtifactoryCollector collector) {
        Set<ObjectId> udId = new HashSet<>();
        udId.add(collector.getId());
        List<ArtifactoryRepo> existingRepos = artifactoryRepoRepository.findByCollectorIdIn(udId);
        List<ArtifactoryRepo> activeRepos = new ArrayList<>();
        clean(collector, existingRepos);
        List<String> instanceUrls = collector.getArtifactoryServers();
        instanceUrls.forEach(instanceUrl -> {
            long start = System.currentTimeMillis();
            logBanner(instanceUrl);
            if (instanceUrl.lastIndexOf('/') == instanceUrl.length() - 1) {
                List<ArtifactoryRepo> repos = artifactoryClient.getRepos(instanceUrl);
                log("Fetched repos", start);
                activeRepos.addAll(repos);
                addNewRepos(repos, existingRepos, collector);
                addNewArtifacts(enabledRepos(collector, instanceUrl));
            } else {
                LOGGER.error("Error with artifactory url: " + instanceUrl + ". Url does not end with '/'");
            }
            log("Finished", start);
        });
    }


    protected void collectArtifactBased(ArtifactoryCollector collector) {
        Set<ObjectId> udId = new HashSet<>();
        udId.add(collector.getId());
        processGenericItems(collector);
        // check whether to only collect enabled items or all
        Set<ArtifactItem> existingItemsSet = artifactItemRepository.findByCollectorIdInSet(collector.getId());
        List<String> instanceUrls = collector.getArtifactoryServers();
        long start = System.currentTimeMillis();
        instanceUrls.forEach(instanceUrl -> {
            logBanner(instanceUrl);
            if (instanceUrl.lastIndexOf('/') == instanceUrl.length() - 1) {
                long lastUpdated = getLastUpdated(collector);
                getRepos().forEach(repo -> {
                    //Multiple patterns for the repo will be supported in future
                    String pattern = (getPatterns().get(repo)).get(0);
                    log("Collecting repository ====>>> " + repo);
                    List<BaseArtifact> baseArtifacts = artifactoryClient.getArtifactItems(instanceUrl, repo, pattern, lastUpdated);
                    addNewArtifactsItems(baseArtifacts, existingItemsSet, collector);
                });
                log("Fetched repos", start, getRepos().size());
            } else {
                LOGGER.error("Error with artifactory url: " + instanceUrl + ". Url does not end with '/'");
            }
        });
        log("Finished", start);
        collector.setLastExecuted(start);
        artifactoryCollectorRepository.save(collector);
    }

    protected  void collectHybridMode(ArtifactoryCollector collector){
        long start = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger();
        Map<String, List<String>> subRepoMap = getSubRepos();
        if (Objects.isNull(collector)) return;
        String instanceUrl = artifactorySettings.getServers().get(0).getUrl();
        List<ArtifactItem> enabledArtifactItems = artifactItemRepository.findEnabledArtifactItems(collector.getId());
        LOGGER.info("Total enabled artifactItems=" + enabledArtifactItems.size());
        getRepos().forEach(repo -> {
            int counter = 0;
            Map<ArtifactItem,List<BinaryArtifact>> processing = artifactoryClient.getLatestBinaryArtifacts(collector,getPattern(repo),instanceUrl,repo);
            for (ArtifactItem artifactItem: enabledArtifactItems) {
                artifactoryClient.normalize(artifactItem);
                String rootRepoName = replaceSubRepos(artifactItem.getRepoName(),subRepoMap);
                if(Objects.nonNull(rootRepoName)){
                    artifactItem.setRepoName(rootRepoName);
                }
                if(processing.keySet().contains(artifactItem)){
                    LOGGER.info("processing artifact=" + artifactItem.getArtifactName()+", repo="+artifactItem.getRepoName());
                    List<BinaryArtifact> binaryArtifacts = processing.get(artifactItem);
                    for (BinaryArtifact newBinaryArtifact: binaryArtifacts) {
                        newBinaryArtifact.setCollectorItemId(artifactItem.getId());
                        BinaryArtifact existingBinaryArtifact = binaryArtifactRepository.findTopByCollectorItemIdAndArtifactVersionOrderByTimestampDesc(artifactItem.getId(),
                                newBinaryArtifact.getArtifactVersion());
                        if (Objects.nonNull(existingBinaryArtifact)) {
                            // update existing binary artifact for that version and update timestamp
                            updateExistingBinaryArtifact(newBinaryArtifact, existingBinaryArtifact);
                            binaryArtifactRepository.save(newBinaryArtifact);
                        } else {
                            // get latest binary artifact for this artifact item with build info
                            attachLatestBuildInfo(artifactItem, newBinaryArtifact);
                            // save immediately to avoid creating multiple new BAs for same collectorItemId and artifactVersion
                            binaryArtifactRepository.save(newBinaryArtifact);
                        }
                    }
                    artifactItem.setLastUpdated(System.currentTimeMillis());
                    artifactItemRepository.save(artifactItem);
                    count.getAndIncrement();
                    counter++;
                }
            }
            LOGGER.info("updated artifacts for repo=" + repo+", updatedCount="+counter);
        });
        long end = System.currentTimeMillis();
        long elapsedTime = (end-start) / 1000;
        LOGGER.info(String.format("ArtifactoryCollectorTask:collect stop, totalProcessSeconds=%d,  totalEnabledArtifacts=%d, totalUpdatedArtifacts=%d",
                elapsedTime, enabledArtifactItems.size(), count.get()));
        collector.setLastExecuted(start);
        collector.setLastExecutedSeconds(elapsedTime);
        collector.setLastExecutionRecordCount(enabledArtifactItems.size());
        artifactoryCollectorRepository.save(collector);
    }

    private String replaceSubRepos(String repoName,Map<String,List<String>> subRepoMap){
        if(subRepoMap.containsKey(repoName)) return repoName;
         Map.Entry found = subRepoMap.entrySet().stream().filter(entry-> !CollectionUtils.isEmpty(entry.getValue()) && entry.getValue().contains(repoName)).filter(Objects::nonNull).findFirst().orElse(null);
         if(Objects.nonNull(found)) return (String) found.getKey();
         return null;
    }



    private void updateExistingBinaryArtifact(BinaryArtifact newBinaryArtifact, BinaryArtifact existingBinaryArtifact) {
        // update all fields except build infos
        if(!org.apache.commons.collections.CollectionUtils.isEmpty(existingBinaryArtifact.getBuildInfos())){
            newBinaryArtifact.setBuildInfos(existingBinaryArtifact.getBuildInfos());
        }
    }


    private void attachLatestBuildInfo(ArtifactItem artifactItem, BinaryArtifact binaryArtifact) {
        // get latest binary artifact associated with the artifact item by desc timestamp
        BinaryArtifact latestWithBuildInfo = binaryArtifactRepository.findTopByCollectorItemIdAndBuildInfosIsNotEmptyOrderByTimestampDesc(artifactItem.getId(), new Sort(Sort.Direction.DESC, "timestamp"));
        if (Objects.isNull(latestWithBuildInfo)) return;
        binaryArtifact.setBuildInfos(latestWithBuildInfo.getBuildInfos());
    }
    private List<String> getPattern(String repoName){
        if(Objects.isNull(repoName)) return null;
        List<String> pattern =  getRepoAndSubRepoPatterns().entrySet().stream().filter(entry -> repoName.contains(entry.getKey())).map(entry -> entry.getValue()).findFirst().orElse(null);
        if (CollectionUtils.isEmpty(pattern)) return null;
        return pattern;
    }

    private List<String> getAssociatedSubRepos(String repoName){
        if(Objects.isNull(repoName)) return null;
        List<String> subRepos =  getSubRepos().entrySet().stream().filter(entry -> repoName.contains(entry.getKey())).map(entry -> entry.getValue()).findFirst().orElse(null);
        return subRepos;
    }

    private void refreshData(Map<ObjectId, Set<ObjectId>> artifactBuilds) {
        artifactBuilds.forEach((artCollectorItemId, buildIdSet) -> {
            Iterable<BinaryArtifact> binaryArtifacts = binaryArtifactRepository.findByCollectorItemId(artCollectorItemId);
            List<Build> associatedBuilds = new ArrayList<>();
            buildIdSet.forEach(buildId -> {
                associatedBuilds.add(buildRepository.findOne(buildId));
            });
            binaryArtifacts.forEach(binaryArtifact -> {
                if (associatedBuilds != null) {
                    binaryArtifact.addBuild(associatedBuilds.get(0));
                    binaryArtifactRepository.save(binaryArtifacts);
                }
            });


        });
    }

    /**
     * Clean up unused artifactory collector items
     *
     * @param collector the {@link ArtifactoryCollector}
     */
    private void clean(ArtifactoryCollector collector, List<ArtifactoryRepo> existingRepos) {
        // find the server url's to collect from
        Set<String> serversToBeCollected = new HashSet<>();
        serversToBeCollected.addAll(collector.getArtifactoryServers());

        // find the repos to collect from each server url above
        List<Set<String>> repoNamesToBeCollected = new ArrayList<Set<String>>();
        List<String[]> allRepos = new ArrayList<>();
        artifactorySettings.getServers().forEach(serverSetting -> {
            allRepos.add((String[]) getRepoAndPatternsForServ(serverSetting.getRepoAndPatterns()).keySet().toArray());
        });
        for (int i = 0; i < allRepos.size(); i++) {
            Set<String> reposSet = new HashSet<>();
            if (allRepos.get(i) != null) {
                reposSet.addAll(Arrays.asList(allRepos.get(i)));
            }
            repoNamesToBeCollected.add(reposSet);
        }

        assert (serversToBeCollected.size() == repoNamesToBeCollected.size());

        List<ArtifactoryRepo> stateChangeRepoList = new ArrayList<>();
        for (ArtifactoryRepo repo : existingRepos) {
            if (isRepoEnabledAndNotCollected(collector, serversToBeCollected, repoNamesToBeCollected, repo) ||  // if it was enabled but not to be collected
                    isRepoDisabledAndToBeCollected(collector, serversToBeCollected, repoNamesToBeCollected, repo)) { // OR it was disabled and now is to be collected
                repo.setEnabled(isRepoCollected(collector, serversToBeCollected, repoNamesToBeCollected, repo));
                stateChangeRepoList.add(repo);
            }
        }
        if (!CollectionUtils.isEmpty(stateChangeRepoList)) {
            artifactoryRepoRepository.save(stateChangeRepoList);
        }
    }

    private boolean isRepoCollected(ArtifactoryCollector collector, Set<String> serversToBeCollected, List<Set<String>> repoNamesToBeCollected, ArtifactoryRepo repo) {
        return collector.getId().equals(repo.getCollectorId())
                && serversToBeCollected.contains(repo.getInstanceUrl())
                && repoNamesToBeCollected.get(collector.getArtifactoryServers().indexOf(repo.getInstanceUrl())).contains(repo.getRepoName());
    }

    private boolean isRepoDisabledAndToBeCollected(ArtifactoryCollector collector, Set<String> serversToBeCollected, List<Set<String>> repoNamesToBeCollected, ArtifactoryRepo repo) {
        return !repo.isEnabled() && (isRepoCollected(collector, serversToBeCollected, repoNamesToBeCollected, repo));
    }

    private boolean isRepoEnabledAndNotCollected(ArtifactoryCollector collector, Set<String> serversToBeCollected, List<Set<String>> repoNamesToBeCollected, ArtifactoryRepo repo) {
        return repo.isEnabled() && (!collector.getId().equals(repo.getCollectorId())
                || !serversToBeCollected.contains(repo.getInstanceUrl())
                || !repoNamesToBeCollected.get(collector.getArtifactoryServers().indexOf(repo.getInstanceUrl())).contains(repo.getRepoName()));
    }


    /**
     * Add any new {@link ArtifactoryRepo}s.
     *
     * @param repos         list of {@link ArtifactoryRepo}s
     * @param existingRepos list of existing {@link ArtifactoryRepo}s
     * @param collector     the {@link ArtifactoryCollector}
     */
    private void addNewRepos(List<ArtifactoryRepo> repos, List<ArtifactoryRepo> existingRepos, ArtifactoryCollector collector) {
        long start = System.currentTimeMillis();
        int count = 0;

        List<ArtifactoryRepo> newRepos = new ArrayList<>();
        for (ArtifactoryRepo repo : repos) {
            ArtifactoryRepo existing = null;
            if (!CollectionUtils.isEmpty(existingRepos) && (existingRepos.contains(repo))) {
                existing = existingRepos.get(existingRepos.indexOf(repo));
            }

            if (existing == null) {
                repo.setCollectorId(collector.getId());
                repo.setEnabled(false); // Do not enable for collection. Will be enabled later
                repo.setDescription(repo.getRepoName());
                newRepos.add(repo);
                count++;
            }
        }
        //save all in one shot
        if (!CollectionUtils.isEmpty(newRepos)) {
            artifactoryRepoRepository.save(newRepos);
        }
        log("New repos", start, count);
    }

    /**
     * Add any new {@link BinaryArtifact}s
     *
     * @param enabledRepos list of enabled {@link ArtifactoryRepo}s
     */
    private void addNewArtifacts(List<ArtifactoryRepo> enabledRepos) {
        long start = System.currentTimeMillis();

        for (ArtifactoryRepo repo : enabledRepos) {
            for (BinaryArtifact artifact : nullSafe(artifactoryClient.getArtifacts(repo.getInstanceUrl(), repo.getRepoName(), repo.getLastUpdated()))) {
                if (artifact != null && isNewArtifact(repo, artifact)) {
                    artifact.setCollectorItemId(repo.getId());
                    binaryArtifactRepository.save(artifact);
                    count.getAndIncrement();
                }
            }
        }

        // Iterate through list of repos and update the lastUpdated timestamp
        for (ArtifactoryRepo repo : enabledRepos) {
            repo.setLastUpdated(start);
        }
        // We set the last update time so need to save it
        artifactoryRepoRepository.save(enabledRepos);
        log("New artifacts", start, count.get());
    }


    /**
     * Add any new {@link ArtifactItem}s
     */

    private void addNewArtifactsItems(List<BaseArtifact> baseArtifacts, Set<ArtifactItem> existingArtifactItems, ArtifactoryCollector collector) {
        long start = System.currentTimeMillis();
        List<BinaryArtifact> binaryArtifacts = new ArrayList<>();
        for (BaseArtifact baseArtifact : baseArtifacts) {
            ArtifactItem newArtifactItem = baseArtifact.getArtifactItem();
            if (newArtifactItem != null && !existingArtifactItems.contains(newArtifactItem)) {
                // changed to 'start' instead of System.currentTimeMillis()
                newArtifactItem.setLastUpdated(start);
                newArtifactItem.setCollectorId(collector.getId());
                newArtifactItem = artifactItemRepository.save(newArtifactItem);
                existingArtifactItems.add(newArtifactItem);
                count.getAndIncrement();
            }
            List<BinaryArtifact> binaryArtifactsAssociated = baseArtifact.getBinaryArtifacts();
            if (!CollectionUtils.isEmpty(binaryArtifactsAssociated) ) {
                for (BinaryArtifact b:binaryArtifactsAssociated) {
                    ArtifactItem found = existingArtifactItems.stream().filter(newArtifactItem::equals).findAny().orElse(null);
                    ObjectId collectorItemId = newArtifactItem.getId()!=null? newArtifactItem.getId(): getExistingArtifactIdAndSave(found);
                    b.setCollectorItemId(collectorItemId);
                    binaryArtifacts.add(b);
                }

            }

        }
        if (!binaryArtifacts.isEmpty()) {
            LOGGER.info("Saving " + binaryArtifacts.size() + " binary artifacts");
            binaryArtifacts.forEach(binaryArtifact -> binaryArtifactRepository.save(binaryArtifact));
        }
        log("New artifacts items", start, count.get());
    }

    private ObjectId getExistingArtifactIdAndSave(ArtifactItem found) {
        if(Objects.nonNull(found)){
            found.setLastUpdated(System.currentTimeMillis());
            artifactItemRepository.save(found);
            return found.getId();
        }
        return null;
    }

    private List<BinaryArtifact> nullSafe(List<BinaryArtifact> builds) {
        return builds == null ? new ArrayList<BinaryArtifact>() : builds;
    }


    private List<ArtifactoryRepo> enabledRepos(ArtifactoryCollector collector, String instanceUrl) {
        return artifactoryRepoRepository.findEnabledArtifactoryRepos(collector.getId(), instanceUrl);
    }

    private boolean isNewArtifact(ArtifactoryRepo repo, BinaryArtifact artifact) {
        return Iterables.size(binaryArtifactRepository.findByAttributes(repo.getId(),
                artifact.getArtifactGroupId(), artifact.getArtifactModule(), artifact.getArtifactVersion(), artifact.getArtifactName(),
                artifact.getArtifactClassifier(), artifact.getArtifactExtension())) == 0;
    }

    private List<String> getRepos() {
        List<String> repos = new ArrayList<>();
        artifactorySettings.getServers().forEach(serverSetting -> {
            repos.addAll(getRepoAndPatternsForServ(serverSetting.getRepoAndPatterns()).keySet());
        });
        return repos;
    }

    private Map<String, List<String>> getPatterns() {
        Map<String, List<String>> patterns = new HashedMap();
        artifactorySettings.getServers().forEach(serverSetting -> {
            patterns.putAll(getRepoAndPatternsForServ(serverSetting.getRepoAndPatterns()));
        });
        return patterns;
    }

    private Map<String, List<String>> getRepoAndSubRepoPatterns() {
        Map<String, List<String>> patterns = new HashedMap();
        artifactorySettings.getServers().forEach(serverSetting -> {
            patterns.putAll(getRepoAndPatternsForServ(serverSetting.getRepoAndPatterns()));
            patterns.putAll(getSubRepoPatternsForServ(serverSetting.getRepoAndPatterns()));
        });
        return patterns;
    }

    private static Map<String, List<String>> getRepoAndPatternsForServ(List<RepoAndPattern> repoAndPatterns) {
        return repoAndPatterns.stream().collect(Collectors.toMap(RepoAndPattern::getRepo, RepoAndPattern::getPatterns));
    }

    private static Map<String, List<String>> getSubRepoPatternsForServ(List<RepoAndPattern> repoAndPatterns) {
        Map<String, List<String>> subRepoToPattern = new HashMap<>();
        Map<List<String>, List<String>> subReposListToPatterns = repoAndPatterns.stream()
                .filter(repoAndPattern -> !CollectionUtils.isEmpty(repoAndPattern.getSubRepos()))
                .collect(Collectors.toMap(RepoAndPattern::getSubRepos, RepoAndPattern::getPatterns));
        subReposListToPatterns.forEach((subRepos, patterns) -> subRepos.forEach(subRepo -> subRepoToPattern.put(subRepo, patterns)));
        return subRepoToPattern;
    }

    private Map<String, List<String>> getSubRepos() {
        Map<String, List<String>> allSubRepos = new HashedMap();
        artifactorySettings.getServers().forEach(serverSetting -> {
            allSubRepos.putAll(getSubReposForServ(serverSetting.getRepoAndPatterns()));
        });
        return allSubRepos;

    }

    private static Map<String, List<String>> getSubReposForServ(List<RepoAndPattern> repoAndPatterns) {
        return repoAndPatterns.stream()
                .filter(repoAndPattern -> !CollectionUtils.isEmpty(repoAndPattern.getSubRepos()))
                .collect(Collectors.toMap(RepoAndPattern::getRepo, RepoAndPattern::getSubRepos));
    }


    private long getLastUpdated(Collector collector) {
        if(!Objects.isNull(collector.getLastExecuted())) {
            return collector.getLastExecuted();
        }else{
            return System.currentTimeMillis() - artifactorySettings.getOffSet();
        }
    }

    protected Map<ObjectId, Set<ObjectId>> processGenericItems(ArtifactoryCollector collector) {
        List<GenericCollectorItem> genericCollectorItems = genericCollectorItemRepository.findAllByToolNameAndProcessTimeEquals(collector.getName(), 0L);
        Map<ObjectId, Set<ObjectId>> artifactBuilds = new HashMap<>();
        genericCollectorItems.forEach(gci -> {
            String capture = capturePattern(gci,ARTIFACT_NAME).trim();
            String captureGroupId = capturePattern(gci,ARTIFACT_GROUP).trim();
            String capturePath = captureGroupId+ SLASH +capture;
            if (StringUtils.isEmpty(capture)) {
                return;
            }
            List<CollectorItem> artifactCollectorItems = collectorItemRepository.findByArtifactNameAndPath(capture,capturePath);
            artifactCollectorItems.forEach(
                    item -> {
                        if (!artifactBuilds.containsKey(item.getId()) || CollectionUtils.isEmpty(artifactBuilds.get(item))) {
                            artifactBuilds.put(item.getId(), new HashSet<>());
                        }
                        //Save as related item. Related Item event listener will process it.
                        artifactBuilds.get(item.getId()).add(gci.getBuildId());
                        RelatedCollectorItem relatedCollectorItem = new RelatedCollectorItem();
                        relatedCollectorItem.setCreationTime(System.currentTimeMillis());
                        relatedCollectorItem.setLeft(gci.getRelatedCollectorItem());
                        relatedCollectorItem.setRight(item.getId());
                        relatedCollectorItem.setSource(this.getClass().toString());
                        relatedCollectorItem.setReason(Reason.ARTIFACT_REASON.getAction());
                        relatedCollectorItemRepository.save(relatedCollectorItem);
                        //set collectorItem as pushed true
                        item.setPushed(true);
                        collectorItemRepository.save(item);
                    }
            );

            // Save generic item as processed, ie, processing time non zero.
            gci.setProcessTime(System.currentTimeMillis());
            genericCollectorItemRepository.save(gci);
        });
        return artifactBuilds;
    }

    protected String capturePattern(GenericCollectorItem gci,int group) {
        List<String> regex = Arrays.asList(artifactorySettings.getCapturePattern());
        return regex
                .stream().map(Pattern::compile)
                .map(p -> p.matcher(gci.getRawData()))
                .filter(Matcher::find)
                .findFirst()
                .map(match -> match.group(group))
                .orElse("");
    }


    @Override
    public Map<String, Object> getGenericCollectorItemOptions(String serverUrl, GenericCollectorItem genericCollectorItem) {
        return null;
    }

}
