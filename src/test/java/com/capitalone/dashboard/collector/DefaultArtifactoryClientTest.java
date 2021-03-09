package com.capitalone.dashboard.collector;

import com.capitalone.dashboard.client.RestClient;
import com.capitalone.dashboard.client.RestOperationsSupplier;
import com.capitalone.dashboard.model.ArtifactItem;
import com.capitalone.dashboard.model.ArtifactoryRepo;
import com.capitalone.dashboard.model.BaseArtifact;
import com.capitalone.dashboard.model.BinaryArtifact;
import com.capitalone.dashboard.model.Build;
import com.capitalone.dashboard.model.RepoAndPattern;
import com.capitalone.dashboard.model.ServerSetting;
import com.capitalone.dashboard.repository.BinaryArtifactRepository;
import com.capitalone.dashboard.util.ArtifactUtilTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultArtifactoryClientTest {

	@Mock private RestOperationsSupplier restOperationsSupplier;
    @Mock private RestOperations rest;
    @Mock private ArtifactorySettings settings;
    @Mock private BinaryArtifactRepository binaryArtifactRepository;
    
    private final DateFormat FULL_DATE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    
    private DefaultArtifactoryClient defaultArtifactoryClient;
    
    @Before
    public void init() {
		settings = new ArtifactorySettings();
    	when(restOperationsSupplier.get()).thenReturn(rest);
        ServerSetting serverSetting = new ServerSetting();
		serverSetting.setUrl("http://localhost:8081/artifactory");
		RepoAndPattern r = new RepoAndPattern();
		r.setPatterns(Arrays.asList(ArtifactUtilTest.IVY_PATTERN1, ArtifactUtilTest.IVY_ARTIFACT_PATTERN1, ArtifactUtilTest.MAVEN_PATTERN1,ArtifactUtilTest.ARTIFACT_PATTERN));
		r.setSubRepos(Arrays.asList("sub-repo-1", "sub-repo-2"));
		serverSetting.setRepoAndPatterns(Collections.singletonList(r));
        settings.setServers(Collections.singletonList(serverSetting));
        settings.setTimeInterval(3);
        settings.setTimeUnit("DAYS");
        defaultArtifactoryClient = new DefaultArtifactoryClient(settings, new RestClient(restOperationsSupplier),binaryArtifactRepository);
    }
    
    @Test
    public void testGetRepos() throws Exception {
    	String reposJson = getJson("repos.json");
    	
    	String instanceUrl = "http://localhost:8081/artifactory/";
    	String reposListUrl = "http://localhost:8081/artifactory/api/repositories";
    	
    	when(rest.exchange(eq(reposListUrl), eq(HttpMethod.GET), Matchers.any(HttpEntity.class), eq(String.class)))
    		.thenReturn(new ResponseEntity<>(reposJson, HttpStatus.OK));
    	List<ArtifactoryRepo> repos = defaultArtifactoryClient.getRepos(instanceUrl);
    	assertThat(repos.size(), is(2));
        assertThat(repos.get(0).getRepoName(), is("release"));
        assertThat(repos.get(0).getRepoUrl(), is("http://localhost:8081/artifactory/release"));
        assertThat(repos.get(1).getRepoName(), is("xldeploy"));
        assertThat(repos.get(1).getRepoUrl(), is("http://localhost:8081/artifactory/xldeploy"));
    }
    
    @Test
    public void testGetEmptyArtifacts() throws Exception {
    	String emptyArtifactsJson = getJson("emptyArtifacts.json");
    	
    	String instanceUrl = "http://localhost:8081/artifactory/";
    	String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
    	String repoName = "release";
    	
    	when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
    		.thenReturn(new ResponseEntity<>(emptyArtifactsJson, HttpStatus.OK));
    	List<BinaryArtifact> artifacts = defaultArtifactoryClient.getArtifacts(instanceUrl, repoName, 0);
    	assertThat(artifacts.size(), is(0));
    }


	@Test
	public void testGetArtifactItems() throws Exception {
		String instanceUrl = "http://localhost:8081/artifactory/";
		String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
		String repoName = "release";

		long lastUpdated = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2) - TimeUnit.HOURS.toMillis(1);
		long currTime = lastUpdated + TimeUnit.HOURS.toMillis(1);

		// with the addition of artifactory pagination, update the times to limit number of calls made in getArtifactItems()
		JSONObject updatedArtifactItems = updateJsonArtifactTimes("artifactItems.json", currTime);
		// get latest timestamp
		List<Map<String,String>> res = ((List) updatedArtifactItems.get("results"));
		long lastTime = (FULL_DATE.parse((res.get(res.size()-1)).get("created")).getTime());
		// mock query json in response
		String artifactItemsJson1 = queryJsonByTime(updatedArtifactItems,
				lastUpdated,
				lastUpdated + TimeUnit.DAYS.toMillis(1));
		String artifactItemsJson2 = queryJsonByTime(updatedArtifactItems,
				lastUpdated + TimeUnit.DAYS.toMillis(1),
				lastUpdated + TimeUnit.DAYS.toMillis(2));
		// additional call from slight offset in milliseconds
		String artifactItemsJson3 = queryJsonByTime(updatedArtifactItems,
				lastUpdated + TimeUnit.DAYS.toMillis(2),
				System.currentTimeMillis());

		when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>(artifactItemsJson1, HttpStatus.OK))
				.thenReturn(new ResponseEntity<>(artifactItemsJson2, HttpStatus.OK))
				.thenReturn(new ResponseEntity<>(artifactItemsJson3, HttpStatus.OK));

		when(binaryArtifactRepository.findByArtifactNameAndArtifactVersion("test-dev","1")).thenReturn(null);
		when(binaryArtifactRepository.findByArtifactNameAndArtifactVersion("test-dev","1")).thenReturn(binaryArtifactIterable(true));
		List<BaseArtifact> baseArtifacts = defaultArtifactoryClient.getArtifactItems(instanceUrl, repoName, ArtifactUtilTest.ARTIFACT_PATTERN,lastUpdated);
		assertThat(baseArtifacts.size(), is(1));
		assertThat(baseArtifacts.get(0).getArtifactItem().getArtifactName(),is("test-dev"));
		assertThat(baseArtifacts.get(0).getArtifactItem().getInstanceUrl(),is("http://localhost:8081/artifactory/"));
		assertThat(baseArtifacts.get(0).getArtifactItem().getRepoName(),is("repoName"));
		assertThat(baseArtifacts.get(0).getArtifactItem().getPath(),is("dummy/test-dev"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getCanonicalName(),is("manifest.json"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getArtifactGroupId(),is("dummy"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getActual_md5(),is("111aadc11ed11b1111df111d16d6c8d821112f3"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getActual_sha1(),is("111aadc11ed11b1111df111d16d6c8d821112f3"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getArtifactExtension(),is("json"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getArtifactName(),is("test-dev"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getType(),is("file"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getModifiedBy(),is("robot"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getModifiedTimeStamp(),is(new Long("1539268736471")));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getCreatedBy(),is("robot"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getCreatedTimeStamp(),is(lastTime));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getArtifactVersion(),is("1"));

	}

	@Test
	public void testGetArtifactItemsWithBuildInfo() throws Exception {
		String instanceUrl = "http://localhost:8081/artifactory/";
		String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
		String repoName = "release";

		long lastUpdated = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2) - TimeUnit.HOURS.toMillis(1);
		long currTime = lastUpdated + TimeUnit.HOURS.toMillis(1);

		// with the addition of artifactory pagination, update the times to limit number of calls made in getArtifactItems()
		JSONObject updatedArtifactItems = updateJsonArtifactTimes("artifactItems.json", currTime);
		// get latest timestamp
		List<Map<String,String>> res = ((List) updatedArtifactItems.get("results"));
		long lastTime = (FULL_DATE.parse((res.get(res.size()-1)).get("created")).getTime());
		// mock query json in response
		String artifactItemsJson1 = queryJsonByTime(updatedArtifactItems,
				lastUpdated,
				lastUpdated + TimeUnit.DAYS.toMillis(1));
		String artifactItemsJson2 = queryJsonByTime(updatedArtifactItems,
				lastUpdated + TimeUnit.DAYS.toMillis(1),
				lastUpdated + TimeUnit.DAYS.toMillis(2));
		// additional call from slight offset in milliseconds
		String artifactItemsJson3 = queryJsonByTime(updatedArtifactItems,
				lastUpdated + TimeUnit.DAYS.toMillis(2),
				System.currentTimeMillis());

		when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>(artifactItemsJson1, HttpStatus.OK))
				.thenReturn(new ResponseEntity<>(artifactItemsJson2, HttpStatus.OK))
				.thenReturn(new ResponseEntity<>(artifactItemsJson3, HttpStatus.OK));
		when(binaryArtifactRepository.findByArtifactNameAndArtifactVersion("test-dev","1"))
				.thenReturn(binaryArtifactIterable(true));

		List<BaseArtifact> baseArtifacts = defaultArtifactoryClient.getArtifactItems(instanceUrl, repoName, ArtifactUtilTest.ARTIFACT_PATTERN,lastUpdated);
		assertThat(baseArtifacts.size(), is(1));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().size(), is(1));
		assertThat(baseArtifacts.get(0).getArtifactItem().getArtifactName(),is("test-dev"));
		assertThat(baseArtifacts.get(0).getArtifactItem().getInstanceUrl(),is("http://localhost:8081/artifactory/"));
		assertThat(baseArtifacts.get(0).getArtifactItem().getRepoName(),is("repoName"));
		assertThat(baseArtifacts.get(0).getArtifactItem().getPath(),is("dummy/test-dev"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getCanonicalName(),is("manifest.json"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getArtifactGroupId(),is("dummy"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getActual_md5(),is("111aadc11ed11b1111df111d16d6c8d821112f3"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getActual_sha1(),is("111aadc11ed11b1111df111d16d6c8d821112f3"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getArtifactExtension(),is("json"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getArtifactName(),is("test-dev"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getType(),is("file"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getModifiedBy(),is("robot"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getModifiedTimeStamp(),is(new Long("1539268736471")));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getCreatedBy(),is("robot"));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getCreatedTimeStamp(),is(lastTime));
		assertThat(baseArtifacts.get(0).getBinaryArtifacts().get(0).getArtifactVersion(),is("1"));
		assertNotNull(baseArtifacts.get(0).getBinaryArtifacts().get(0).getCollectorItemId());
		assertNotNull(baseArtifacts.get(0).getBinaryArtifacts().get(0).getBuildInfos());

	}

	// TEST new Hybrid Mode
	@Test
	public void testGetArtifactsNoExistingMatchingBinaryArtifactAttributes() throws Exception {
		// [scenario] no existing binary artifact with matching collectorItemId and version found in DB
		String instanceUrl = "http://localhost:8081/artifactory/";
		String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
		String repoName = "release";
		String response = getJson("binaryArtifacts.json");
		ObjectId id = ObjectId.get();
		// artifact item
		ArtifactItem ai = createArtifactItem(id, "test-dev", instanceUrl, repoName);
		List<String> patterns = new ArrayList<>();
		patterns.add(ArtifactUtilTest.ARTIFACT_PATTERN);
		List<String> subRepos = settings.getServers().get(0).getRepoAndPatterns().get(0).getSubRepos();

		when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>(response, HttpStatus.OK));

		when(binaryArtifactRepository.findTopByCollectorItemIdAndArtifactVersionOrderByTimestampDesc(id, "1")).thenReturn(null);
		// binary artifact found with matching collector item id
		when(binaryArtifactRepository.findTopByCollectorItemIdAndBuildInfosIsNotEmptyOrderByTimestampDesc(id, new Sort(Sort.Direction.DESC, "timestamp"))).thenReturn(binaryArtifactLatestCollectorItemId(id, true));
		List<BinaryArtifact> binaryArtifacts = defaultArtifactoryClient.getArtifacts(ai, patterns);
		assertThat(binaryArtifacts.size(), is(0));
	}

	@Test
	public void testGetArtifactsWithExistingMatchingBinaryArtifactAttributes() throws Exception {
		// [scenario] found existing binary artifact with matching artifact name, version, path, and repo found in DB
		String instanceUrl = "http://localhost:8081/artifactory/";
		String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
		String repoName = "release";
		String response = getJson("binaryArtifacts.json");
		ObjectId id = ObjectId.get();
		// artifact item
		ArtifactItem ai = createArtifactItem(id, "test-dev", instanceUrl, repoName);
		List<String> patterns = new ArrayList<>();
		patterns.add(ArtifactUtilTest.ARTIFACT_PATTERN);
		List<String> subRepos = settings.getServers().get(0).getRepoAndPatterns().get(0).getSubRepos();

		when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>(response, HttpStatus.OK));

		BinaryArtifact matchedBA = createMatchedExistingBinaryArtifact(id, "test-dev", "1", "dummy/test-dev/1", repoName, true);
		when(binaryArtifactRepository.findTopByCollectorItemIdAndArtifactVersionOrderByTimestampDesc(id, "1")).thenReturn(matchedBA);
		List<BinaryArtifact> binaryArtifacts = defaultArtifactoryClient.getArtifacts(ai, patterns);
		assertThat(binaryArtifacts.size(), is(1));
		assertThat(binaryArtifacts.get(0).getArtifactName(),is("test-dev"));
		assertThat(binaryArtifacts.get(0).getCanonicalName(),is("manifest.json"));
		assertThat(binaryArtifacts.get(0).getArtifactGroupId(),is("dummy"));
		assertThat(binaryArtifacts.get(0).getActual_md5(),is("111aadc11ed11b1111df111d16d6c8d821112f1"));
		assertThat(binaryArtifacts.get(0).getActual_sha1(),is("111aadc11ed11b1111df111d16d6c8d821112f1"));
		assertThat(binaryArtifacts.get(0).getArtifactExtension(),is("json"));
		assertThat(binaryArtifacts.get(0).getType(),is("file"));
		assertThat(binaryArtifacts.get(0).getModifiedBy(),is("robot"));
		assertThat(binaryArtifacts.get(0).getModifiedTimeStamp(),is(FULL_DATE.parse("2018-10-11T14:38:56.471Z").getTime()));
		assertThat(binaryArtifacts.get(0).getCreatedBy(),is("robot"));
		assertThat(binaryArtifacts.get(0).getCreatedTimeStamp(),is(FULL_DATE.parse("2018-10-11T14:27:16.031Z").getTime()));
		assertThat(binaryArtifacts.get(0).getArtifactVersion(),is("1"));
		assertThat(binaryArtifacts.get(0).getVirtualRepos(), is(Arrays.asList("docker-managed")));
	}

	// test with having to iterate through multiple patterns with no patterns matched
	@Test
	public void testGetArtifactsIterateMultiplePatternsNoMatch() throws Exception {
		String instanceUrl = "http://localhost:8081/artifactory/";
		String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
		String repoName = "release";
		String response = getJson("binaryArtifacts.json");
		ObjectId id = ObjectId.get();
		// artifact item
		ArtifactItem ai = createArtifactItem(id, "test-dev", instanceUrl, repoName);
		List<String> patterns = new ArrayList<>();
		patterns.add(ArtifactUtilTest.MISC_PATTERN1);
		patterns.add(ArtifactUtilTest.MISC_PATTERN2);

		when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>(response, HttpStatus.OK));

		List<BinaryArtifact> binaryArtifacts = defaultArtifactoryClient.getArtifacts(ai, patterns);
		assertThat(binaryArtifacts.size(), is(0));
	}

	// test with version null
	@Test
	public void testGetArtifactsVersionNull() throws Exception {
		String instanceUrl = "http://localhost:8081/artifactory/";
		String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
		String repoName = "release";
		Map<String, String> fieldsToUpdate = new HashMap<>();
		fieldsToUpdate.put("path", "dummy/test-dev");
		String response = updateJsonArtifactFields("binaryArtifacts.json", fieldsToUpdate);
		ObjectId id = ObjectId.get();
		// artifact item
		ArtifactItem ai = createArtifactItem(id, "test-dev", instanceUrl, repoName);
		List<String> patterns = new ArrayList<>();
		patterns.add(ArtifactUtilTest.ARTIFACT_PATTERN);

		when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>(response, HttpStatus.OK));

		List<BinaryArtifact> binaryArtifacts = defaultArtifactoryClient.getArtifacts(ai, patterns);
		assertThat(binaryArtifacts.size(), is(0));
	}

	// test with invalid path returning no artifacts
	@Test
	public void testGetArtifactsInvalidPath() throws Exception {
		String instanceUrl = "http://localhost:8081/artifactory/";
		String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
		String repoName = "sub-repo-1";
		String emptyResponse = getJson("emptyArtifacts.json");
		ObjectId id = ObjectId.get();
		// artifact item
		ArtifactItem ai = createArtifactItem(id, "test-dev", instanceUrl, repoName);
		List<String> patterns = new ArrayList<>();
		patterns.add(ArtifactUtilTest.ARTIFACT_PATTERN);

		// invalid path returns no results
		when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>(emptyResponse, HttpStatus.OK));

		List<BinaryArtifact> binaryArtifacts = defaultArtifactoryClient.getArtifacts(ai, patterns);
		assertThat(binaryArtifacts.size(), is(0));
	}

	@Test
    public void testGetMavenArtifacts() throws Exception {
    	String mavenArtifactsJson = getJson("mavenArtifacts.json");
    	
    	String instanceUrl = "http://localhost:8081/artifactory/";
    	String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
    	String repoName = "release";
    	
    	when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
    		.thenReturn(new ResponseEntity<>(mavenArtifactsJson, HttpStatus.OK));
    	List<BinaryArtifact> artifacts = defaultArtifactoryClient.getArtifacts(instanceUrl, repoName, 0);
    	assertThat(artifacts.size(), is(1));
    	
    	assertThat(artifacts.get(0).getArtifactName(), is("helloworld"));
    	assertThat(artifacts.get(0).getArtifactGroupId(), is("com.mycompany.myapp"));
    	assertThat(artifacts.get(0).getArtifactVersion(), is("4.8.5.20160909-091018I"));
    	assertThat(artifacts.get(0).getCanonicalName(), is("helloworld-4.8.5.20160909-091018I.jar"));
    	assertThat(artifacts.get(0).getTimestamp(), is(FULL_DATE.parse("2016-09-09T09:10:37.945-04:00").getTime()));
    	assertThat(artifacts.get(0).getBuildUrl(), is("http://localhost:8080/job/myname_helloworld/1/"));
    	assertThat(artifacts.get(0).getBuildNumber(), is("1"));
    	assertThat(artifacts.get(0).getInstanceUrl(), is("http://localhost:8080/"));
    	assertThat(artifacts.get(0).getJobName(), is("myname_helloworld"));
    	assertThat(artifacts.get(0).getJobUrl(), is("http://localhost:8080/job/myname_helloworld"));
    	assertThat(artifacts.get(0).getScmUrl(), is("https://github.com/~myname/helloworld.git"));
    	assertThat(artifacts.get(0).getScmBranch(), is("origin/master"));
    	assertThat(artifacts.get(0).getScmRevisionNumber(), is("943a7c299ec551d985356e5ad52766b38c52e893"));
    }
    
    @Test
    public void testGetIvyArtifacts() throws Exception {
    	String ivyArtifactsJson = getJson("ivyArtifacts.json");
    	
    	String instanceUrl = "http://localhost:8081/artifactory/";
    	String aqlUrl = "http://localhost:8081/artifactory/api/search/aql";
    	String repoName = "release";
    	
    	when(rest.exchange(eq(aqlUrl), eq(HttpMethod.POST), Matchers.any(HttpEntity.class), eq(String.class)))
    		.thenReturn(new ResponseEntity<>(ivyArtifactsJson, HttpStatus.OK));
    	List<BinaryArtifact> artifacts = defaultArtifactoryClient.getArtifacts(instanceUrl, repoName, 0);
    	assertThat(artifacts.size(), is(2));
    	
    	assertThat(artifacts.get(0).getArtifactName(), is("helloworld"));
    	assertThat(artifacts.get(0).getArtifactGroupId(), is("com.mycompany.myapp"));
    	assertThat(artifacts.get(0).getArtifactVersion(), is("4.8.5.20160909-091018I"));
    	assertThat(artifacts.get(0).getCanonicalName(), is("helloworld-4.8.5.20160909-091018I.jar"));
    	assertThat(artifacts.get(0).getTimestamp(), is(FULL_DATE.parse("2016-09-09T09:10:37.945-04:00").getTime()));
    	assertThat(artifacts.get(0).getBuildUrl(), is("http://localhost:8080/job/myname_helloworld/1/"));
    	assertThat(artifacts.get(0).getBuildNumber(), is("1"));
    	assertThat(artifacts.get(0).getInstanceUrl(), is("http://localhost:8080/"));
    	assertThat(artifacts.get(0).getJobName(), is("myname_helloworld"));
    	assertThat(artifacts.get(0).getJobUrl(), is("http://localhost:8080/job/myname_helloworld"));
    	assertThat(artifacts.get(0).getScmUrl(), is("https://github.com/~myname/helloworld.git"));
    	assertThat(artifacts.get(0).getScmBranch(), is("origin/master"));
    	assertThat(artifacts.get(0).getScmRevisionNumber(), is("943a7c299ec551d985356e5ad52766b38c52e893"));
    	
    	assertThat(artifacts.get(1).getArtifactName(), is("ivy"));
    	assertThat(artifacts.get(1).getArtifactGroupId(), is("com.mycompany.myapp"));
    	assertThat(artifacts.get(1).getArtifactVersion(), is("4.8.5.20160909-091018I"));
    	assertThat(artifacts.get(1).getCanonicalName(), is("ivy-4.8.5.20160909-091018I.xml"));
    	assertThat(artifacts.get(1).getTimestamp(), is(FULL_DATE.parse("2016-10-13T05:10:49.209-04:00").getTime()));
    }
    
    private String getJson(String fileName) throws IOException {
        InputStream inputStream = DefaultArtifactoryClient.class.getResourceAsStream(fileName);
        return IOUtils.toString(inputStream);
    }

	// Artifactory Pagination Helper: returns artifact items json with updated times for testing purposes
	private JSONObject updateJsonArtifactTimes(String fileName, long currentTime) throws IOException {
		InputStream inputStream = DefaultArtifactoryClient.class.getResourceAsStream(fileName);
		String jstr = IOUtils.toString(inputStream);
		ObjectMapper mapper = new ObjectMapper();
		Map jsonMap = mapper.readValue(jstr, Map.class);
		List<Map<String, String>> results = (List) jsonMap.get("results");
		long updatedTime = currentTime;

		for (Map<String, String> j : results) {
			// override default dates to last few days for testing purposes
			j.replace("created", (FULL_DATE.format(new Date(updatedTime))));
			updatedTime += TimeUnit.DAYS.toMillis(1);
		}

		JSONObject response = new JSONObject();
		response.put("results", results);
		response.put("range", jsonMap.get("range"));
		return response;
	}

	// Artifactory Pagination Helper: returns queried json string based on time interval
	private String queryJsonByTime(JSONObject artifactItems, long createdGT, long createdLTE) {
		List<Map<String, String>> results = (List) artifactItems.get("results");
		List<Map<String, String>> queriedResults = new ArrayList<>();

		for (Map<String, String> j : results) {
			try {
				Date d = FULL_DATE.parse(j.get("created"));
				if (d.getTime() > createdGT && d.getTime() <= createdLTE) {
					queriedResults.add(j);
				}
			} catch (java.text.ParseException e) {
				e.printStackTrace();
			}
		}

		JSONObject response = new JSONObject();
		response.put("results", queriedResults);
		response.put("range", artifactItems.get("range"));
		return response.toJSONString();

	}

	// new getArtifacts helper
	private String updateJsonArtifactFields(String fileName, Map<String, String> fields) throws IOException {
		InputStream inputStream = DefaultArtifactoryClient.class.getResourceAsStream(fileName);
		String jstr = IOUtils.toString(inputStream);
		ObjectMapper mapper = new ObjectMapper();
		Map jsonMap = mapper.readValue(jstr, Map.class);
		List<Map<String, String>> results = (List) jsonMap.get("results");

		for (Map<String, String> j : results) {
			// override default field value(s) for testing purposes
			for (String field : fields.keySet()) {
				j.replace(field, fields.get(field));
			}
		}

		JSONObject response = new JSONObject();
		response.put("results", results);
		response.put("range", jsonMap.get("range"));
		return response.toJSONString();
	}

    private Iterable<BinaryArtifact> binaryArtifactIterable(boolean buildInfo){
    	BinaryArtifact b = new BinaryArtifact();
		b.setType("file");
		b.setCreatedTimeStamp(new Long("1539268036031"));
		b.setCreatedBy("auto");
		b.setModifiedTimeStamp(new Long("1539268036031"));
		b.setModifiedBy("auto");
		b.setActual_md5("111aadc11ed11b1111df111d16d6c8d821112f1");
		b.setActual_sha1("111aadc11ed11b1111df111d16d6c8d821112f1");
		b.setCanonicalName("name");
		b.setTimestamp(new Long("1539268036031"));
		b.setCollectorItemId(ObjectId.get());
		if(buildInfo){
			b.setBuildInfos(buildInfo());
		}


		BinaryArtifact b_1 = new BinaryArtifact();
		b_1.setType("file");
		b_1.setCreatedTimeStamp(new Long("1539268036031"));
		b_1.setCreatedBy("auto");
		b_1.setModifiedTimeStamp(new Long("1539268036031"));
		b_1.setModifiedBy("auto");
		b_1.setActual_md5("111aadc11ed11b1111df111d16d6c8d821112f1");
		b_1.setActual_sha1("111aadc11ed11b1111df111d16d6c8d821112f1");
		b_1.setCanonicalName("name");
		b_1.setTimestamp(new Long("1539268036031"));
		b_1.setCollectorItemId(ObjectId.get());


		return Arrays.asList(b,b_1);
	}

	private List<Build> buildInfo(){
    	Build build = new Build();
    	build.setBuildUrl("http://localhost:8082/generic/test/job");
    	build.setNumber("773");
    	build.setTimestamp(new Long("1539268036031"));
    	build.setStartedBy("auto");
    	build.setCollectorItemId(ObjectId.get());
    	return Arrays.asList(build);
	}

	private BinaryArtifact binaryArtifactLatestCollectorItemId(ObjectId collectorItemId, boolean buildInfo){
		BinaryArtifact b = new BinaryArtifact();
		b.setType("file");
		b.setCreatedTimeStamp(new Long("1539268036031"));
		b.setCreatedBy("auto");
		b.setModifiedTimeStamp(new Long("1539268036031"));
		b.setModifiedBy("auto");
		b.setActual_md5("111aadc11ed11b1111df111d16d6c8d821112f1");
		b.setActual_sha1("111aadc11ed11b1111df111d16d6c8d821112f1");
		b.setCanonicalName("name");
		b.setArtifactName("test-dev");
		b.setArtifactVersion("0");
		b.setTimestamp(new Long("1539268036031"));
		b.setCollectorItemId(collectorItemId);
		if(buildInfo){
			b.setBuildInfos(buildInfo());
		}

		return b;
	}

	private BinaryArtifact createMatchedExistingBinaryArtifact(ObjectId collectorItemId, String artifactName, String artifactVersion, String path, String repoName, boolean buildInfo){
		BinaryArtifact b = new BinaryArtifact();
		b.setType("file");
		b.setCreatedTimeStamp(new Long("1539268036031"));
		b.setCreatedBy("auto");
		b.setModifiedTimeStamp(new Long("1539268036031"));
		b.setModifiedBy("auto");
		b.setActual_md5("111aadc11ed11b1111df111d16d6c8d821112f1");
		b.setActual_sha1("111aadc11ed11b1111df111d16d6c8d821112f1");
		b.setCanonicalName("name");
		b.setArtifactName(artifactName);
		b.setArtifactVersion(artifactVersion);
		b.setRepo(repoName);
		b.setPath(path);
		b.setTimestamp(new Long("1539268036031"));
		b.setCollectorItemId(collectorItemId);
		if(buildInfo){
			b.setBuildInfos(buildInfo());
		}

		return b;
	}

	private ArtifactItem createArtifactItem(ObjectId id, String artifactName, String instanceUrl, String repoName) {
    	ArtifactItem a = new ArtifactItem();
    	a.setArtifactName(artifactName);
    	a.setInstanceUrl(instanceUrl);
    	a.setRepoName(repoName);
    	a.setPath("dummy/test-dev/1");
    	a.setId(id);

    	return a;
	}
}
