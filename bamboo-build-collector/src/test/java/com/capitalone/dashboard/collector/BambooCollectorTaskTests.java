package com.capitalone.dashboard.collector;

import com.capitalone.dashboard.model.Build;
import com.capitalone.dashboard.model.Component;
import com.capitalone.dashboard.model.BambooCollector;
import com.capitalone.dashboard.model.BambooJob;
import com.capitalone.dashboard.repository.BuildRepository;
import com.capitalone.dashboard.repository.ComponentRepository;
import com.capitalone.dashboard.repository.BambooCollectorRepository;
import com.capitalone.dashboard.repository.BambooJobRepository;
import com.google.common.collect.Sets;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.scheduling.TaskScheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BambooCollectorTaskTests {

    @Mock private TaskScheduler taskScheduler;
    @Mock private BambooCollectorRepository bambooCollectorRepository;
    @Mock private BambooJobRepository bambooJobRepository;
    @Mock private BuildRepository buildRepository;
    @Mock private BambooClient bambooClient;
    @Mock private BambooSettings bambooSettings;
    @Mock private ComponentRepository dbComponentRepository;

    @InjectMocks private BambooCollectorTask task;

    private static final String SERVER1 = "server1";

    @Test
    public void collect_noBuildServers_nothingAdded() {
        when(dbComponentRepository.findAll()).thenReturn(components());
        task.collect(new BambooCollector());
        verifyZeroInteractions(bambooClient, buildRepository);
    }

    @Test
    public void collect_noJobsOnServer_nothingAdded() {
        when(bambooClient.getInstanceJobs(SERVER1)).thenReturn(new HashMap<BambooJob, Set<Build>>());
        when(dbComponentRepository.findAll()).thenReturn(components());
        task.collect(collectorWithOneServer());

        verify(bambooClient).getInstanceJobs(SERVER1);
        verifyNoMoreInteractions(bambooClient, buildRepository);
    }

    @Test
    public void collect_twoJobs_jobsAdded() {
        when(bambooClient.getInstanceJobs(SERVER1)).thenReturn(twoJobsWithTwoBuilds(SERVER1));
        when(dbComponentRepository.findAll()).thenReturn(components());
        task.collect(collectorWithOneServer());

        verify(bambooJobRepository, times(2)).save(any(BambooJob.class));
    }

    @Test
    public void collect_oneJob_exists_notAdded() {
        BambooCollector collector = collectorWithOneServer();
        BambooJob job = bambooJob("JOB1", SERVER1, "JOB1_URL");
        when(bambooClient.getInstanceJobs(SERVER1)).thenReturn(oneJobWithBuilds(job));
        when(bambooJobRepository.findBambooJob(collector.getId(), SERVER1, job.getJobName()))
                .thenReturn(job);
        when(dbComponentRepository.findAll()).thenReturn(components());

        task.collect(collector);

        verify(bambooJobRepository, never()).save(job);
    }

    @Test
    public void collect_jobNotEnabled_buildNotAdded() {
        BambooCollector collector = collectorWithOneServer();
        BambooJob job = bambooJob("JOB1", SERVER1, "JOB1_URL");
        Build build = build("JOB1_1", "JOB1_1_URL");

        when(bambooClient.getInstanceJobs(SERVER1)).thenReturn(oneJobWithBuilds(job, build));
        when(dbComponentRepository.findAll()).thenReturn(components());
        task.collect(collector);

        verify(buildRepository, never()).save(build);
    }

    @Test
    public void collect_jobEnabled_buildExists_buildNotAdded() {
        BambooCollector collector = collectorWithOneServer();
        BambooJob job = bambooJob("JOB1", SERVER1, "JOB1_URL");
        Build build = build("JOB1_1", "JOB1_1_URL");

        when(bambooClient.getInstanceJobs(SERVER1)).thenReturn(oneJobWithBuilds(job, build));
        when(bambooJobRepository.findEnabledBambooJobs(collector.getId(), SERVER1))
                .thenReturn(Arrays.asList(job));
        when(buildRepository.findByCollectorItemIdAndNumber(job.getId(), build.getNumber())).thenReturn(build);
        when(dbComponentRepository.findAll()).thenReturn(components());
        task.collect(collector);

        verify(buildRepository, never()).save(build);
    }

    @Test
    public void collect_jobEnabled_newBuild_buildAdded() {
        BambooCollector collector = collectorWithOneServer();
        BambooJob job = bambooJob("JOB1", SERVER1, "JOB1_URL");
        Build build = build("JOB1_1", "JOB1_1_URL");

        when(bambooClient.getInstanceJobs(SERVER1)).thenReturn(oneJobWithBuilds(job, build));
        when(bambooJobRepository.findEnabledBambooJobs(collector.getId(), SERVER1))
                .thenReturn(Arrays.asList(job));
        when(buildRepository.findByCollectorItemIdAndNumber(job.getId(), build.getNumber())).thenReturn(null);
        when(bambooClient.getBuildDetails(build.getBuildUrl())).thenReturn(build);
        when(dbComponentRepository.findAll()).thenReturn(components());
        task.collect(collector);

        verify(buildRepository, times(1)).save(build);
    }

    private BambooCollector collectorWithOneServer() {
        return BambooCollector.prototype(Arrays.asList(SERVER1));
    }

    private Map<BambooJob, Set<Build>> oneJobWithBuilds(BambooJob job, Build... builds) {
        Map<BambooJob, Set<Build>> jobs = new HashMap<>();
        jobs.put(job, Sets.newHashSet(builds));
        return jobs;
    }

    private Map<BambooJob, Set<Build>> twoJobsWithTwoBuilds(String server) {
        Map<BambooJob, Set<Build>> jobs = new HashMap<>();
        jobs.put(bambooJob("JOB1", server, "JOB1_URL"), Sets.newHashSet(build("JOB1_1", "JOB1_1_URL"), build("JOB1_2", "JOB1_2_URL")));
        jobs.put(bambooJob("JOB2", server, "JOB2_URL"), Sets.newHashSet(build("JOB2_1", "JOB2_1_URL"), build("JOB2_2", "JOB2_2_URL")));
        return jobs;
    }

    private BambooJob bambooJob(String jobName, String instanceUrl, String jobUrl) {
        BambooJob job = new BambooJob();
        job.setJobName(jobName);
        job.setInstanceUrl(instanceUrl);
        job.setJobUrl(jobUrl);
        return job;
    }

    private Build build(String number, String url) {
        Build build = new Build();
        build.setNumber(number);
        build.setBuildUrl(url);
        return build;
    }

    private ArrayList<com.capitalone.dashboard.model.Component> components() {
    	ArrayList<com.capitalone.dashboard.model.Component> cArray = new ArrayList<com.capitalone.dashboard.model.Component>();
    	com.capitalone.dashboard.model.Component c = new Component();
    	c.setId(new ObjectId());
    	c.setName("COMPONENT1");
    	c.setOwner("JOHN");
    	cArray.add(c);
    	return cArray;
    }
}
