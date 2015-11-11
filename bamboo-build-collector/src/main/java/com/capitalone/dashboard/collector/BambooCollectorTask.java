package com.capitalone.dashboard.collector;

import com.capitalone.dashboard.model.Build;
import com.capitalone.dashboard.model.CollectorItem;
import com.capitalone.dashboard.model.CollectorType;
import com.capitalone.dashboard.model.BambooCollector;
import com.capitalone.dashboard.model.BambooJob;
import com.capitalone.dashboard.repository.BaseCollectorRepository;
import com.capitalone.dashboard.repository.BuildRepository;
import com.capitalone.dashboard.repository.ComponentRepository;
import com.capitalone.dashboard.repository.BambooCollectorRepository;
import com.capitalone.dashboard.repository.BambooJobRepository;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CollectorTask that fetches Build information from Bamboo
 */
@Component
public class BambooCollectorTask extends CollectorTask<BambooCollector> {

	private static final Log LOG = LogFactory.getLog(BambooCollectorTask.class);

	private final BambooCollectorRepository bambooCollectorRepository;
	private final BambooJobRepository bambooJobRepository;
	private final BuildRepository buildRepository;
	private final BambooClient bambooClient;
	private final BambooSettings bambooSettings;
	private final ComponentRepository dbComponentRepository;
	private final int CLEANUP_INTERVAL = 3600000;

	@Autowired
	public BambooCollectorTask(TaskScheduler taskScheduler,
			BambooCollectorRepository bambooCollectorRepository,
			BambooJobRepository bambooJobRepository,
			BuildRepository buildRepository, BambooClient bambooClient,
			BambooSettings bambooSettings,
			ComponentRepository dbComponentRepository) {
		super(taskScheduler, "Bamboo");
		this.bambooCollectorRepository = bambooCollectorRepository;
		this.bambooJobRepository = bambooJobRepository;
		this.buildRepository = buildRepository;
		this.bambooClient = bambooClient;
		this.bambooSettings = bambooSettings;
		this.dbComponentRepository = dbComponentRepository;
	}

	@Override
	public BambooCollector getCollector() {
		return BambooCollector.prototype(bambooSettings.getServers());
	}

	@Override
	public BaseCollectorRepository<BambooCollector> getCollectorRepository() {
		return bambooCollectorRepository;
	}

	@Override
	public String getCron() {
		return bambooSettings.getCron();
	}

	@Override
	public void collect(BambooCollector collector) {
		long start = System.currentTimeMillis();

		// Clean up every hour
		if ((start - collector.getLastExecuted()) > CLEANUP_INTERVAL) {
			clean(collector);
		}
		for (String instanceUrl : collector.getBuildServers()) {
			logInstanceBanner(instanceUrl);

			Map<BambooJob, Set<Build>> buildsByJob = bambooClient
					.getInstanceJobs(instanceUrl);
			log("Fetched jobs", start);

			addNewJobs(buildsByJob.keySet(), collector);

			addNewBuilds(enabledJobs(collector, instanceUrl), buildsByJob);

			log("Finished", start);
		}

	}

	/**
	 * Clean up unused bamboo collector items
	 *
	 * @param collector
	 *            the {@link BambooCollector}
	 */

	private void clean(BambooCollector collector) {
		Set<ObjectId> uniqueIDs = new HashSet<ObjectId>();
		for (com.capitalone.dashboard.model.Component comp : dbComponentRepository
				.findAll()) {
			if (comp.getCollectorItems() != null
					&& !comp.getCollectorItems().isEmpty()) {
				List<CollectorItem> itemList = comp.getCollectorItems().get(
						CollectorType.Build);
				if (itemList != null) {
					for (CollectorItem ci : itemList) {
						if (ci != null
								&& ci.getCollectorId().equals(collector.getId())) {
							uniqueIDs.add(ci.getId());
						}
					}
				}
			}
		}
		List<BambooJob> jobList = new ArrayList<BambooJob>();
		Set<ObjectId> udId = new HashSet<ObjectId>();
		udId.add(collector.getId());
		for (BambooJob job : bambooJobRepository.findByCollectorIdIn(udId)) {
			if (job != null) {
				job.setEnabled(uniqueIDs.contains(job.getId()));
				jobList.add(job);
			}
		}
		bambooJobRepository.save(jobList);
	}

	/**
	 * Iterates over the enabled build jobs and adds new builds to the database.
	 *
	 * @param enabledJobs
	 *            list of enabled {@link BambooJob}s
	 * @param buildsByJob
	 *            maps a {@link BambooJob} to a set of {@link Build}s.
	 */
	private void addNewBuilds(List<BambooJob> enabledJobs,
			Map<BambooJob, Set<Build>> buildsByJob) {
		long start = System.currentTimeMillis();
		int count = 0;

		for (BambooJob job : enabledJobs) {

			for (Build buildSummary : nullSafe(buildsByJob.get(job))) {

				if (isNewBuild(job, buildSummary)) {
					Build build = bambooClient.getBuildDetails(buildSummary
							.getBuildUrl());
					if (build != null) {
						build.setCollectorItemId(job.getId());
						buildRepository.save(build);
						count++;
					}
				}

			}
		}
		log("New builds", start, count);
	}

	private Set<Build> nullSafe(Set<Build> builds) {
		return builds == null ? new HashSet<Build>() : builds;
	}

	/**
	 * Adds new {@link BambooJob}s to the database as disabled jobs.
	 *
	 * @param jobs
	 *            list of {@link BambooJob}s
	 * @param collector
	 *            the {@link BambooCollector}
	 */
	private void addNewJobs(Set<BambooJob> jobs, BambooCollector collector) {
		long start = System.currentTimeMillis();
		int count = 0;

		for (BambooJob job : jobs) {

			if (isNewJob(collector, job)) {
				job.setCollectorId(collector.getId());
				job.setEnabled(false); // Do not enable for collection. Will be
										// enabled when added to dashboard
				job.setDescription(job.getJobName());
				bambooJobRepository.save(job);
				count++;
			}

		}
		log("New jobs", start, count);
	}

	private List<BambooJob> enabledJobs(BambooCollector collector,
			String instanceUrl) {
		return bambooJobRepository.findEnabledBambooJobs(collector.getId(),
				instanceUrl);
	}

	private boolean isNewJob(BambooCollector collector, BambooJob job) {
		return bambooJobRepository.findBambooJob(collector.getId(),
				job.getInstanceUrl(), job.getJobName()) == null;
	}

	private boolean isNewBuild(BambooJob job, Build build) {
		return buildRepository.findByCollectorItemIdAndNumber(job.getId(),
				build.getNumber()) == null;
	}

	private void log(String marker, long start) {
		log(marker, start, null);
	}

	private void log(String text, long start, Integer count) {
		long end = System.currentTimeMillis();
		String elapsed = ((end - start) / 1000) + "s";
		String token2 = "";
		String token3;
		if (count == null) {
			token3 = StringUtils.leftPad(elapsed, 30 - text.length());
		} else {
			String countStr = count.toString();
			token2 = StringUtils.leftPad(countStr, 20 - text.length());
			token3 = StringUtils.leftPad(elapsed, 10);
		}
		LOG.info(text + token2 + token3);
	}

	private void logInstanceBanner(String instanceUrl) {
		LOG.info("------------------------------");
		LOG.info(instanceUrl);
		LOG.info("------------------------------");
	}
}
