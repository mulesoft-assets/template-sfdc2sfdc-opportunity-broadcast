package org.mule.kicks.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mule.kicks.builders.SfdcObjectBuilder.anOpportunity;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.context.notification.NotificationException;
import org.mule.kicks.builders.SfdcObjectBuilder;
import org.mule.kicks.test.utils.BatchTestHelper;
import org.mule.kicks.test.utils.ListenerProbe;
import org.mule.kicks.test.utils.PipelineSynchronizeListener;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.tck.probe.PollingProber;
import org.mule.tck.probe.Prober;
import org.mule.transport.NullPayload;

import com.google.common.collect.Maps;
import com.sforce.soap.partner.SaveResult;

/**
 * The objective of this class is to validate the correct behavior of the Mule Kick that make calls to external systems.
 * 
 */
public class BusinessLogicIT extends AbstractKickTestCase {

	private static final String POLL_FLOW_NAME = "triggerFlow";
	private static final String KICK_NAME = "sfdc2sfdc-opportunity-onewaysync";

	private static final int TIMEOUT_SECONDS = 60;

	private static SubflowInterceptingChainLifecycleWrapper checkOpportunityflow;
	private static List<Map<String, Object>> createdOpportunitiesInA = new ArrayList<Map<String, Object>>();

	private final Prober pollProber = new PollingProber(10000, 1000);
	private final PipelineSynchronizeListener pipelineListener = new PipelineSynchronizeListener(POLL_FLOW_NAME);

	private BatchTestHelper helper;

	@BeforeClass
	public static void setTestProperties() {
		System.setProperty("page.size", "1000");

		// Set the frequency between polls to 10 seconds
		System.setProperty("poll.frequencyMillis", "10000");

		// Set the poll starting delay to 20 seconds
		System.setProperty("poll.startDelayMillis", "20000");

		// Setting Default Watermark Expression to query SFDC with LastModifiedDate greater than ten seconds before current time
		System.setProperty("watermark.default.expression", "#[groovy: new Date(System.currentTimeMillis() - 10000).format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", TimeZone.getTimeZone('UTC'))]");
	}

	@Before
	public void setUp() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();

		helper = new BatchTestHelper(muleContext);

		// Flow to retrieve opportunities from target system after syncing
		checkOpportunityflow = getSubFlow("retrieveOpportunityFlow");
		checkOpportunityflow.initialise();

		createEntities();
	}

	@After
	public void tearDown() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);

		deleteEntities();
	}

	@Test
	public void testMainFlow() throws Exception {
		// Run poll and wait for it to run
		runSchedulersOnce(POLL_FLOW_NAME);
		waitForPollToRun();

		// Wait for the batch job executed by the poll flow to finish
		helper.awaitJobTermination(TIMEOUT_SECONDS * 1000, 500);
		helper.assertJobWasSuccessful();

		// Assert first object was not sync
		assertEquals("The opportunity should not have been sync", null, invokeRetrieveOpportunityFlow(checkOpportunityflow, createdOpportunitiesInA.get(0)));

		// Assert second object was sync to target system
		Map<String, Object> payload = invokeRetrieveOpportunityFlow(checkOpportunityflow, createdOpportunitiesInA.get(1));
		assertEquals("The opportunity should have been sync", createdOpportunitiesInA.get(1)
																						.get("Name"), payload.get("Name"));
		// Assert third object was sync to target system
		final Map<String, Object> thirdOpportunity = new HashMap<String, Object>(createdOpportunitiesInA.get(2));
		payload = invokeRetrieveOpportunityFlow(checkOpportunityflow, thirdOpportunity);
		assertEquals("The opportunity should exist in target system", payload.get("Name"), thirdOpportunity.get("Name"));
	}

	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}

	private void waitForPollToRun() {
		pollProber.check(new ListenerProbe(pipelineListener));
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> invokeRetrieveOpportunityFlow(final SubflowInterceptingChainLifecycleWrapper flow, final Map<String, Object> opportunity) throws Exception {
		final Map<String, Object> opportunityMap = new HashMap<String, Object>();

		opportunityMap.put("Name", opportunity.get("Name"));
		final MuleEvent event = flow.process(getTestEvent(opportunityMap, MessageExchangePattern.REQUEST_RESPONSE));
		final Object payload = event.getMessage()
									.getPayload();
		if (payload instanceof NullPayload) {
			return null;
		} else {
			return (Map<String, Object>) payload;
		}
	}

	@SuppressWarnings("unchecked")
	private void createEntities() throws MuleException, Exception {

		// Create object in target system to be update
		final SubflowInterceptingChainLifecycleWrapper createOpportunityInBFlow = getSubFlow("createOpportunityFlowB");
		createOpportunityInBFlow.initialise();

		SfdcObjectBuilder updateOpportunity = anOpportunity().with("Name", buildUniqueName(KICK_NAME, "DemoUpdate"))
																.with("Amount", 12000.0);

		final List<Map<String, Object>> createdOpportunityInB = new ArrayList<Map<String, Object>>();
		// This opportunity should BE sync (updated) as the industry is Education, has more than 5000 Employees and the record exists in the target system
		createdOpportunityInB.add(updateOpportunity.with("StageName", "MyStageBeforeUpdated")
													.with("CloseDate", date("2032-06-12"))
													.with("Probability", "50")
													.build());
		createOpportunityInBFlow.process(getTestEvent(createdOpportunityInB, MessageExchangePattern.REQUEST_RESPONSE));

		// Create opportunities in source system to be or not to be synced
		final SubflowInterceptingChainLifecycleWrapper createOpportunityInAFlow = getSubFlow("createOpportunityFlowA");
		createOpportunityInAFlow.initialise();

		// This opportunity should not be synced as the amount is less than 5000
		createdOpportunitiesInA.add(anOpportunity().with("Name", buildUniqueName(KICK_NAME, "DemoFilter"))
													.with("Amount", 100)
													.with("StageName", "NoStage")
													.with("CloseDate", date("2050-10-10"))
													.with("Probability", "1")
													.build());

		// This opportunity should BE synced (inserted) as the amount is greater than 5000
		createdOpportunitiesInA.add(anOpportunity().with("Name", buildUniqueName(KICK_NAME, "DemoCreate"))
													.with("Amount", 10000)
													.with("StageName", "NewStage")
													.with("CloseDate", date("2051-11-11"))
													.with("Probability", "100")
													.build());

		// This opportunity should BE synced (updated) as the amount is greater than 5000
		createdOpportunitiesInA.add(updateOpportunity.with("StageName", "MyStageAfterUpdated")
														.with("CloseDate", date("2040-07-13"))
														.build());

		final MuleEvent event = createOpportunityInAFlow.process(getTestEvent(createdOpportunitiesInA, MessageExchangePattern.REQUEST_RESPONSE));
		final List<SaveResult> results = (List<SaveResult>) event.getMessage()
																	.getPayload();
		int i = 0;
		for (SaveResult result : results) {
			Map<String, Object> opportunityInA = createdOpportunitiesInA.get(i);
			opportunityInA.put("Id", result.getId());
			i++;
		}
	}

	private void deleteEntities() throws MuleException, Exception {
		// Delete the created opportunities in A
		SubflowInterceptingChainLifecycleWrapper deleteOpportunityFromAflow = getSubFlow("deleteOpportunityFromAFlow");
		deleteOpportunityFromAflow.initialise();

		final List<Object> idList = new ArrayList<Object>();
		for (final Map<String, Object> c : createdOpportunitiesInA) {
			idList.add(c.get("Id"));
		}
		deleteOpportunityFromAflow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));

		// Delete the created opportunities in B
		SubflowInterceptingChainLifecycleWrapper deleteOpportunityFromBflow = getSubFlow("deleteOpportunityFromBFlow");
		deleteOpportunityFromBflow.initialise();

		idList.clear();
		for (final Map<String, Object> createdOpportunity : createdOpportunitiesInA) {
			final Map<String, Object> opportunity = invokeRetrieveOpportunityFlow(checkOpportunityflow, createdOpportunity);
			if (opportunity != null) {
				idList.add(opportunity.get("Id"));
			}
		}
		deleteOpportunityFromBflow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));
	}

	private Date date(String dateString) throws ParseException {
		return new SimpleDateFormat("yyyy-MM-dd").parse(dateString);
	}

}
