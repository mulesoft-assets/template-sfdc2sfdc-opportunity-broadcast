package org.mule.kicks.integration;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mule.kicks.builders.SfdcObjectBuilder.anAccount;

import java.util.ArrayList;
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
import org.mule.api.context.notification.ServerNotification;
import org.mule.context.notification.NotificationException;
import org.mule.kicks.builders.SfdcObjectBuilder;
import org.mule.kicks.test.utils.BatchTestHelper;
import org.mule.kicks.test.utils.ListenerProbe;
import org.mule.kicks.test.utils.PipelineSynchronizeListener;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.tck.probe.PollingProber;
import org.mule.tck.probe.Probe;
import org.mule.tck.probe.Prober;
import org.mule.transport.NullPayload;

import com.mulesoft.module.batch.api.notification.BatchNotification;
import com.mulesoft.module.batch.api.notification.BatchNotificationListener;
import com.mulesoft.module.batch.engine.BatchJobInstanceStore;
import com.sforce.soap.partner.SaveResult;

/**
 * The objective of this class is to validate the correct behavior of the Mule Kick that make calls to external systems.
 * 
 */
public class BusinessLogicIT extends AbstractKickTestCase {

	private static final String POLL_FLOW_NAME = "triggerFlow";
	private static final String KICK_NAME = "sfdc2sfdc-accounts-onewaysync";

	private static final int TIMEOUT_MILLIS = 60;

	private static SubflowInterceptingChainLifecycleWrapper checkAccountflow;
	private static List<Map<String, Object>> createdAccountsInA = new ArrayList<Map<String, Object>>();

	private final Prober pollProber = new PollingProber(10000, 1000);
	private final PipelineSynchronizeListener pipelineListener = new PipelineSynchronizeListener(POLL_FLOW_NAME);

	private BatchTestHelper helper;

	@BeforeClass
	public static void setTestProperties() {
		System.setProperty("page.size", "1000");

		// Set the frequency between polls to 10 seconds
		System.setProperty("polling.frequency", "10000");

		// Set the poll starting delay to 20 seconds
		System.setProperty("polling.start.delay", "20000");

		// Setting Default Watermark Expression to query SFDC with LastModifiedDate greater than ten seconds before current time
		System.setProperty("watermark.default.expression", "#[groovy: new Date(System.currentTimeMillis() - 10000).format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", TimeZone.getTimeZone('UTC'))]");
	}

	@Before
	public void setUp() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();

		helper = new BatchTestHelper(muleContext);

		// Flow to retrieve accounts from target system after syncing
		checkAccountflow = getSubFlow("retrieveAccountFlow");
		checkAccountflow.initialise();

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
		helper.awaitJobTermination(TIMEOUT_MILLIS * 1000, 500);
		helper.assertJobWasSuccessful();

		// Assert first object was not sync
		assertEquals("The account should not have been sync", null, invokeRetrieveAccountFlow(checkAccountflow, createdAccountsInA.get(0)));

		// Assert second object was not sync
		assertEquals("The account should not have been sync", null, invokeRetrieveAccountFlow(checkAccountflow, createdAccountsInA.get(1)));

		// Assert third object was sync to target system
		Map<String, Object> payload = invokeRetrieveAccountFlow(checkAccountflow, createdAccountsInA.get(2));
		assertEquals("The account should have been sync", createdAccountsInA.get(2)
																			.get("Name"), payload.get("Name"));
		// Assert fourth object was sync to target system
		final Map<String, Object> fourthAccount = createdAccountsInA.get(3);
		payload = invokeRetrieveAccountFlow(checkAccountflow, fourthAccount);
		assertEquals("The account should have been sync (Name)", fourthAccount.get("Name"), payload.get("Name"));
	}

	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}

	private void waitForPollToRun() {
		pollProber.check(new ListenerProbe(pipelineListener));
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> invokeRetrieveAccountFlow(final SubflowInterceptingChainLifecycleWrapper flow, final Map<String, Object> account) throws Exception {
		final Map<String, Object> accountMap = new HashMap<String, Object>();

		accountMap.put("Name", account.get("Name"));
		final MuleEvent event = flow.process(getTestEvent(accountMap, MessageExchangePattern.REQUEST_RESPONSE));
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
		final SubflowInterceptingChainLifecycleWrapper createAccountInBFlow = getSubFlow("createAccountFlowB");
		createAccountInBFlow.initialise();

		SfdcObjectBuilder updateAccount = anAccount().with("Name", buildUniqueName(KICK_NAME, "DemoUpdateAccount"))
														.with("Industry", "Education");

		final List<Map<String, Object>> createdAccountInB = new ArrayList<Map<String, Object>>();
		// This account should BE sync (updated) as the industry is Education, has more than 5000 Employees and the record exists in the target system
		createdAccountInB.add(updateAccount.with("NumberOfEmployees", 17000)
											.build());
		createAccountInBFlow.process(getTestEvent(createdAccountInB, MessageExchangePattern.REQUEST_RESPONSE));

		// Create accounts in source system to be or not to be synced
		final SubflowInterceptingChainLifecycleWrapper createAccountInAFlow = getSubFlow("createAccountFlowA");
		createAccountInAFlow.initialise();

		// This account should not be synced as the industry is not "Education" or "Government"
		createdAccountsInA.add(anAccount().with("Name", buildUniqueName(KICK_NAME, "DemoFilterIndustryAccount"))
											.with("Industry", "Insurance")
											.with("NumberOfEmployees", 17000)
											.build());

		// This account should not be synced as the number of employees is less than 5000
		createdAccountsInA.add(anAccount().with("Name", buildUniqueName(KICK_NAME, "DemoFilterIndustryAccount"))
											.with("Industry", "Government")
											.with("NumberOfEmployees", 2500)
											.build());

		// This account should BE synced (inserted) as the number of employees if greater than 5000 and the industry is "Government"
		createdAccountsInA.add(anAccount().with("Name", buildUniqueName(KICK_NAME, "DemoCreateAccount"))
											.with("Industry", "Government")
											.with("NumberOfEmployees", 18000)
											.build());

		// This account should BE synced (updated) as the number of employees if greater than 5000 and the industry is "Education"
		createdAccountsInA.add(updateAccount.with("NumberOfEmployees", 12000)
											.build());

		final MuleEvent event = createAccountInAFlow.process(getTestEvent(createdAccountsInA, MessageExchangePattern.REQUEST_RESPONSE));
		final List<SaveResult> results = (List<SaveResult>) event.getMessage()
																	.getPayload();
		int i = 0;
		for (SaveResult result : results) {
			Map<String, Object> accountInA = createdAccountsInA.get(i);
			accountInA.put("Id", result.getId());
			i++;
		}
	}

	private void deleteEntities() throws MuleException, Exception {
		// Delete the created accounts in A
		SubflowInterceptingChainLifecycleWrapper deleteAccountFromAflow = getSubFlow("deleteAccountFromAFlow");
		deleteAccountFromAflow.initialise();

		final List<Object> idList = new ArrayList<Object>();
		for (final Map<String, Object> c : createdAccountsInA) {
			idList.add(c.get("Id"));
		}
		deleteAccountFromAflow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));

		// Delete the created accounts in B
		SubflowInterceptingChainLifecycleWrapper deleteAccountFromBflow = getSubFlow("deleteAccountFromBFlow");
		deleteAccountFromBflow.initialise();

		idList.clear();
		for (final Map<String, Object> createdAccount : createdAccountsInA) {
			final Map<String, Object> account = invokeRetrieveAccountFlow(checkAccountflow, createdAccount);
			if (account != null) {
				idList.add(account.get("Id"));
			}
		}
		deleteAccountFromBflow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));
	}

}
