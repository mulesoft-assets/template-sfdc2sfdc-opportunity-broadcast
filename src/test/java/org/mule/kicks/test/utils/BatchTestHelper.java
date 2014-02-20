package org.mule.kicks.test.utils;

import static junit.framework.Assert.assertTrue;

import org.mule.api.MuleContext;
import org.mule.api.context.notification.ServerNotification;
import org.mule.api.registry.RegistrationException;
import org.mule.context.notification.NotificationException;
import org.mule.tck.probe.PollingProber;
import org.mule.tck.probe.Probe;
import org.mule.tck.probe.Prober;

import com.mulesoft.module.batch.api.notification.BatchNotification;
import com.mulesoft.module.batch.api.notification.BatchNotificationListener;
import com.mulesoft.module.batch.engine.BatchJobInstanceStore;

public class BatchTestHelper {

	private Prober prober;
	protected Boolean failed;
	protected BatchJobInstanceStore jobInstanceStore;

	public BatchTestHelper(MuleContext muleContext) throws RegistrationException, NotificationException {
		failed = null;
		jobInstanceStore = muleContext.getRegistry()
										.lookupObject(BatchJobInstanceStore.class);
		muleContext.registerListener(new BatchWaitListener());
	}

	public void awaitJobTermination(long timeoutMillis, long pollDelayMillis) throws Exception {
		this.prober = new PollingProber(timeoutMillis, pollDelayMillis);
		this.prober.check(new Probe() {

			@Override
			public boolean isSatisfied() {
				return failed != null;
			}

			@Override
			public String describeFailure() {
				return "batch job timed out";
			}
		});
	}

	protected boolean wasJobSuccessful() {
		return this.failed != null ? !this.failed : false;
	}

	public void assertJobWasSuccessful() {
		assertTrue("Batch job was not successful", wasJobSuccessful());
	}
	
	/*
	 * Helper Classes 
	 */
	
	protected class BatchWaitListener implements BatchNotificationListener {

		public synchronized void onNotification(ServerNotification notification) {
			final int action = notification.getAction();

			if (action == BatchNotification.JOB_SUCCESSFUL || action == BatchNotification.JOB_STOPPED) {
				failed = false;
			} else if (action == BatchNotification.JOB_PROCESS_RECORDS_FAILED || action == BatchNotification.LOAD_PHASE_FAILED || action == BatchNotification.INPUT_PHASE_FAILED
					|| action == BatchNotification.ON_COMPLETE_FAILED) {

				failed = true;
			}
		}
	}

}
