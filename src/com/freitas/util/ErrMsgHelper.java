package com.freitas.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;

public class ErrMsgHelper {

	public static String getCleanError(JobClient jobClient, org.apache.hadoop.mapred.JobID oldJobId) throws IOException {
		Map<String,String> errMap = new HashMap<String,String>();
		TaskReport[] mapReports = jobClient.getMapTaskReports(oldJobId);
		processTaskReport(mapReports, errMap);
		if (!errMap.isEmpty()){
			// found err msg in map phase, can return msg
			return ErrMsgHelper.compressErrorMessages(errMap);
		}

		// otherwise go and check the reducers for errors
		TaskReport[] redReports = jobClient.getReduceTaskReports(oldJobId);
		ErrMsgHelper.processTaskReport(redReports, errMap);
		if (!errMap.isEmpty()){
			return ErrMsgHelper.compressErrorMessages(errMap);
		}
		return null;
	}


	public static void processTaskReport(TaskReport[] mapReports, Map<String,String> errMap) {
		for (TaskReport report : mapReports){
			TIPStatus status = report.getCurrentStatus();
			if (status.compareTo(TIPStatus.COMPLETE) == 0){
				// not interested in tasks that completed
				continue;
			}
			for (String err : report.getDiagnostics()){
				// only need first line, can get the rest from logs
				// and don't want to get duplicates
				String[] errArr = err.split("\n");
				errMap.put(errArr[0],errArr[0]);
			}
		}
	}


	public static String compressErrorMessages(Map<String,String> errMap) {
		StringBuilder sb = new StringBuilder(512);
		for (String msg : errMap.keySet()){
			sb.append(msg);
			sb.append(", ");
		}
		sb.delete(sb.length()-2, sb.length());
		return sb.toString();
	}


}
