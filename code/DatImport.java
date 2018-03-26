package com.esped.ops.TX;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.esped.lib.AuditVars;
import com.esped.lib.DATES;
import com.esped.lib.DBitem;
import com.esped.lib.ERR;
import com.esped.lib.Logs;
import com.esped.lib.SAV;
import com.esped.lib.StudentVars;

public class DatImport {

	/*
	 * Record operation
	 * 
	 */
	public void logHistory(Connection conn, int org, String tname, String rollbackname, String who, ERR msg,
			boolean dryrun, String wstart, String wfinish) {
		String hist = "REF_DATAIMPORT_HISTORY";
		Vector<DBitem> vparams = new Vector<DBitem>();
		vparams.add(new DBitem(hist, "tname", tname));
		vparams.add(new DBitem(hist, "trollbackname", rollbackname));
		vparams.add(new DBitem(hist, "twho", who));
		vparams.add(new DBitem(hist, "glog", msg.getbuf()));
		if (dryrun) {
			vparams.add(new DBitem(hist, "kdryrun", "X"));
		}
		vparams.add(new DBitem(hist, "wstart", wstart));
		vparams.add(new DBitem(hist, "wfinish", wfinish));
		//Yang
		SAV.insertRecord(conn, null, null, hist, SAV.WHERE_ORGONLY, org, 0, "", vparams, 0);
	}

	/*
	 * Import data and customize log style
	 * 
	 * @param unknownSidList: List saves all the unknown id
	 * 
	 * @param noInformationRecordsMap: Map saves reason why record can not be
	 * processed for all records
	 * 
	 * @param updateRecords: Map which saves all the students' records to be
	 * processed
	 * 
	 * @param table: production table
	 * 
	 * @param tname: file type
	 * 
	 * @param dryrun: dry run
	 * 
	 * @param rollbackname: for getting log from database
	 */
	public ERR executeUpdate(Connection conn, AuditVars audit, int org, List<String> unknownSidList,
			Map<Integer, Map<String, String>> noInformationSubjectMap, Map<String, List<Vector<DBitem>>> updateRecords,
			String table, String tname, boolean dryrun, String rollbackname) {
		ERR msg = new ERR();
		String wstart = "";
		String wfinish = "";
		String updateMsg = "";

		// loop through students
		try {
			if (updateRecords.size() != 0 || unknownSidList.size() != 0) {
				updateMsg = "Starting " + tname.toUpperCase() + " Update: ";
				wstart = DATES.sysDate(DATES.stdtime);
				msg.addbuf(updateMsg);
				msg.addbuf("Update starting on ", DATES.sysDate(DATES.stdhuman));
				if (dryrun) {
					msg.addbuf("*** DRY RUN ONLY ***", "\n", "No updates will be done");
				}
				for (Map.Entry<String, List<Vector<DBitem>>> entry : updateRecords.entrySet()) {
					StudentVars SV = new StudentVars();
					int sid = Integer.parseInt(entry.getKey());
					String gsid = StudentVars.getGSID(conn, org, sid);
					SV = StudentVars.getStudentInfo(conn, org, sid);
					List<Vector<DBitem>> recordsList = entry.getValue();
					msg.addbuf("");
					msg.addbuf("----------------------Processing student: ", SV.lastname, ", ", SV.firstname, " (",
							SV.LocalID, ")----------------------");
					for (int i = 0; i < recordsList.size(); i++) {
						DatImportSAV.auditPreviousValuesForDat(conn, audit, table, org, sid, gsid, 0, recordsList.get(i), SV,
								"REF_DATAIMPORT_HISTORY", rollbackname, dryrun, true, msg);
					}
					for (Map.Entry<String, String> entry2 : noInformationSubjectMap.get(sid).entrySet()) {
						msg.addbuf("");
						msg.addbuf("[" + entry2.getKey() + "]: " + entry2.getValue());
					}
				}
				msg.addbuf("--------------------------------------------------------------------------------------------");
				for (String usid : unknownSidList) {
					if (!usid.equals("")) {
						msg.addbuf("Unknown Student Id: " + usid);
					}
				}
				msg.addbuf("--------------------------------------------------------------------------------------------");
				msg.addbuf("<Update completed on ", DATES.sysDate(DATES.stdhuman) + ">");
				msg.addbuf("<Unique Student Records Processed: " + updateRecords.size() + ">");
				msg.addbuf("<Unknown Student Records: " + unknownSidList.size() + ">");
				wfinish = DATES.sysDate(DATES.stdtime);

				logHistory(conn, org, tname, rollbackname, audit.m_emailname, msg, dryrun, wstart, wfinish);
			}
		} catch (Exception e) {
			Logs.error("DatImport.executeUpdate", e);
		} 
		return msg;
	}
}