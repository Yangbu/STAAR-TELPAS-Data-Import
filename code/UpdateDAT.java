package com.esped.proc.TX.LPAC;

import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.esped.lib.AppVars;
import com.esped.lib.DATES;
import com.esped.lib.DBitem;
import com.esped.lib.ERR;
import com.esped.lib.FILE;
import com.esped.lib.Logs;
import com.esped.lib.StudentVars;
import com.esped.lib.file.DataFileDAT;
import com.esped.login.SessionData;
import com.esped.ops.TX.DatImport;
import com.esped.ops.US.DATMapping;

public class UpdateDAT {

	/*
	 * Main function
	 * 
	 * @param fileName: uploaded file
	 * 
	 * @param dropdownlistItem: file type
	 * 
	 * @param doit: dry run flag
	 * 
	 * @param rollbackname: for getting log from database
	 */
	public void run(HttpServletRequest _req, HttpServletResponse _res, AppVars _cons, String fileName,
			String dropdownlistItem, String doit, String rollbackname) {
		String fileType = "";
		// Simplify file type
		for (String s : dropdownlistItem.trim().split("\\s+")) {
			fileType += s.toLowerCase();
		}
		String tyear = fileType.substring(0, 4);
		String tname = fileType.substring(4);
		String methodname = "Update" + tname.toUpperCase();
		String table = tname.equals("telpas") ? "REVIEW_TELPAS_TESTS" : "REVIEW_STAAR_TESTS";
		DatImport datImport = new DatImport();
		boolean dryrun = doit.equalsIgnoreCase("true") ? false : true;
		DataFileDAT df = null;
		ERR err = new ERR();
		ERR msg = new ERR();
		String fname = "";
		int[] lineCounter = { 0 };

		try {
			Connection conn = _cons.getConnection();
			// Records from Table REF_DATAIMPORT_MAPPING
			Vector<DATMapping> vmap = DATMapping.getDATMappings(conn, tname, tyear);

			// Reason records from Table REF_DATAIMPORT_SUBJECT_MAPPING
			Vector<DATMapping> reasonMap = DATMapping.getSubMappings(conn, tname, tyear, "REASON");

			// Field records from Table REF_DATAIMPORT_SUBJECT_MAPPING
			Vector<DATMapping> subMap = DATMapping.getSubMappings(conn, tname, tyear, "FIELD");

			// Records from Table REF_DATAIMPORT_CODE
			Vector<DATMapping> codeMap = DATMapping.getCodeMappings(conn, tname, tyear);

			// Length limitation from Table REF_DATAIMPORT_FILE
			Vector<DATMapping> lengthMap = DATMapping.getFileMappings(conn, dropdownlistItem);

			int org = SessionData.getOrg_Uid(_req, _cons.SS);
			// can not find configuration for this year or type
			if (vmap.isEmpty() && subMap.isEmpty() && codeMap.isEmpty()) {
				msg.addbuf("Current system doesn't support this file type");
				msg.addbuf("");
				datImport.logHistory(conn, org, tname, rollbackname, _cons.AUDIT.m_emailname, msg, dryrun,
						DATES.sysDate(DATES.stdtime), DATES.sysDate(DATES.stdtime));
				return;
			}

			fname = SessionData.getUploadFilename(_cons.SS);
			if (org > 0 && !fname.equals("") && (FILE.fileExists(fname, true))) {
				df = new DataFileDAT();
				if (df != null) {
					if (df.openFileHelper(fname, vmap, reasonMap, subMap, codeMap, lengthMap)) {
						// Saves all new data
						List<String> dataList = new LinkedList<>();

						// Saves corresponding column names in production table
						List<String> nameList = new LinkedList<>();

						// saves all the iepsed_student_id
						List<String> sidList = new LinkedList<>();

						// Saves all the unknown id
						List<String> unknownSidList = new LinkedList<>();

						// Saves all unprocessed reason
						Map<Integer, Map<String, String>> noInformationRecordsMap = new HashMap<>();

						// Saves unprocessed reason for this line
						Map<String, String> noInformationRecords = new HashMap<>();

						// Saves all the new data for corresponding
						// iepsed_student_id
						List<Vector<DBitem>> vParamsList = new LinkedList<>();

						while (df.nextLine(tname, tyear, lineCounter, dataList, nameList, noInformationRecords, msg)) {
							lineCounter[0]++;
							// Wrong Data found
							if (!msg.getbuf().isEmpty()) {
								datImport.logHistory(conn, org, tname, rollbackname, _cons.AUDIT.m_emailname, msg,
										dryrun, DATES.sysDate(DATES.stdtime), DATES.sysDate(DATES.stdtime));
								deleteFile(fname);
								return;
							} else {
								importHelper(_cons, table, org, dryrun, err, conn, dataList, nameList, vParamsList,
										sidList, unknownSidList, noInformationRecords, noInformationRecordsMap);
							}
						}
						Map<String, List<Vector<DBitem>>> updateRecords = new HashMap<>();
						dataHelper(updateRecords, sidList, vParamsList);
						datImport.executeUpdate(conn, _cons.AUDIT, org, unknownSidList, noInformationRecordsMap,
								updateRecords, table, tname, dryrun, rollbackname);
					} else {
						msg.addbuf("Uploaded file no longer exists.");
					}
					msg.addbuf("Update completed on ", DATES.sysDate(DATES.stdhuman));
				}
			} else {
				msg.addbuf("Unable to process the uploaded file. Please reupload the file");
				msg.addbuf("");
				datImport.logHistory(conn, org, tname, rollbackname, _cons.AUDIT.m_emailname, msg, dryrun,
						DATES.sysDate(DATES.stdtime), DATES.sysDate(DATES.stdtime));
				Logs.error(methodname, "Unable to process the uploaded file (" + fname + ")");
			}
		} catch (Exception e) {
			Logs.logMsg(methodname, e.getMessage(), Logs.ERROR);
		} finally {
			if (df != null) {
				df.closeFile();
			}
		}
		if (!dryrun)
			deleteFile(fname);
		SessionData.putServletError(_req, _cons.SS, err.getbuf());
	}

	/*
	 * Process data
	 * 
	 * @param fileType: uploaded file
	 * 
	 * @param table: production table
	 * 
	 * @param dryrun: dry run
	 * 
	 * @param dataList: List saves all new data
	 * 
	 * @param nameList: List saves corresponding column names in production
	 * table
	 * 
	 * @param vParamsList: List saves all the new data for corresponding
	 * iepsed_student_id
	 * 
	 * @param sidList: List saves all the iepsed_student_id
	 * 
	 * @param unknownSidList: List saves all the unknown id
	 * 
	 * @param noInformationRecords: Map saves unprocessed reason for this line
	 * 
	 * @param noInformationRecordsMap: Map saves all unprocessed reason
	 */
	private void importHelper(AppVars _cons, String table, int org, boolean dryrun, ERR err, Connection conn,
			List<String> dataList, List<String> nameList, List<Vector<DBitem>> vParamsList, List<String> sidList,
			List<String> unknownSidList, Map<String, String> noInformationRecords,
			Map<Integer, Map<String, String>> noInformationRecordsMap) {
		String tstateid = dataList.get(0);
		StudentVars SV = StudentVars.getStudentById(conn, org, "tstateid", tstateid, "");
		if (SV.sid > 0 && SV.org > 0) {
			if (!noInformationRecordsMap.containsKey(SV.sid)) {
				noInformationRecordsMap.put(SV.sid, new HashMap<>());
			}
			for (Map.Entry<String, String> entry : noInformationRecords.entrySet()) {
				noInformationRecordsMap.get(SV.sid).put(entry.getKey(), entry.getValue());
			}
			int i = 1;
			while (i < dataList.size()) {
				sidList.add(SV.sid + "");
				Vector<DBitem> vParams = new Vector<DBitem>();
				for (int j = 0; j < nameList.size(); j++) {
					if (nameList.get(j).equals("TYEAR") || nameList.get(j).equals("ITESTYEAR")) {
						vParams.add(new DBitem(table, nameList.get(j), "20" + dataList.get(i++)));
					} else {
						vParams.add(new DBitem(table, nameList.get(j), dataList.get(i++)));
					}
				}
				vParamsList.add(vParams);
			}
		} else {
			unknownSidList.add(tstateid);
		}
	}

	/*
	 * Save all sid and corresponding updated data to a single Map
	 * 
	 * @param updateRecords: Map which saves all the students' records to be
	 * processed
	 * 
	 * @param sidList: List saves all the iepsed_student_id
	 * 
	 * @param vParamsList: List saves all the new data for corresponding
	 * iepsed_student_id
	 * 
	 */
	private void dataHelper(Map<String, List<Vector<DBitem>>> updateRecords, List<String> sidList,
			List<Vector<DBitem>> vParamsList) {
		for (int i = 0; i < sidList.size(); i++) {
			if (!updateRecords.containsKey(sidList.get(i))) {
				updateRecords.put(sidList.get(i), new LinkedList<>());
			}
			updateRecords.get(sidList.get(i)).add(vParamsList.get(i));
		}
	}

	private void deleteFile(String fname) {
		try {
			if (!fname.equals("")) {
				FILE.deleteFile(fname);
			}
		} catch (Exception e) {
			Logs.error("Error deleting file: " + fname, e);
		}
	}
}