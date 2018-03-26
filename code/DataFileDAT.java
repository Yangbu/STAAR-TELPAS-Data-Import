package com.esped.lib.file;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import com.esped.lib.*;
import com.esped.ops.US.DATMapping;

public class DataFileDAT {

	private static final Set<String> subjectSet = new HashSet<>(
			Arrays.asList("Reading", "Math", "Writing", "Social Studies", "Science"));
	private static Map<String, Set<String>> reasonDataMap = new HashMap<>();
	private static Map<String, String> fieldsMap = new HashMap<>();
	private static Map<String, List<String>> subjectMap = new HashMap<>();
	private static Set<String> subFieldSet = new HashSet<>();
	private static Set<String> nameSet = new HashSet<>();
	private FileInputStream fis = null;
	private InputStreamReader isr = null;
	private BufferedReader istream = null;
	private static String m_filename = "";
	private static Vector<DATMapping> vmap = new Vector<>();
	private static Vector<DATMapping> reasonMap = new Vector<>(); // Saves
																	// unprocessed
																	// fields'
																	// information
	private static Vector<DATMapping> subMap = new Vector<>();
	private static Vector<DATMapping> codeMap = new Vector<>();
	private static Vector<DATMapping> lengthMap = new Vector<>();
	private static boolean ignoreFlag = false;

	public boolean openFileHelper(String fname, Vector<DATMapping> vmap, Vector<DATMapping> reasonMap,
			Vector<DATMapping> subMap, Vector<DATMapping> codeMap, Vector<DATMapping> lengthMap) {
		DataFileDAT.vmap = vmap;
		DataFileDAT.reasonMap = reasonMap;
		DataFileDAT.subMap = subMap;
		DataFileDAT.codeMap = codeMap;
		DataFileDAT.lengthMap = lengthMap;
		return openFile(fname);
	}

	public boolean openFileHelper(String fname, Vector<DATMapping> vmap) {
		DataFileDAT.vmap = vmap;
		return openFile(fname);
	}

	public boolean openFile(String fname) {
		boolean sts = false;
		if (fname != null && !fname.equals("")) {
			m_filename = fname;
			try {
				fis = new FileInputStream(m_filename);
				isr = new InputStreamReader(fis);
				istream = new BufferedReader(isr);
				sts = true;
			} catch (Exception e) {
				Logs.error("DataFileDAT.openFile", e);
			}
		}
		return (sts);
	}

	public boolean nextLine(String tname, String tyear, int[] lineCounter, List<String> dataList, List<String> nameList,
			Map<String, String> noInformationRecords, ERR msg) {
		boolean sts = false;
		if (dataList != null && nameList != null && istream != null) {
			dataList.clear();
			nameList.clear();
			String sLine = "";
			try {
				sLine = istream.readLine();
				if (sLine != null) {
					parseTXTLine(sLine, tname, tyear, lineCounter, dataList, nameList, noInformationRecords, msg);
					sts = true;
				}
			} catch (Exception e) {
				Logs.error("DataFileDAT.nextLine", e);
			}
		}
		return (sts);
	}

	public void closeFile() {
		try {
			if (istream != null) {
				istream.close();
			}
			istream = null;
		} catch (Exception e) {
			Logs.error("DataFileDAT.closeFile - istream", e);
		}
		try {
			if (isr != null) {
				isr.close();
			}
			isr = null;
		} catch (Exception e) {
			Logs.error("DataFileDAT.closeFile - isr", e);
		}
		try {
			if (fis != null) {
				fis.close();
			}
			fis = null;
		} catch (Exception e) {
			Logs.error("DataFileDAT.closeFile - fis", e);
		}
	}

	/*
	 * Get data from file line by line
	 * 
	 * @param sLine: one line record from file
	 * 
	 * @param tname: file type
	 * 
	 * @param lineCounter: line counter
	 * 
	 * @param dataList: List saves all new data
	 * 
	 * @param nameList: List saves corresponding column names in production
	 * table
	 * 
	 * @param noInformationRecords: Map saves unprocessed reason for this line
	 */
	public static void parseTXTLine(String sLine, String tname, String tyear, int[] lineCounter, List<String> dataList,
			List<String> nameList, Map<String, String> noInformationRecords, ERR msg) {
		subFieldSet.clear();
		reasonDataMap.clear();
		fieldsMap.clear();
		subjectMap.clear();
		nameSet.clear();
		noInformationRecords.clear();
		if (sLine.length() > 0) {
			sLine = sLine.trim();

			// Check first line
			if (lineCounter[0] == 0) {
				// Type checking
				if (!administrationDateChecker(sLine, tname, tyear, subMap)) {
					msg.addbuf("The Administration year doesn't match. Please choose the correct file");
					return;
				}
			}
			// Line Length checking for each line
			if (!lineLengthChecker(sLine)) {
				msg.addbuf("Line " + (lineCounter[0] + 1) + " : Length doesn't match TEA's record");
				return;
			}

			String studentId = getStudentId(sLine, subMap);
			// Type 1 : No indicator, Always Import data
			if (vmap.size() == 0) {
				// Get unprocessed reason if there is for this field
				getUnprocessedReason(sLine, reasonMap, noInformationRecords, msg, lineCounter);
				for (DATMapping subdm : subMap) {
					// If reasonDataMap doesn't contain this field, then process
					if (!reasonDataMap.containsKey(subdm.tsubject)
							|| !reasonDataMap.get(subdm.tsubject).contains(subdm.tnamedmap)) {
						if (subdm.timportindicator.equals("")) {
							importData(sLine, subdm, dataList, nameList, noInformationRecords, msg, lineCounter);
						}
					}
				}
			}
			// Type 2 : Has indicator
			else {
				// Get unprocessed reason if there is for this record
				getUnprocessedReason(sLine, reasonMap, noInformationRecords, msg, lineCounter);
				for (DATMapping dm : vmap) {
					// Has the value of processing indicator, Import record
					if (dm.tindicator.equals(sLine.substring(dm.istart, dm.iend).trim())) {
						ignoreFlag = false;

						// Check if has the value of "non process" for this
						// field
						for (DATMapping cdm : codeMap) {
							if (cdm.tnamedmap.equals(dm.tnamedmap)
									&& cdm.trawdata.equals(sLine.substring(dm.istart, dm.iend).trim())) {
								ignoreFlag = true;

								// Get subject name
								String subjectName = "";
								if (!subjectSet.contains(cdm.tnamedmap)) {
									subjectName = getSubjectName(sLine, subMap, codeMap);
								}
								// Save the unprocessed record and corresponding
								// reason
								noInformationRecords.put(subjectName.equals("") ? cdm.tnamedmap : subjectName,
										cdm.tmappeddata);
							}
						}

						if (ignoreFlag)
							continue;
						// several entries found
						if (subjectSet.contains(dm.tnamedmap)) {
							dataList.add(dm.tnamedmap);
							if (!nameSet.contains("LSUBJECT")) {
								nameList.add("LSUBJECT");
								nameSet.add("LSUBJECT");
							}
						}
						for (DATMapping subdm : subMap) {
							if (dm.tindicator.equals(subdm.timportindicator)) {
								if (subdm.tsubject.equals("") || dm.tnamedmap.equals(subdm.tsubject)) {
									// If reasonDataMap doesn't contain this
									// field, then process
									if (!reasonDataMap.containsKey(subdm.tsubject)
											|| !reasonDataMap.get(subdm.tsubject).contains(subdm.tnamedmap)) {
										importData(sLine, subdm, dataList, nameList, noInformationRecords, msg,
												lineCounter);
									}
								}
							}
						}
					}
				}
			}
			dataList.add(0, studentId);
		}
	}

	/*
	 * Get reason that can not be processed for this filed
	 * 
	 * @param sLine: one line record from file
	 * 
	 * @param reasonMap: Map saves all reasons from Table
	 * REF_DATAIMPORT_SUBJECT_MAPPING
	 * 
	 * @param noInformationRecords: Map saves unprocessed reason for this line
	 */
	private static void getUnprocessedReason(String sLine, Vector<DATMapping> reasonMap,
			Map<String, String> noInformationRecords, ERR msg, int[] lineCounter) {
		for (DATMapping rdm : reasonMap) {
			String valueInFile = sLine.substring(rdm.istart, rdm.iend);
			if (!valueInFile.equals(rdm.timportindicator)) {
				if (!reasonDataMap.containsKey(rdm.tsubject)) {
					reasonDataMap.put(rdm.tsubject, new HashSet<>());
				}
				reasonDataMap.get(rdm.tsubject).add(rdm.tnamedmap);
				boolean errFlag = true;
				for (DATMapping cdm : codeMap) {
					String field = rdm.tnamedmap;
					if (cdm.tnamedmap.equals(field) && cdm.tmappingtype.equals("REASON")
							&& valueInFile.equals(cdm.trawdata)) {
						errFlag = false;
						// Save the unprocessed record and corresponding reason
						noInformationRecords.put(field, cdm.tmappeddata);
						break;
					}
				}
				if (errFlag) {
					msg.addbuf("Line " + (lineCounter[0] + 1) + ": Wrong Data found [" + valueInFile
							+ "], Location from [" + rdm.istart + "] to [" + rdm.iend + "]");
					return;
				}
			}
		}
	}

	/*
	 * Import data
	 * 
	 * @param sLine: one line record from file
	 * 
	 * @param subdm: a field in this line
	 * 
	 * @param dataList: List saves all new data
	 * 
	 * @param nameList: List saves corresponding column names in production
	 * table
	 * 
	 * @param noInformationRecords: Map saves unprocessed reason for this line
	 */
	private static void importData(String sLine, DATMapping subdm, List<String> dataList, List<String> nameList,
			Map<String, String> noInformationRecords, ERR msg, int[] lineCounter) {
		// Import default value
		if (!subdm.tdefaultvalue.equals("")) {
			importDefault(dataList, subdm, nameList);
		}
		// Import original value
		else if (subdm.tmappingindicator.equals("")) {
			importOriginal(sLine, dataList, subdm, nameList);
		}
		// Import mapping value
		else {
			String field = subdm.tnamedmap;
			boolean errFlag = true;
			for (DATMapping cdm : codeMap) {
				if (cdm.tnamedmap.equals(field) && sLine.substring(subdm.istart, subdm.iend).equals(cdm.trawdata)
						&& cdm.tmappingtype.equals("FIELD")) {
					errFlag = false;
					dataList.add(cdm.tmappeddata);
					if (!nameSet.contains(field)) {
						nameList.add(field);
						nameSet.add(field);
					}
					break;
				}
			}
			if (errFlag) {
				msg.addbuf("Line " + (lineCounter[0] + 1) + ": Wrong Data found ["
						+ sLine.substring(subdm.istart, subdm.iend) + "], Location from [" + subdm.istart + "] to ["
						+ subdm.iend + "]");
				return;
			}
		}
	}

	/*
	 * Import default data
	 * 
	 * @param dataList: List saves all new data
	 * 
	 * @param subdm: a field in this line
	 * 
	 * @param nameList: List saves corresponding column names in production
	 * table
	 */
	private static void importDefault(List<String> dataList, DATMapping subdm, List<String> nameList) {
		if (!subdm.tdefaultvalue.equals("")) {
			dataList.add(subdm.tdefaultvalue);
			if (!nameSet.contains(subdm.tnamedmap)) {
				nameList.add(subdm.tnamedmap);
				nameSet.add(subdm.tnamedmap);
			}
		}
	}

	/*
	 * Import original data
	 * 
	 * @param sLine: one line record from file
	 * 
	 * @param dataList: List saves all new data
	 * 
	 * @param subdm: a field in this line
	 * 
	 * @param nameList: List saves corresponding column names in production
	 * table
	 */
	private static void importOriginal(String sLine, List<String> dataList, DATMapping subdm, List<String> nameList) {
		if (!subdm.tnamedmap.equals("TSTATEID")) {
			dataList.add(sLine.substring(subdm.istart, subdm.iend));
			if (!nameSet.contains(subdm.tnamedmap)) {
				nameList.add(subdm.tnamedmap);
				nameSet.add(subdm.tnamedmap);
			}
		}
	}

	/*
	 * Check whether administration date in dropdownlist matches the file
	 * 
	 * @param sLine: one line record from file
	 * 
	 * @param tname: file type
	 * 
	 * @param tname: file year
	 * 
	 * @param subMap: Map saves all field records from Table
	 * REF_DATAIMPORT_SUBJECT_MAPPING
	 */
	private static boolean administrationDateChecker(String sLine, String tname, String tyear,
			Vector<DATMapping> subMap) {
		for (DATMapping subdm : subMap) {
			if (subdm.tname.equals(tname) && subdm.tnamedmap.equals("ITESTYEAR")) {
				if (!sLine.substring(subdm.istart, subdm.iend).equals(tyear.substring(2))) {
					return false;
				}
			}
		}
		return true;
	}

	/*
	 * Check whether the length of line in file matches TEA's records
	 * 
	 * @param sLine: one line record from file
	 */
	private static boolean lineLengthChecker(String sLine) {
		if (sLine.length() != lengthMap.get(0).ilinelength) {
			return false;
		}
		return true;
	}

	/*
	 * Get subject name
	 * 
	 * @param sLine: one line record from file
	 * 
	 * @param subMap: All field records from Table
	 * REF_DATAIMPORT_SUBJECT_MAPPING
	 * 
	 * @param codeMap: All records from Table REF_DATAIMPORT_CODE
	 * 
	 */
	private static String getSubjectName(String sLine, Vector<DATMapping> subMap, Vector<DATMapping> codeMap) {
		String subjectName = "";
		for (DATMapping subdm : subMap) {
			if (subdm.tnamedmap.equals("LSUBJECT")) {
				if (!subdm.tdefaultvalue.equals("")) {
					subjectName = subdm.tdefaultvalue;
					break;
				} else if (subdm.tmappingindicator.equals("")) {
					subjectName = sLine.substring(subdm.istart, subdm.iend);
					break;
				} else {
					for (DATMapping cdm : codeMap) {
						if (cdm.tnamedmap.equals("LSUBJECT")
								&& sLine.substring(subdm.istart, subdm.iend).equals(cdm.trawdata)) {
							subjectName = cdm.tmappeddata;
							break;
						}
					}
				}
			}
		}
		return subjectName;
	}

	/*
	 * Get student id
	 * 
	 * @param sLine: one line record from file
	 * 
	 * @param subMap: Map saves all field records from Table
	 * REF_DATAIMPORT_SUBJECT_MAPPING
	 * 
	 */
	private static String getStudentId(String sLine, Vector<DATMapping> subMap) {
		String studentId = "";
		for (DATMapping subdm : subMap) {
			if (subdm.tnamedmap.equals("TSTATEID")) {
				studentId = sLine.substring(subdm.istart, subdm.iend);
				break;
			}
		}
		return studentId;
	}
}