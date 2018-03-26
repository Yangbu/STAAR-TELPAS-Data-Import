package com.esped.ops.TX;

import com.esped.lib.AuditVars;
import com.esped.lib.DATES;
import com.esped.lib.DBitem;
import com.esped.lib.DBvars;
import com.esped.lib.ERR;
import com.esped.lib.Logs;
import com.esped.lib.SAV;
import com.esped.lib.SQL;
import com.esped.lib.STR;
import com.esped.lib.StudentVars;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

public class DatImportSAV {

	private static boolean duplicatedIndicator = false;
	private static boolean updateIndicator = false;
	private static String dupSubject = "";
	private static String dupYear = "";
	private static String dupMonth = "";
	public static final int WHERE_ORGSIDSUBJECT = 60;

	public static void auditPreviousValuesForDat(Connection conn, AuditVars audit, String cTable, int org, int sid,
			String gsid, int iunique, Vector<DBitem> vParams, StudentVars SV, String rollbacktable, String rollbackname,
			boolean dryrun, boolean showsid, ERR msglog) {
		auditPreviousValuesForDat(conn, audit, cTable, org, sid, gsid, iunique, vParams, SV.firstname, SV.lastname,
				SV.StudentType, rollbacktable, rollbackname, dryrun, showsid, msglog);
	}

	public static void auditPreviousValuesForDat(Connection conn, AuditVars audit, String cTable, int org, int sid,
			String gsid, int iunique, Vector<DBitem> vParams, String firstname, String lastname, String studenttype,
			String rollbacktable, String rollbackname, boolean dryrun, boolean showsid, ERR msglog) {
		String methodname = "auditPreviousValuesForDat";
		String sql = "";
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		int pcnt = 0;
		int i = 0;
		try {
			cTable = cTable.toUpperCase();
			sql = "select * from " + cTable;

			// set up the where clause
			// always has org
			sql = sql + " where iorg_uid=?";
			// see if there's an iunique
			if (iunique > 0) {
				sql = sql + " and iunique=?";
			}
			// see if there's a sid
			if (sid > 0) {
				sql = sql + " and iesped_student_id=?";
			}

			pstmt = conn.prepareStatement(sql);
			pcnt = 1;
			// set the where clause variables
			// always has org
			pstmt.setInt(pcnt++, org);
			// see if there's an iunique
			if (iunique > 0) {
				pstmt.setInt(pcnt++, iunique);
			}
			// always has sid
			pstmt.setInt(pcnt++, sid);

			rset = pstmt.executeQuery();

			duplicatedIndicator = false;
			updateIndicator = false;

			while (rset.next()) {
				duplicatedIndicator = false;
				updateIndicator = false;
				dupYear = "";
				dupMonth = "";
				checkcriterionParams(rset, vParams, cTable);

				// duplicated record
				if (duplicatedIndicator) {
					if (msglog != null) {
						String sidstr = "";
						if (showsid) {
							sidstr = " for sid=" + String.valueOf(sid);
						}
						msglog.addbuf("");
						// STAAR
						if (cTable.equals("REVIEW_STAAR_TESTS")) {
							msglog.addbuf("Duplicated record [" + dupSubject + "]", sidstr);
						}
						// TELPAS
						else {
							msglog.addbuf("Duplicated record", sidstr);
						}
						msglog.addbuf("Administration Date: " + dupYear + " " + dupMonth);
					}
					break;
				}
				// update current record
				else if (updateIndicator) {
					updateCurrent(conn, audit, cTable, org, sid, gsid, iunique, vParams, firstname, lastname,
							studenttype, rollbacktable, rollbackname, dryrun, showsid, msglog, rset, i);
					break;
				}
			}
			// add new record
			if (!duplicatedIndicator && !updateIndicator) {
				addNewRecord(conn, audit, cTable, org, sid, gsid, iunique, vParams, firstname, lastname, studenttype,
						rollbacktable, rollbackname, dryrun, showsid, msglog, i);
			}

		} catch (Exception e) {
			Logs.error(methodname + " exception: " + e.getMessage());
		} finally {
			rset = SQL.clearResultSet(rset);
			pstmt = SQL.clearPreparedStatement(pstmt);
		}
	}

	private static void updateCurrent(Connection conn, AuditVars audit, String cTable, int org, int sid, String gsid,
			int iunique, Vector<DBitem> vParams, String firstname, String lastname, String studenttype,
			String rollbacktable, String rollbackname, boolean dryrun, boolean showsid, ERR msglog, ResultSet rset,
			int i) {
		String methodname = "updateCurrent";
		try {
			if (msglog != null) {
				String sidstr = "";
				if (showsid) {
					sidstr = " for sid=" + String.valueOf(sid);
				}
				msglog.addbuf("");
				// STAAR
				if (cTable.equals("REVIEW_STAAR_TESTS")) {
					msglog.addbuf("Update current record [" + dupSubject + "]", sidstr);
				}
				// TELPAS
				else {
					msglog.addbuf("Update current record", sidstr);
				}
			}
			for (i = 0; i < vParams.size(); i++) {
				DBitem dbi = vParams.elementAt(i);
				if (dbi.table != null && dbi.column != null && !dbi.column.equals("")
						&& dbi.table.equalsIgnoreCase(cTable)) {
					if (dbi.type.equalsIgnoreCase("e") || dbi.type.equalsIgnoreCase("f")
							|| dbi.type.equalsIgnoreCase("x")) {
						// skip these
					} else {
						String oldval = "";
						if (dbi.type.equalsIgnoreCase("d")) {
							oldval = DATES.convertDate(rset.getDate(dbi.column), null);
						} else {
							oldval = STR.cn(rset.getString(dbi.column));
						}

						if (!oldval.equals(dbi.value)) {
							if (msglog != null) {
								msglog.addbuf("Changing ", dbi.table, "$", dbi.column, " from [", oldval, "] to [",
										dbi.value, "]");
							}
							if (!dryrun) {
								audit.auditChange(audit.m_auditconn, org, sid, firstname, lastname, studenttype,
										dbi.table, dbi.column, audit.m_emailname, oldval, dbi.value);
								SAV.insertRollbackEntry(conn, rollbacktable, rollbackname, org, sid, "", dbi.table,
										dbi.column, oldval, dbi.value, audit.m_emailname, firstname, lastname);
							}
						}
					}
				}
			}
			if (!dryrun) {
				saveRecord(conn, audit, null, cTable, WHERE_ORGSIDSUBJECT, org, sid, gsid, iunique, vParams, false,
						false, false);
			}
		} catch (Exception e) {
			Logs.error(methodname + " exception: " + e.getMessage());
		}
	}

	private static void addNewRecord(Connection conn, AuditVars audit, String cTable, int org, int sid, String gsid,
			int iunique, Vector<DBitem> vParams, String firstname, String lastname, String studenttype,
			String rollbacktable, String rollbackname, boolean dryrun, boolean showsid, ERR msglog, int i) {
		if (msglog != null) {
			String sidstr = "";
			if (showsid) {
				sidstr = " for sid=" + String.valueOf(sid);
			}
			msglog.addbuf("");
			// STAAR
			if (cTable.equals("REVIEW_STAAR_TESTS")) {
				msglog.addbuf("Add new record [" + getSubject(vParams) + "]", sidstr);
			}
			// TELPAS
			else {
				msglog.addbuf("Add new record", sidstr);
			}
		}
		for (i = 0; i < vParams.size(); i++) {
			DBitem dbi = vParams.elementAt(i);
			if (dbi.table != null && dbi.column != null && !dbi.column.equals("")
					&& dbi.table.toUpperCase().equals(cTable)) {
				if (dbi.type.equalsIgnoreCase("e") || dbi.type.equalsIgnoreCase("f")
						|| dbi.type.equalsIgnoreCase("x")) {
					// skip these
				} else {
					if (msglog != null) {
						msglog.addbuf("Setting ", dbi.table, "$", dbi.column, " to [", dbi.value, "]");
					}
					if (!dryrun) {
						audit.auditChange(audit.m_auditconn, org, sid, firstname, lastname, studenttype, dbi.table,
								dbi.column, audit.m_emailname, "", dbi.value);
						SAV.insertRollbackEntry(conn, rollbacktable, rollbackname, org, sid, "", dbi.table, dbi.column,
								"", dbi.value, audit.m_emailname, firstname, lastname);
					}
				}
			}
		}
		if (!dryrun) {
			SAV.insertRecord(conn, audit, null, cTable, SAV.WHERE_ORGSID, org, sid, gsid, vParams, 0);
		}
	}

	/*
	 * Get subject name
	 */
	private static String getSubject(Vector<DBitem> vParams) {
		String sub = "";
		for (DBitem db : vParams) {
			if (db.column.equals("LSUBJECT")) {
				sub = db.value;
				break;
			}
		}
		return sub;
	}

	/*
	 * Check to determine if the record is a 'duplicated' or an 'update': A
	 * 'duplicated' is defined as two records with the same student, same test,
	 * same year, same subject, same window, same scale score An 'update' record
	 * is defined as two records with the same student, same test, same year,
	 * same subject, same window, different score or different score code
	 * 
	 */
	private static void checkcriterionParams(ResultSet rset, Vector<DBitem> vParams, String cTable) {
		if (cTable.equals("REVIEW_STAAR_TESTS")) {
			Set<String> criterionSet = new HashSet<>();
			criterionSet.addAll(Arrays.asList("LTEST", "TYEAR", "ITESTMONTH", "LSUBJECT"));
			checkSTAAR(rset, vParams, cTable, criterionSet);
		} else {
			Set<String> criterionSet = new HashSet<>();
			criterionSet.addAll(Arrays.asList("TYEAR", "ITESTMONTH"));
			checkTELPAS(rset, vParams, cTable, criterionSet);
		}
	}

	private static void checkSTAAR(ResultSet rset, Vector<DBitem> vParams, String cTable, Set<String> criterionSet) {
		String oldScale = "";
		String newScale = "";
		String methodname = "checkSTAAR";
		dupSubject = "";
		try {
			for (DBitem dbi : vParams) {
				if (criterionSet.contains(dbi.column)) {
					if (dbi.table != null && dbi.column != null && !dbi.column.equals("")
							&& dbi.table.toUpperCase().equals(cTable)) {
						if (dbi.type.equalsIgnoreCase("e") || dbi.type.equalsIgnoreCase("f")
								|| dbi.type.equalsIgnoreCase("x")) {
							// skip these
						} else {
							if (dbi.column.equals("LSUBJECT"))
								dupSubject = dbi.value;
							if (dbi.column.equals("TYEAR"))
								dupYear = dbi.value;
							if (dbi.column.equals("ITESTMONTH"))
								dupMonth = dbi.value;
							String oldval = "";
							if (dbi.type.equalsIgnoreCase("d")) {
								oldval = DATES.convertDate(rset.getDate(dbi.column), null);
							} else {
								oldval = STR.cn(rset.getString(dbi.column));
							}
							if (!oldval.equals(dbi.value)) {
								dupYear = "";
								dupMonth = "";
								return;
							}

						}
					}
				} else if (dbi.column.equals("TSCALE_CODE")) {
					oldScale = STR.cn(rset.getString("TSCALE_CODE"));
					newScale = dbi.value;
				}
			}
			if (oldScale.equals(newScale)) {
				duplicatedIndicator = true;
			} else {
				updateIndicator = true;
			}

		} catch (Exception e) {
			Logs.error(methodname + " exception: " + e.getMessage());
		}
	}

	private static void checkTELPAS(ResultSet rset, Vector<DBitem> vParams, String cTable, Set<String> criterionSet) {
		String methodname = "checkTELPAS";
		try {
			for (DBitem dbi : vParams) {
				if (criterionSet.contains(dbi.column)) {
					if (dbi.table != null && dbi.column != null && !dbi.column.equals("")
							&& dbi.table.toUpperCase().equals(cTable)) {
						if (dbi.type.equalsIgnoreCase("e") || dbi.type.equalsIgnoreCase("f")
								|| dbi.type.equalsIgnoreCase("x")) {
							// skip these
						} else {
							if (dbi.column.equals("TYEAR"))
								dupYear = dbi.value;
							if (dbi.column.equals("ITESTMONTH"))
								dupMonth = dbi.value;
							String oldval = "";
							if (dbi.type.equalsIgnoreCase("d")) {
								oldval = DATES.convertDate(rset.getDate(dbi.column), null);
							} else {
								oldval = STR.cn(rset.getString(dbi.column));
							}
							if (!oldval.equals(dbi.value)) {
								dupYear = "";
								dupMonth = "";
								return;
							}
						}
					}
				}
			}
			duplicatedIndicator = true;

		} catch (Exception e) {
			Logs.error(methodname + " exception: " + e.getMessage());
		}
	}

	public static String saveRecord(Connection conn, AuditVars audit, ERR err, String cTable, int wheretype, int org,
			int sid, String gsid, int iunique, Vector<DBitem> vParams, boolean insertnew, boolean auditchange,
			boolean whenchange) {
		String methodname = "saveRecord";
		String ret = "";
		String sql = "";
		String auditstr = "";
		DBvars DBV = new DBvars(conn);
		PreparedStatement pstmt = null;
		int pcnt = 0;
		int ucnt = 0;
		String sep = " ";
		int i = 0;
		boolean doupdate = false;

		if (err == null) {
			err = new ERR();
		}
		// make sure only one record will get updated
		int savecount = checkSaveCount(conn, cTable, wheretype, org, sid, gsid, iunique);
		if (savecount != 1) {
			Logs.error(methodname + ": invalid save count",
					"table=" + cTable + ", org=" + org + ", sid=" + sid + ", gsid=" + gsid + ", iunique=" + iunique);
			return ("");
		}

		try {
			cTable = cTable.toUpperCase();
			sql = "update " + cTable + " set";
			auditstr = "update " + cTable + " set";

			for (i = 0; i < vParams.size(); i++) {
				DBitem dbi = vParams.elementAt(i);
				if (dbi.table != null && dbi.column != null && !dbi.column.equals("")
						&& dbi.table.toUpperCase().equals(cTable)) {
					if (// dbi.type.equalsIgnoreCase("e")|| GR 29050
					dbi.type.equalsIgnoreCase("f") || dbi.type.equalsIgnoreCase("x")) {
						// skip these
					} else {
						doupdate = true;
						dbi.value = STR.cn(dbi.value);
						String v = dbi.value;
						if (dbi.type.equalsIgnoreCase("w")) {
							// see if the value is a well-formatted timestamp
							if (dbi.value.equalsIgnoreCase("sysdate")
									|| DATES.parseTimestamp(dbi.value, DATES.stdtime) == null) {
								dbi.value = "sysdate";
								sql = sql + sep + dbi.column + "=" + DBV.getSysdateFunction();
							} else {
								sql = sql + sep + dbi.column + "=?";
							}
						} else if (dbi.type.equalsIgnoreCase("d") && dbi.value.equalsIgnoreCase("sysdate")) {
							sql = sql + sep + dbi.column + "=" + DBV.getSysdateFunction();
						} else {
							if (dbi.value.equals("")) {
								sql = sql + sep + dbi.column + "=null";
								v = "null";
							} else {
								sql = sql + sep + dbi.column + "=?";
								v = "'" + v + "'";
							}
						}
						auditstr = auditstr + sep + dbi.column + "=" + v;
						sep = ",";
					}
				}
			}

			String wClause = "WHERE iorg_uid=%ORGUID% and iesped_student_id=%SID% and lsubject=%SUBJECT% ";
			sql = sql + " " + wClause;
			sql = STR.replaceString(sql, "%SID%", "?");
			sql = STR.replaceString(sql, "%ORGUID%", "?");
			sql = STR.replaceString(sql, "%SUBJECT%", "?");
			auditstr = auditstr + " " + wClause;
			auditstr = STR.replaceString(auditstr, "%SID%", String.valueOf(sid));
			auditstr = STR.replaceString(auditstr, "%ORGUID%", String.valueOf(org));
			auditstr = STR.replaceString(auditstr, "%SUBJECT%", dupSubject);
			if (iunique > 0) {
				sql = sql + " AND iunique=?";
				auditstr = auditstr + " AND iunique=" + iunique;
			}

			if (doupdate) {
				// auto audit change
				if (auditchange) {
					SAV.auditPreviousValues(conn, audit, cTable, org, sid, iunique, vParams, false, "", "");
				}
				pstmt = conn.prepareStatement(sql);
				pcnt = 1;

				for (i = 0; i < vParams.size(); i++) {
					DBitem dbi = vParams.elementAt(i);
					if (dbi.table != null && dbi.column != null && !dbi.column.equals("")
							&& dbi.table.toUpperCase().equals(cTable)) {
						if (// dbi.type.equalsIgnoreCase("e") || GR 29050
						dbi.type.equalsIgnoreCase("f") || dbi.type.equalsIgnoreCase("x")) {
							// skip these
						} else {
							if (!dbi.value.equals("")) {
								if (dbi.type.equalsIgnoreCase("w")) {
									// convert to Timestamp
									if (!dbi.value.equalsIgnoreCase("sysdate")) {
										Timestamp xdate = DATES.parseTimestamp(dbi.value, DATES.stdtime);
										pstmt.setTimestamp(pcnt++, xdate);
									}
								} else if (dbi.type.equalsIgnoreCase("d")) {
									// convert to Date
									if (!dbi.value.equalsIgnoreCase("sysdate")) {
										Date xdate = DATES.parseDateString(dbi.value);
										pstmt.setDate(pcnt++, xdate);
									}
								} else if (dbi.type.equalsIgnoreCase("i")) {
									// convert to int
									// PROBLEM: some $i are actually floating
									// point numbers
									// so just do a setString for now and hope
									// int xint = Integer.parseInt(dbi.value);
									// pstmt.setInt(pcnt++, xint);
									pstmt.setString(pcnt++, dbi.value);
								} else {
									pstmt.setString(pcnt++, dbi.value);
								}
							}
						}
					}
				}
				pstmt.setInt(pcnt++, org);
				pstmt.setInt(pcnt++, sid);
				pstmt.setString(pcnt++, dupSubject);
				// add the iunique
				if (iunique > 0) {
					pstmt.setInt(pcnt++, iunique);
				}

				Logs.logMsg("saveRecord Statement ", sql, Logs.SQL);
				ucnt = pstmt.executeUpdate();
				Logs.logMsg("saveRecord Statement ", ucnt + " Rows Updated", Logs.SQL);
				if (audit != null) {
					audit.auditLog(methodname, ucnt + " Rows Updated", auditstr);
				}
				// record-level when and who changed
				SAV.updateRecordChanged(conn, audit, cTable, org, sid, iunique);
			} else {
				if (Logs.isDebugEnabled())
					Logs.debug(methodname + ": no matching columns to update");
			}
		} catch (Exception e) {
			Logs.error(methodname + " exception: " + e.getMessage());
			err.adderror(e.getMessage());
			// SessionData.putServletError(_req, e.getMessage());
		} finally {
			pstmt = SQL.clearPreparedStatement(pstmt);
		}

		return (ret);
	}

	public static int checkSaveCount(Connection conn, String cTable, int wheretype, int org, int sid, String gsid,
			int iunique) {
		String methodname = "checkSaveCount";
		String sql = "";
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		int pcnt = 0;
		int cnt = 0;

		try {
			String wClause = "WHERE iorg_uid=%ORGUID% and iesped_student_id=%SID% and lsubject=%SUBJECT% ";
			wClause = STR.replaceString(wClause, "%SID%", "?");
			wClause = STR.replaceString(wClause, "%ORGUID%", "?");
			wClause = STR.replaceString(wClause, "%SUBJECT%", "?");
			if (iunique > 0) {
				wClause = wClause + " AND iunique=?";
			}

			// check count
			sql = "select count(*) from " + cTable + " " + wClause;
			pstmt = conn.prepareStatement(sql);
			pcnt = 1;
			pstmt.setInt(pcnt++, org);
			pstmt.setInt(pcnt++, sid);
			pstmt.setString(pcnt++, dupSubject);
			if (iunique > 0) {
				pstmt.setInt(pcnt++, iunique);
			}
			rset = pstmt.executeQuery();
			if (rset.next()) {
				cnt = rset.getInt(1);
			}
		} catch (Exception e) {
			Logs.error(methodname + " exception: " + e.getMessage());
		} finally {
			rset = SQL.clearResultSet(rset);
			pstmt = SQL.clearPreparedStatement(pstmt);
		}

		return (cnt);
	}
}
