package com.esped.lib;

import com.esped.exceptions.InvalidEspedSqlException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Vector;

public class SAV
{

public static final int WHERE_ORGSID = 0;
public static final int WHERE_ORGONLY = 1;
public static final int WHERE_ORGSIDGSID = 2;
public static final int WHERE_TRANSFER = 50;
public static final int WHERE_SYSADMIN = 75;
public static final int WHERE_ORGZERO = 80;
public static final int WHERE_TGSIDONLY = 85;
public static final int WHERE_CUSTOM = 99;



public static void updateRecordOrg(
Connection conn,
AuditVars audit,
String cTable,
int org,
Vector<DBitem> vParams)
{
	updateRecord(conn, audit, null, cTable, WHERE_ORGONLY, org, 0, "", vParams);
}

public static void updateRecord(
Connection conn,
AuditVars audit,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
Vector<DBitem> vParams)
{
	updateRecord(conn, audit, null, cTable, wheretype, org, sid, gsid, vParams);
}

public static void updateRecord(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
Vector<DBitem> vParams)
{
	String methodname = "updateRecord";
	PreparedStatement pstmt = null;
	ResultSet rset = null;
	int cnt = 0;

	try
	{
		if (err == null)
		{
			err = new ERR();
		}
		cnt = checkSaveCount(conn, cTable, wheretype, org, sid, gsid, 0);
		if (cnt > 1)
		{
			Logs.error(methodname + ": Invalid update RowCount=" + cnt, "table=" + cTable + ", org=" + org + ", sid=" + sid + ", gsid=" + gsid);
		}
		else
		{
			// if none, insert a blank row first
			if (cnt == 0)
			{
				// don't insert into the main STUDENT/STUDENT_MEETING table
				if (cTable.equals("STUDENT") || cTable.equals("STUDENT_MEETING"))
				{
					Logs.error(methodname + ": attempt to insert into STUDENT table");
					return;
				}

				insertBlankRecord(conn, audit, err, cTable, wheretype, org, sid, gsid, 0);
			}

			saveRecord(conn, audit, err, cTable, wheretype, org, sid, gsid, 0, vParams, false);
		}
	}
	catch (Exception e)
	{
		Logs.error(methodname, e);
		err.adderror(e.getMessage());
	}
	finally
	{
		rset = SQL.clearResultSet(rset);
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
}

/*
** Saves a single record
*/
public static String saveRecord(
Connection conn,
AuditVars audit,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
int iunique,
Vector<DBitem> vParams)
{
	return(saveRecord(conn, audit, null, cTable, wheretype, org, sid, gsid, iunique, vParams, false));
}

public static String saveRecord(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
int iunique,
Vector<DBitem> vParams)
{
	return(saveRecord(conn, audit, err, cTable, wheretype, org, sid, gsid, iunique, vParams, false));
}

public static String saveRecord(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
int iunique,
Vector<DBitem> vParams,
boolean insertnew)
{
	return(saveRecord(conn, audit, err, cTable, wheretype, org, sid, gsid, iunique, vParams, insertnew, false, false));
}

public static String saveRecord(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
int iunique,
Vector<DBitem> vParams,
boolean insertnew,
boolean auditchange,
boolean whenchange)
{
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

	if (err == null)
	{
		err = new ERR();
	}
	// make sure only one record will get updated
	int savecount = checkSaveCount(conn, cTable, wheretype, org, sid, gsid, iunique);
	if (savecount == 0 && insertnew)
	{
		// insert blank record first
		savecount = insertBlankRecord(conn, audit, err, cTable, wheretype, org, sid, gsid, iunique);
	}
	if (savecount != 1)
	{
		Logs.error(methodname + ": invalid save count", "table=" + cTable + ", org=" + org + ", sid=" + sid + ", gsid=" + gsid + ", iunique=" + iunique);
		return("");
	}
	
	try
	{
		cTable = cTable.toUpperCase();
		sql = "update " + cTable + " set";
		auditstr = "update " + cTable + " set";
		
		for (i=0 ; i < vParams.size() ; i++)
		{
			DBitem dbi = vParams.elementAt(i);
			if (dbi.table != null && dbi.column != null && !dbi.column.equals("") && dbi.table.toUpperCase().equals(cTable))
			{
				if (//dbi.type.equalsIgnoreCase("e")|| GR 29050 
					dbi.type.equalsIgnoreCase("f")
				|| dbi.type.equalsIgnoreCase("x"))
				{
					// skip these
				}
				else
				{
					doupdate = true;
					dbi.value = STR.cn(dbi.value);
					String v = dbi.value;
					if (dbi.type.equalsIgnoreCase("w"))
					{
						// see if the value is a well-formatted timestamp
						if (dbi.value.equalsIgnoreCase("sysdate")
						|| DATES.parseTimestamp(dbi.value, DATES.stdtime) == null)
						{
							dbi.value = "sysdate";
							sql = sql + sep + dbi.column + "=" + DBV.getSysdateFunction();
						}
						else
						{
							sql = sql + sep + dbi.column + "=?";
						}
					}
					else if (dbi.type.equalsIgnoreCase("d") && dbi.value.equalsIgnoreCase("sysdate"))
					{
						sql = sql + sep + dbi.column + "=" + DBV.getSysdateFunction();
					}
					else
					{
						if (dbi.value.equals(""))
						{
							sql = sql + sep + dbi.column + "=null";
							v = "null";
						}
						else
						{
							sql = sql + sep + dbi.column + "=?";
							v = "'" + v + "'";
						}
					}
					auditstr = auditstr + sep + dbi.column + "=" + v;
					sep = ",";
				}
			}
		}

		String wClause = getWhereClause(wheretype);
		sql = sql + " " + wClause;
		sql = STR.replaceString(sql, "%SID%", "?");
		sql = STR.replaceString(sql, "%GSID%", "?");
		sql = STR.replaceString(sql, "%ORGUID%", "?");
		auditstr = auditstr + " " + wClause;
		auditstr = STR.replaceString(auditstr, "%SID%", String.valueOf(sid));
		auditstr = STR.replaceString(auditstr, "%GSID%", gsid);
		auditstr = STR.replaceString(auditstr, "%ORGUID%", String.valueOf(org));
		if (iunique > 0)
		{
			sql = sql + " AND iunique=?";
			auditstr = auditstr + " AND iunique=" + iunique;
		}
		
		if (doupdate)
		{
			// auto audit change
			if (auditchange)
			{
				auditPreviousValues(conn, audit, cTable, org, sid, iunique, vParams, false, "", "");
			}
			pstmt = conn.prepareStatement(sql);
			pcnt = 1;

			for (i=0 ; i < vParams.size() ; i++)
			{
				DBitem dbi = vParams.elementAt(i);
				if (dbi.table != null && dbi.column != null && !dbi.column.equals("") && dbi.table.toUpperCase().equals(cTable))
				{
					if (//dbi.type.equalsIgnoreCase("e") || GR 29050 
					   dbi.type.equalsIgnoreCase("f")
					|| dbi.type.equalsIgnoreCase("x"))
					{
						// skip these
					}
					else
					{
						if (!dbi.value.equals(""))
						{
							if (dbi.type.equalsIgnoreCase("w"))
							{
								// convert to Timestamp
								if (!dbi.value.equalsIgnoreCase("sysdate"))
								{
									Timestamp xdate = DATES.parseTimestamp(dbi.value, DATES.stdtime);
									pstmt.setTimestamp(pcnt++, xdate);
								}
							}
							else if (dbi.type.equalsIgnoreCase("d"))
							{
								// convert to Date
								if (!dbi.value.equalsIgnoreCase("sysdate"))
								{
									Date xdate = DATES.parseDateString(dbi.value);
									pstmt.setDate(pcnt++, xdate);
								}
							}
							else if (dbi.type.equalsIgnoreCase("i"))
							{
								// convert to int
								// PROBLEM:  some $i are actually floating point numbers
								// so just do a setString for now and hope
								//int xint = Integer.parseInt(dbi.value);
								//pstmt.setInt(pcnt++, xint);
								pstmt.setString(pcnt++, dbi.value);
							}
							else
							{
								pstmt.setString(pcnt++, dbi.value);
							}
						}
					}
				}
			}
			
			switch (wheretype)
			{
			case WHERE_ORGONLY:
				pstmt.setInt(pcnt++, org);
				break;
			case WHERE_ORGSID:
				pstmt.setInt(pcnt++, org);
				pstmt.setInt(pcnt++, sid);
				break;
			case WHERE_ORGSIDGSID:
				pstmt.setInt(pcnt++, org);
			case WHERE_TGSIDONLY:				
				pstmt.setString(pcnt++, gsid);
				break;
			case WHERE_SYSADMIN:
			case WHERE_ORGZERO:
				// no bind variables
				break;
			case WHERE_TRANSFER:
				pstmt.setInt(pcnt++, org);
				break;
			default:
				break;
			}
			// add the iunique
			if (iunique > 0)
			{
				pstmt.setInt(pcnt++, iunique);
			}
			
			Logs.logMsg("saveRecord Statement ", sql, Logs.SQL );
			ucnt = pstmt.executeUpdate();
			Logs.logMsg("saveRecord Statement ", ucnt + " Rows Updated", Logs.SQL);
			if (audit != null)
			{
				audit.auditLog(methodname, ucnt + " Rows Updated", auditstr);
			}
			// record-level when and who changed
			updateRecordChanged(conn, audit, cTable, org, sid, iunique);
		}
		else
		{
			if (Logs.isDebugEnabled()) Logs.debug(methodname + ": no matching columns to update");
		}
	}
	catch (Exception e)
	{
		Logs.error(methodname + " exception: " + e.getMessage());
		err.adderror(e.getMessage());
  		//SessionData.putServletError(_req, e.getMessage());
	}
	finally
	{
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
	
	return(ret);
}

/*
** Inserts a record into a table that has an iunique
** 
*/
public static int insertRecord(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
Vector<DBitem> vParams)
{
	return(insertRecord(conn, audit, err, cTable, wheretype, org, sid, gsid, vParams, 0));
}

public static int insertRecord(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
Vector<DBitem> vParams,
int preserveiunique)
{
	return(insertRecord(conn, audit, err, cTable, wheretype, org, sid, gsid, vParams, preserveiunique, false));
}

public static int insertRecord(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
Vector<DBitem> vParams,
int preserveiunique,
boolean auditsid)
{
	String methodname = "insertRecord";
	String sql = "";
	String sqlvals = "";
	String auditsql = "";
	String auditsqlvals = "";
	DBvars DBV = new DBvars(conn);
	PreparedStatement pstmt = null;
	int pcnt = 0;
	int ucnt = 0;
	int iunique = 0;
	String sep = ",";
	int i = 0;
	boolean doupdate = true;
	String checkseq = DBV.getSequence();
	String checklast = DBV.getLastInsert();
	
	try
	{
		if (err == null)
		{
			err = new ERR();
		}
		cTable = cTable.toUpperCase();
		// don't insert into the main STUDENT/STUDENT_MEETING table
		if (cTable.equals("STUDENT") || cTable.equals("STUDENT_MEETING"))
		{
			Logs.error(methodname + ": attempt to insert into STUDENT table");
			return(0);
		}
		sql = "insert into " + cTable + " (";
		sqlvals = ") values (";
		auditsql = sql;
		auditsqlvals = ") values (";

		// see if a predetermined iunique is to be used
		if (preserveiunique > 0)
		{
			iunique = preserveiunique;
			sql = sql + "iunique,";
			sqlvals = sqlvals + "?,";
			auditsql = auditsql + "iunique,";
			auditsqlvals = auditsqlvals + String.valueOf(iunique) + ",";
			doupdate = true;
		}
		else
		{
			// get the next iunique
			// on Oracle, this comes from a sequence
			// on MySQL this is blank and generated later by auto_increment
			iunique = SQL.getNextSeq(conn, checkseq, cTable);
			if (iunique > 0)
			{
				sql = sql + DBV.getInsertIunique();
				sqlvals = sqlvals + "?,";
				auditsql = auditsql + DBV.getInsertIunique();
				auditsqlvals = auditsqlvals + String.valueOf(iunique) + ",";
			}
		}

		switch (wheretype)
		{
		case WHERE_ORGONLY:
			sql = sql + "iorg_uid";
			sqlvals = sqlvals + "?";
			auditsql = auditsql + "iorg_uid";
			auditsqlvals = auditsqlvals + String.valueOf(org);
			break;
		case WHERE_TGSIDONLY:
			sql = sql + "tgsid";
			sqlvals = sqlvals + "?";
			auditsql = auditsql + "tgsid";
			auditsqlvals = auditsqlvals + String.valueOf(org);
			break;
		case WHERE_ORGSID:
		case WHERE_ORGSIDGSID:
			sql = sql + "iorg_uid,iesped_student_id";
			sqlvals = sqlvals + "?,?";
			auditsql = auditsql + "iorg_uid,iesped_student_id";
			auditsqlvals = auditsqlvals + String.valueOf(org) + "," + String.valueOf(sid);
			if (!gsid.equals(""))
			{
				sql = sql + ",tgsid";
				sqlvals = sqlvals + ",?";
				auditsql = auditsql + ",tgsid";
				auditsqlvals = auditsqlvals + ",'" + gsid + "'";
			}
			break;
		case WHERE_ORGZERO:
		case WHERE_SYSADMIN:
			if (!hasOrgParam(cTable, vParams))
			{
				sql = sql + "iorg_uid";
				sqlvals = sqlvals + "0";
				auditsql = auditsql + "iorg_uid";
				auditsqlvals = auditsqlvals + "0";
			}
			else
			{
				sep = "";
			}
			break;
		case WHERE_TRANSFER:
		default:
			Logs.error("invalid apptype for insert operation");
			return(0);
		}
		
		//
		for (i=0 ; i < vParams.size() ; i++)
		{
			DBitem dbi = vParams.elementAt(i);
			if (dbi.table != null && dbi.column != null && !dbi.column.equals("") && dbi.table.toUpperCase().equals(cTable))
			{
				if (//dbi.type.equalsIgnoreCase("e") || GR 29050
						dbi.type.equalsIgnoreCase("f")
				|| dbi.type.equalsIgnoreCase("x"))
				{
					// skip these
				}
				else
				{
					doupdate = true;
					dbi.value = STR.cn(dbi.value);
					String v = dbi.value;
					if (dbi.type.equalsIgnoreCase("w"))
					{
						// see if the value is a well-formatted timestamp
						if (dbi.value.equalsIgnoreCase("sysdate")
						|| DATES.parseTimestamp(dbi.value, DATES.stdtime) == null)
						{
							dbi.value = "sysdate";
							sql = sql + sep + dbi.column;
							sqlvals = sqlvals + sep + DBV.getSysdateFunction();
						}
						else
						{
							sql = sql + sep + dbi.column;
							sqlvals = sqlvals + sep + "?";
							v = "'" + v + "'";
						}
					}
					else if (dbi.type.equalsIgnoreCase("d") && dbi.value.equalsIgnoreCase("sysdate"))
					{
						sql = sql + sep + dbi.column;
						sqlvals = sqlvals + sep + DBV.getSysdateFunction();
					}
					else
					{
						if (dbi.value.equals(""))
						{
							sql = sql + sep + dbi.column;
							sqlvals = sqlvals + sep + "null";
							v = "null";
						}
						else
						{
							sql = sql + sep + dbi.column;
							sqlvals = sqlvals + sep + "?";
							v = "'" + v + "'";
						}
					}
					auditsql = auditsql + sep + dbi.column;
					auditsqlvals = auditsqlvals + sep + v;
					sep = ",";
				}
			}
		}

		sql = sql + sqlvals + ")";
		auditsql = auditsql + auditsqlvals + ")";

		if (doupdate)
		{
			pstmt = conn.prepareStatement(sql);
			pcnt = 1;
			if (iunique > 0)
			{
				pstmt.setInt(pcnt++, iunique);
			}

			switch (wheretype)
			{
			case WHERE_ORGONLY:
				pstmt.setInt(pcnt++, org);
				break;
			case WHERE_ORGSID:
			case WHERE_ORGSIDGSID:
				pstmt.setInt(pcnt++, org);
				pstmt.setInt(pcnt++, sid);
				if (!gsid.equals(""))
				{
					pstmt.setString(pcnt++, gsid);
				}
				break;
			case WHERE_ORGZERO:
			case WHERE_SYSADMIN:
				break;
			case WHERE_TGSIDONLY:
				if (!gsid.equals(""))
				{
					pstmt.setString(pcnt++, gsid);
				}
				break;
			case WHERE_TRANSFER:
			default:
				Logs.error("invalid apptype for insert operation");
				return(0);
			}
			
			for (i=0 ; i < vParams.size() ; i++)
			{
				DBitem dbi = vParams.elementAt(i);
				if (dbi.table != null && dbi.column != null && !dbi.column.equals("") && dbi.table.toUpperCase().equals(cTable))
				{
					if (//dbi.type.equalsIgnoreCase("e") || GR 29050 
						dbi.type.equalsIgnoreCase("f")
					|| dbi.type.equalsIgnoreCase("x"))
					{
						// skip these
					}
					else
					{
						if (!dbi.value.equals(""))
						{
							if (dbi.type.equalsIgnoreCase("w"))
							{
								// convert to Date
								if (!dbi.value.equalsIgnoreCase("sysdate"))
								{
									Timestamp xdate = DATES.parseTimestamp(dbi.value, DATES.stdtime);
									pstmt.setTimestamp(pcnt++, xdate);
								}
							}
							else if (dbi.type.equalsIgnoreCase("d"))
							{
								// convert to Date
								if (!dbi.value.equalsIgnoreCase("sysdate"))
								{
									Date xdate = DATES.parseDateString(dbi.value);
									pstmt.setDate(pcnt++, xdate);
								}
							}
							else if (dbi.type.equalsIgnoreCase("i"))
							{
								// convert to int
								// PROBLEM:  some $i are actually floating point numbers
								// so just do a setString for now and hope
								//int xint = Integer.parseInt(dbi.value);
								//pstmt.setInt(pcnt++, xint);
								pstmt.setString(pcnt++, dbi.value);
							}
							else
							{
								pstmt.setString(pcnt++, dbi.value);
							}
						}
					}
				}
			}
			
			Logs.logMsg("insertRecord Statement ", sql, Logs.SQL );
			ucnt = pstmt.executeUpdate();
			Logs.logMsg("insertRecord Statement ", ucnt + " Rows Inserted", Logs.SQL);
			if (iunique == 0)
			{
				iunique = SQL.getNextSeq(conn, checklast, cTable);
			}
			// add the resulting iunique to the end of the audit string
			// have to make sure it fits into the field which is currently 4000 characters
			auditsql = STR.MaxString(auditsql, 3980);
			auditsql = auditsql + " => " + iunique;
			if (audit != null)
			{
				if (auditsid)
				{
					audit.auditLogSid(methodname, ucnt + " Rows Updated", auditsql, conn, org, sid, 0);
				}
				else
				{
					audit.auditLog(methodname, ucnt + " Rows Updated", auditsql);
				}
			}
			// record-level when and who changed
			updateRecordChanged(conn, audit, cTable, org, sid, iunique);
		}
		else
		{
			if (Logs.isDebugEnabled()) Logs.debug(methodname + ": no matching columns to update");
		}
	}
	catch (Exception e)
	{
		Logs.error(methodname + " exception: " + e.getMessage());
		err.adderror(e.getMessage());
	}
	finally
	{
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
	
	return(iunique);
}

public static int checkSaveCount(
Connection conn,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
int iunique)
{
	String methodname = "checkSaveCount";
	String sql = "";
	PreparedStatement pstmt = null;
	ResultSet rset = null;
	int pcnt = 0;
	int cnt = 0;

	try
	{
		String wClause = getWhereClause(wheretype);
		wClause = STR.replaceString( wClause, "%SID%", "?");
		wClause = STR.replaceString( wClause, "%GSID%", "?");
		wClause = STR.replaceString( wClause, "%ORGUID%", "?");
		if (iunique > 0)
		{
			wClause = wClause + " AND iunique=?";
		}
		
		// check count
		sql = "select count(*) from " + cTable + " " + wClause;
		pstmt = conn.prepareStatement(sql);
		pcnt = 1;
		switch (wheretype)
		{
		case WHERE_ORGONLY:
			pstmt.setInt(pcnt++, org);
			break;
		case WHERE_ORGSID:
			pstmt.setInt(pcnt++, org);
			pstmt.setInt(pcnt++, sid);
			break;
		case WHERE_ORGSIDGSID:
			pstmt.setInt(pcnt++, org);
		case WHERE_TGSIDONLY:
			pstmt.setString(pcnt++, gsid);
			break;
		case WHERE_SYSADMIN:
		case WHERE_ORGZERO:
			// no bind variables
			break;
		case WHERE_TRANSFER:
			pstmt.setInt(pcnt++, org);
			break;
		default:
			break;
		}
		if (iunique > 0)
		{
			pstmt.setInt(pcnt++, iunique);
		}
		rset = pstmt.executeQuery();
		if (rset.next())
		{
			cnt = rset.getInt(1);
		}
	}
	catch (Exception e)
	{
		Logs.error(methodname + " exception: " + e.getMessage());
	}
	finally
	{
		rset = SQL.clearResultSet(rset);
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
	
	return(cnt);
}

public static boolean hasOrgParam(String cTable, Vector<DBitem> vParams)
{
	boolean sts = false;
	int i = 0;
	for (i=0 ; i < vParams.size() ; i++)
	{
		DBitem dbi = vParams.elementAt(i);
		if (dbi.table != null && dbi.column != null
		&& dbi.table.toUpperCase().equals(cTable)
		&& dbi.column.equalsIgnoreCase("iorg_uid"))
		{
			sts = true;
			break;
		}
	}
	return(sts);
}

public static void auditPreviousValues(
Connection conn,
AuditVars audit,
String cTable,
int org,
int sid,
int iunique,
Vector<DBitem> vParams,
boolean dryrun,
String firstname,
String lastname)
{
	StudentVars SV = StudentVars.getStudentInfo(conn, org, sid);
	auditPreviousValues(conn, audit, cTable, org, sid, iunique, vParams, SV.firstname, SV.lastname, SV.StudentType, "", "", false, true, null);
}

public static void auditPreviousValues(
Connection conn,
AuditVars audit,
String cTable,
int org,
int sid,
int iunique,
Vector<DBitem> vParams,
StudentVars SV,
String rollbacktable,
String rollbackname,
boolean dryrun,
boolean showsid,
ERR msglog)
{
	auditPreviousValues(conn, audit, cTable, org, sid, iunique, vParams, SV.firstname, SV.lastname, SV.StudentType, rollbacktable, rollbackname, dryrun, showsid, msglog);
}

public static void auditPreviousValues(
Connection conn,
AuditVars audit,
String cTable,
int org,
int sid,
int iunique,
Vector<DBitem> vParams,
String firstname,
String lastname,
String studenttype,
String rollbacktable,
String rollbackname,
boolean dryrun,
boolean showsid,
ERR msglog)
{
	String methodname = "auditPreviousValues";
	String sql = "";
	String sep = "";
	PreparedStatement pstmt = null;
	ResultSet rset = null;
	int pcnt = 0;
	int i = 0;
	
	try
	{
		cTable = cTable.toUpperCase();
		sql = "select * from " + cTable;

		// set up the where clause
		// always has org
		sql = sql + " where iorg_uid=?";
		// see if there's an iunique
		if (iunique > 0)
		{
			sql = sql + " and iunique=?";
		}
		// see if there's a sid
		if (sid > 0)
		{
			sql = sql + " and iesped_student_id=?";
		}

		pstmt = conn.prepareStatement(sql);
		pcnt = 1;
		// set the where clause variables
		// always has org
		pstmt.setInt(pcnt++, org);
		// see if there's an iunique
		if (iunique > 0)
		{
			pstmt.setInt(pcnt++, iunique);
		}
		// see if there's a sid
		if (sid > 0)
		{
			pstmt.setInt(pcnt++, sid);
		}

		rset = pstmt.executeQuery();
		while (rset.next())
		{
			sep = "";
			for (i=0 ; i < vParams.size() ; i++)
			{
				DBitem dbi = vParams.elementAt(i);
				if (dbi.table != null && dbi.column != null && !dbi.column.equals("") && dbi.table.toUpperCase().equals(cTable))
				{
					if (dbi.type.equalsIgnoreCase("e")
					|| dbi.type.equalsIgnoreCase("f")
					|| dbi.type.equalsIgnoreCase("x"))
					{
						// skip these
					}
					else
					{
						String oldval = "";
						if (dbi.type.equalsIgnoreCase("d"))
						{
							oldval = DATES.convertDate(rset.getDate(dbi.column), null);
						}
						else
						{
							oldval = STR.cn(rset.getString(dbi.column));
						}
						sep = ", ";
						
						// see if the new value is different from the old one 
						if (!oldval.equals(dbi.value))
						{
							if (msglog != null)
							{
								String sidstr = "";
								if (showsid)
								{
									sidstr = " for sid=" + String.valueOf(sid);
								}
								msglog.addbuf("Changing ", dbi.table, "$", dbi.column,
									" from [", oldval, "] to [", dbi.value, "]", sidstr);
							}
							if (!dryrun)
							{
								audit.auditChange(audit.m_auditconn, org, sid, firstname, lastname, studenttype, dbi.table,
									dbi.column, audit.m_emailname, oldval, dbi.value);
								insertRollbackEntry(conn, rollbacktable, rollbackname, org, sid, "", dbi.table, dbi.column, oldval,
									dbi.value, audit.m_emailname, firstname, lastname);
							}
						}
					}
				}
			}
		}
	}
	catch (Exception e)
	{
		Logs.error(methodname + " exception: " + e.getMessage());
	}
	finally
	{
		rset = SQL.clearResultSet(rset);
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
}

public static void insertRollbackEntry(
Connection conn,
String rollbacktable,
String rollbackname,
int org,
int sid,
String tgsid,
String ttable,
String tcolumn,
String toldvalue,
String tnewvalue,
String twho,
String lastname,
String firstname)
{
	if (!rollbacktable.equals("") && !rollbackname.equals(""))
	{
		Vector <DBitem> vParams = new Vector<DBitem>();
		vParams.add(new DBitem(rollbacktable, "TROLLBACKNAME", rollbackname));
		vParams.add(new DBitem(rollbacktable, "TTABLE", ttable));
		vParams.add(new DBitem(rollbacktable, "TCOLUMN", tcolumn));
		vParams.add(new DBitem(rollbacktable, "TOLDVALUE", toldvalue));
		vParams.add(new DBitem(rollbacktable, "TNEWVALUE", tnewvalue));
		vParams.add(new DBitem(rollbacktable, "TWHO", twho));
		vParams.add(new DBitem(rollbacktable, "TLASTNAME", lastname));
		vParams.add(new DBitem(rollbacktable, "TFIRSTNAME", firstname));
		SAV.insertRecord(conn, null, rollbacktable, SAV.WHERE_ORGSID, org, sid, tgsid, vParams);
	}
} 	

public static String getWhereClause(int wtype)
{
	String w = "WHERE ";
	switch (wtype)
	{
	case WHERE_ORGSID:
		w = "WHERE iorg_uid=%ORGUID% and iesped_student_id=%SID% ";
		break;
	case WHERE_ORGSIDGSID:
		w = "WHERE iorg_uid=%ORGUID% and tgsid=%GSID% ";
		break;
	case WHERE_ORGONLY:
		w = "WHERE iorg_uid=%ORGUID% ";
		break;
	case WHERE_SYSADMIN:
		w = "WHERE iorg_uid>=0 ";
		break;
	case WHERE_ORGZERO:
		w = "WHERE iorg_uid=0 ";
		break;
	case WHERE_TRANSFER:
		w = "WHERE iorg_uid=%ORGUID% or iorg_uid is not null ";
		break;
	case WHERE_TGSIDONLY:
		w = "WHERE tgsid=%GSID%";
		break;
	default:
		w = "WHERE ";
		break;
	}
	
	return(w);
}

public static void updateDateChanged(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
int iunique,
Vector<DBitem> vParams)
{
	setDateChanged(conn, audit, err, cTable, wheretype, org, sid, gsid, iunique, vParams, null);
}

public static void setDateChanged(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
int iunique,
Vector<DBitem> vParams,
Timestamp wc)
{
	String methodname = "updateDateChanged";
	String sql = "";
	PreparedStatement pstmt = null;
	int pcnt = 1;
	ResultSet rset = null;
	int cnt = 0;
	int ucnt = 0;

	try
	{
		String wctable = "WC_" + cTable;
		// only update if the WC table exists
		HashMap<String, String> hCols = SQL.getTableColumns(conn, wctable, false);
		if (hCols.isEmpty())
		{
			return;
		}
		// get anything in the vParams for this table and copy it
		// into a new version that will update the date changed
		Vector<DBitem> vNew = new Vector<DBitem>();
		for (int i=0 ; i < vParams.size() ; i++)
		{
			DBitem dbi = vParams.elementAt(i);
			if (dbi.table.equalsIgnoreCase(cTable))
			{
				// check if the column name exists (the hash map has them in lower case)
				String tmpcol = "w_" + dbi.column.toLowerCase();
				if (hCols.get(tmpcol) != null)
				{
					String d = "sysdate";
					if (wc != null)
					{
						d = DATES.convertDate(wc, DATES.stdtime);
					}
					vNew.add(new DBitem(wctable, "w_" + dbi.column, d));
				}
			}
		}
		// switch table name to the when changed name
		cTable = wctable;
		if (err == null)
		{
			err = new ERR();
		}
		cnt = checkSaveCount(conn, cTable, wheretype, org, sid, gsid, iunique);
		if (cnt > 1)
		{
			Logs.error(methodname + ": Invalid update RowCount=" + cnt, "table=" + cTable + ", org=" + org + ", sid=" + sid + ", gsid=" + gsid + ", iunique=" + iunique);
		}
		else
		{
			// if none, insert a blank row first
			if (cnt == 0)
			{
				insertBlankRecord(conn, audit, err, cTable, wheretype, org, sid, gsid, iunique);
			}

			saveRecord(conn, null, err, cTable, wheretype, org, sid, gsid, iunique, vNew, false);
		}
	}
	catch (Exception e)
	{
		Logs.error(methodname + " exception: " + e.getMessage());
		err.adderror(e.getMessage());
	}
	finally
	{
		rset = SQL.clearResultSet(rset);
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
}

public static void updateFieldIunique(
Connection conn,
int iunique,
String table,
String column,
String value)
{
	PreparedStatement pstmt = null;
	try
	{
		final String sql = "update " + table + " set " + column + "=? where iunique=?";
		pstmt = conn.prepareStatement(sql);
		int pcnt = 1;
		pstmt.setString(pcnt++, value);
		pstmt.setInt(pcnt++, iunique);
		int ucnt = pstmt.executeUpdate();
		if (ucnt != 1)
		{
			Logs.error("updateFieldIunique, bad update count=" + ucnt + ", table=" + table + " column=" + column + " iunique=" + iunique);
		}
		// record-level when and who changed
		updateRecordChanged(conn, null, table, 0, 0, iunique);
	}
	catch (Exception e)
	{
		Logs.error("updateFieldIunique", e);
	}
	finally
	{
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
}

public static int insertBlankRecord(
Connection conn,
AuditVars audit,
ERR err,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
int iunique)
{
	String methodname = "insertBlankRecord";
	String sql = "";
	PreparedStatement pstmt = null;
	int pcnt = 1;
	int ucnt = 0;

	try
	{
		// don't insert into the main STUDENT/STUDENT_MEETING table
		if (cTable.equals("STUDENT") || cTable.equals("STUDENT_MEETING"))
		{
			Logs.error(methodname + ": attempt to insert into STUDENT table");
			return(0);
		}

		switch (wheretype)
		{
		case WHERE_ORGONLY:
			if (iunique > 0)
			{
				sql = "insert into " + cTable + " (iunique,iorg_uid) values (?,?)";
			}
			else
			{
				sql = "insert into " + cTable + " (iorg_uid) values (?)";
			}
			break;
		case WHERE_ORGSID:
		case WHERE_ORGSIDGSID:
			if (iunique > 0)
			{
				sql = "insert into " + cTable + " (iunique,iorg_uid,iesped_student_id,tgsid) values (?,?,?,?)";
			}
			else
			{
				sql = "insert into " + cTable + " (iorg_uid,iesped_student_id,tgsid) values (?,?,?)";
			}
			break;
		case WHERE_TGSIDONLY:
			if (iunique > 0)
			{
				sql = "insert into " + cTable + " (iunique,tgsid) values (?,?)";
			}
			else
			{
				sql = "insert into " + cTable + " (tgsid) values (?)";
			}
			break;
		case WHERE_SYSADMIN:
		case WHERE_ORGZERO:
		case WHERE_TRANSFER:
		default:
			break;
		}
		if (!sql.equals(""))
		{
			pstmt = conn.prepareStatement(sql);
			pcnt = 1;
			switch (wheretype)
			{
			case WHERE_ORGONLY:
				if (iunique > 0)
				{
					pstmt.setInt(pcnt++, iunique);
					pstmt.setInt(pcnt++, org);
				}
				else
				{
					pstmt.setInt(pcnt++, org);
				}
				break;
			case WHERE_ORGSID:
			case WHERE_ORGSIDGSID:
				if (iunique > 0)
				{
					pstmt.setInt(pcnt++, iunique);
					pstmt.setInt(pcnt++, org);
					pstmt.setInt(pcnt++, sid);
					pstmt.setString(pcnt++, gsid);
				}
				else
				{
					pstmt.setInt(pcnt++, org);
					pstmt.setInt(pcnt++, sid);
					pstmt.setString(pcnt++, gsid);
				}
				break;
			case WHERE_TGSIDONLY:
				if (iunique > 0)
				{
					pstmt.setInt(pcnt++, iunique);
					pstmt.setString(pcnt++, gsid);
				}
				else
				{
					pstmt.setString(pcnt++, gsid);
				}
				break;			
			case WHERE_SYSADMIN:
			case WHERE_ORGZERO:
			case WHERE_TRANSFER:
			default:
				break;
			}

			ucnt = pstmt.executeUpdate();
			// record-level when and who changed
			updateRecordChanged(conn, audit, cTable, org, sid, iunique);
		}
	}
	catch (Exception e)
	{
		Logs.error(methodname + " exception: " + e.getMessage());
	}
	finally
	{
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
	return(ucnt);
}

public static String cleanRichText(Connection conn, String s)
{
	String ret = s;
	// make sure it starts with <html>
	ret = ret.trim();
	if (ret.length() > 0)
	{
		String start = ret.substring(0, 6);
		if (!start.equalsIgnoreCase("<html>"))
		{
			ret = "<HTML>" + ret;
		}
		// remove html comments
		int comment = ret.indexOf("<!--");
		while (comment >= 0)
		{
			int endcomment = ret.indexOf("-->", comment);
			if (endcomment >= 0)
			{
				ret = ret.substring(0, comment) + ret.substring(endcomment + 3);
				comment = ret.indexOf("<!--");
			}
			else
			{
				ret = ret.substring(0, comment);
				comment = -1;
			}
		}

		// remove "local" img tags
		ret = STR.removeLocalImgHTML(ret);
		
		// string replacements - get from the database
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		try
		{
			String sql = "select tsource,treplace from REF_RICHTEXTSUB where iorg_uid=0 order by torder";
			pstmt = conn.prepareStatement(sql);
			rset = pstmt.executeQuery();
			while (rset.next())
			{
				String src = SQL.getResultSetString(rset, "tsource");
				String repl = SQL.getResultSetString(rset, "treplace");
				if (!src.equals(""))
				{
					ret = STR.replaceString(ret, src, repl);
				}
			}
		}
		catch (Exception e)
		{
			Logs.error("cleanRichText", e);
		}
		finally
		{
			rset = SQL.clearResultSet(rset);
			pstmt = SQL.clearPreparedStatement(pstmt);
		}
		/*
		// no rowspan for BIRT, replace with a "harmless" tag name
		ret = STR.replaceString(ret, "rowspan=", "yyy=");

		// doesn't seem to like "display: block;" either
		ret = STR.replaceString(ret, "display: block;", "");
		ret = STR.replaceString(ret, "display: block", "");
		*/
		
		// make sure it ends with </html>
		String end = ret.substring(ret.length() - 7);
		if (!end.equalsIgnoreCase("</html>"))
		{
			ret = ret + "</HTML>";
		}
	}
	return(ret);
}

public static int insertRecord(
Connection conn,
AuditVars audit,
String cTable,
int wheretype,
int org,
int sid,
String gsid,
Vector<DBitem> vParams)
{
	return(insertRecord(conn, audit, null, cTable, wheretype, org, sid, gsid, vParams));
}

public static void updateRecordChanged(
Connection conn,
AuditVars audit,
String cTable,
int org,
int sid,
int iunique)
{
	String methodname = "updateRecordChanged";
	String sql = "";
	String sqlone = "";
	String who = "esped@esped.com";
	PreparedStatement pstmt = null;
	ResultSet rset = null;
	int ucnt = 0;
	int pcnt = 1;
	DBvars DBV = new DBvars(conn);

	try
	{
		if (audit != null)
		{
			who = audit.m_emailname;
		}
		String sep = " where ";
		sqlone = "select wwhenmod,twhomod from " + cTable;
		sql = "update " + cTable + " set WWHENMOD=" + DBV.getSysdateFunction() + ",TWHOMOD=?";
		if (org > 0)
		{
			sqlone = sqlone + sep + "iorg_uid=?";
			sql = sql + sep + "iorg_uid=?";
			sep = " and ";
		}
		// if an iunique was sent in, only use it otherwise try the sid
		if (iunique > 0)
		{
			sqlone = sqlone + sep + "iunique=?";
			sql = sql + sep + "iunique=?";
			sep = " and ";
		}
		else if (sid > 0)
		{
			sqlone = sqlone + sep + "iesped_student_id=?";
			sql = sql + sep + "iesped_student_id=?";
			sep = " and ";
		}

		// make sure only one record will be updated
		try
		{
			pstmt = conn.prepareStatement(sqlone);
			pcnt = 1;
			if (org > 0)
			{
				pstmt.setInt(pcnt++, org);
			}
			if (iunique > 0)
			{
				pstmt.setInt(pcnt++, iunique);
			}
			else if (sid > 0)
			{
				pstmt.setInt(pcnt++, sid);
			}
			rset = pstmt.executeQuery();
			while (rset.next())
			{
				ucnt++;
				if (ucnt > 1)
				{
					break;
				}
			}
		}
		catch (Exception e)
		{
			Logs.debug(methodname, " exception: ", e.getMessage());
		}
		finally
		{
			rset = SQL.clearResultSet(rset);
			pstmt = SQL.clearPreparedStatement(pstmt);
		}
		
		if (ucnt == 1)
		{
			try
			{
				pstmt = conn.prepareStatement(sql);
				pcnt = 1;
				pstmt.setString(pcnt++, who);
				if (org > 0)
				{
					pstmt.setInt(pcnt++, org);
				}
				if (iunique > 0)
				{
					pstmt.setInt(pcnt++, iunique);
				}
				else if (sid > 0)
				{
					pstmt.setInt(pcnt++, sid);
				}
				ucnt = pstmt.executeUpdate();
			}
			catch (Exception e)
			{
				Logs.debug(methodname, " exception: ", e.getMessage());
			}
			finally
			{
				pstmt = SQL.clearPreparedStatement(pstmt);
			}
		}
	}
	catch (Exception e)
	{
		Logs.debug(methodname, " exception: ", e.getMessage());
	}
	finally
	{
		rset = SQL.clearResultSet(rset);
		pstmt = SQL.clearPreparedStatement(pstmt);
	}
}


    public static void deleteRecordByID(Connection conn, AuditVars audit, String table, Integer org, Integer sid, Integer iunique) {
        String methodname = "deleteRecordByID";
        String who = "esped@esped.com";
        PreparedStatement pstmt = null;
        ResultSet rset = null;
        int ucnt = 0;
        int pcnt = 1;
        DBvars DBV = new DBvars(conn);
        StringBuilder sql=new StringBuilder("DELETE FROM "+table+" WHERE IUNIQUE="+iunique);
        if(org!=null){
            sql.append(" AND IORG_UID="+org);
        }
        if(sid!=null){
            sql.append(" AND IESPED_STUDENT_ID="+sid);
        }
        try {
            SQL.executeDelete(sql.toString(),conn);
        } catch (InvalidEspedSqlException e) {
            Logs.error(methodname,e);
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


}
 