package com.esped.apps.US;
 
// copyright 2001
//
//   User.Java
//   eSped.com March 20, 2002
//----------------------------------------
 
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypes;

import com.esped.lib.AppVars;
import com.esped.lib.ERR;
import com.esped.lib.FILE;
import com.esped.lib.IP;
import com.esped.lib.Logs;
import com.esped.lib.STR;
import com.esped.login.SessionData;

public class Upload extends HttpServlet
{

private String sAppName = "";
private String machinename = "";
private Vector<String> vAllowedExtensions = new Vector<String>();

public void init(ServletConfig config) throws ServletException
{
	super.init(config);
	sAppName  = this.getClass().getName();
	machinename = IP.getMachineName();
	vAllowedExtensions.add("xls");
	vAllowedExtensions.add("xlsx");
	vAllowedExtensions.add("csv");
	vAllowedExtensions.add("tsv");
}

public void service(
HttpServletRequest _req,
HttpServletResponse _res)
throws ServletException, IOException
{
	uploadfile(_req, _res);
}

public void doPost(
HttpServletRequest _req,
HttpServletResponse _res)
throws ServletException, IOException
{
	uploadfile(_req, _res);
}

public int maxUploadSize()
{
	return(20000000);
}

public boolean isAllowedExtension(String ext)
{
	boolean isAllowed = vAllowedExtensions.contains(ext.toLowerCase());
	return(isAllowed);
}

public boolean isImage()
{
	return(false);
}

public boolean isMatchingMimeType(String ext1, String ext2)
{
	boolean ret = false;
	Vector<String> vAllowedExtensionMapping = new Vector<String>();
	vAllowedExtensionMapping.add("jpg^jpe");
	vAllowedExtensionMapping.add("jpg^jpeg");
	vAllowedExtensionMapping.add("m4v^m4a");
	vAllowedExtensionMapping.add("mp4a^m4a");
	vAllowedExtensionMapping.add("mpga^mp3");
	vAllowedExtensionMapping.add("mpeg^mpg");
	vAllowedExtensionMapping.add("tiff^tif");
	if (ext1.equalsIgnoreCase(ext2))
	{
		ret = true;
	}
	else
	{
		String map = ext1 + "^" + ext2;
		if (vAllowedExtensionMapping.contains(map.toLowerCase()))
		{
			ret = true;
		}
	}
	return(ret);
}

public String generateUniqueUploadFile(int org)
{
	String ret = "";
	// the root path could come from REF_PROPERTIES
	String path = "/var/esped/uploads/" + org;
	// make sure upload directory exists
    File d = new File(path);
    if (!d.isDirectory())
    {
    	if (d.isFile())
    	{
    		d.delete();
    	}
    	d.mkdirs();
    }

	String uniquefile = UUID.randomUUID().toString() + ".upload";
	ret = STR.addPath(path, uniquefile);
	return(ret);
}

public String getUploadFile(HttpServletRequest _req, AppVars _cons, int org, ERR err)
{
	String ufile = "";
    DiskFileItemFactory factory = new DiskFileItemFactory();
    ServletFileUpload SFU = new ServletFileUpload(factory);
	List<FileItem> items = null;
	String fname = "";
	try
	{
		// don't use this - causes a http connection reset
		// TODO: might want to set this to a really high value
		//SFU.setFileSizeMax(maxUploadSize());
		items = SFU.parseRequest(_req);
		if (items != null)
		{
			for (FileItem fi : items)
			{
				if (!fi.isFormField())
				{
					fname = fi.getName();
					long fsize = fi.getSize();
					String ext = STR.getFileExtension(fname);
					if (isAllowedExtension(ext))
					{
						// if we've already uploaded a file in this session, use the same name
						// to prevent maliciously uploading tons of files to the server
						// just need to change the file extension to match the incoming file name
						ufile = SessionData.getUploadFilename(_cons.SS);
						if (ufile.equals(""))
						{
							ufile = generateUniqueUploadFile(org);
						}
						ufile = STR.changeFileExtension(ufile, ext);
						// make sure the file is under the correct size
						if (fsize > maxUploadSize())
						{
							err.adderror("Maximum upload file size exceeded.");
							Logs.error("Maximum upload file size exceeded");
							FILE.deleteFile(ufile);
							ufile = "";
						}
						else
						{
							File dd = new File(ufile);
							fi.write(dd);
							// verify the contents of the file
							TikaConfig tika = new TikaConfig();
							Metadata metadata = new Metadata();
							metadata.set(Metadata.RESOURCE_NAME_KEY, dd.toString());
							MediaType mimetype = tika.getDetector().detect(TikaInputStream.get(dd), metadata);
							String mimeTypeString = mimetype.toString();
							MimeTypes allTypes = MimeTypes.getDefaultMimeTypes();
							MimeType type = allTypes.forName(mimeTypeString);
							String realExtension = STR.getFileExtension(type.getExtension()).toLowerCase();
							if (isMatchingMimeType(realExtension, ext))
							{
								_cons.AUDIT.auditLog(sAppName, "Upload File", "client=" + fname + ", server=" + ufile + ", size=" + dd.length());
							}
							else
							{
								err.adderror("File content does not match file extension.");
								Logs.error("File content does not match file extension: real=" + realExtension + " file=" + ext);
								FILE.deleteFile(ufile);
								ufile = "";
							}
						}
						// only allow one file upload
						break;
					}
					else
					{
						err.adderror("Upload file extension not allowed");
						Logs.error("Upload file extension not allowed: " + ext);
						FILE.deleteFile(ufile);
						ufile = "";
						break;
					}
				}
			}
		}
	}
	catch (Exception e)
	{
		Logs.exception(e, "org=", String.valueOf(org), ", fname=", fname, ", ufile=", ufile);
		ufile = "";
	}
	return(ufile);
}

public void uploadfile(
HttpServletRequest _req,
HttpServletResponse _res)
{
	AppVars myCon  = new AppVars();
	try
	{
		myCon.STATS.app = sAppName;
		myCon.STATS.page = "uploadfile";
		myCon.STATS.perfservlet("dopost-start");
		if (SessionData.ValidateSession(_req, myCon))
		{
			// performance logging org and user
			myCon.STATS.org = SessionData.getOrg_Uid(_req, myCon.SS);
			myCon.STATS.user = SessionData.getemailname(_req, myCon.SS);
			myCon.machinename = machinename;
			myCon.APPCLASSNAME = sAppName;
			// set up audit logging
			myCon.AUDIT.m_auditjndi = SessionData.getConnectionString(_req, myCon.SS) + "audit";
			myCon.AUDIT.m_emailname = STR.cn(SessionData.getemailname(_req, myCon.SS)).trim();
			myCon.AUDIT.m_agent = STR.cn(SessionData.getAgent(myCon.SS),"Agent Unknown?").trim();
			myCon.AUDIT.m_sessionid = STR.cn(_req.getRequestedSessionId(), "Unknown Session").trim();
			myCon.AUDIT.m_ipaddr = STR.cn(_req.getRemoteAddr(), "Unknown IP").trim();
			myCon.AUDIT.m_appname = sAppName;
			// set the current values of other audit data from the session block
			myCon.AUDIT.m_org = SessionData.getOrg_Uid(_req, myCon.SS);
			myCon.AUDIT.m_sid = SessionData.getSID(_req, myCon.SS);
			myCon.AUDIT.m_gsid = SessionData.getStudentGSID(_req, myCon.SS);
			myCon.AUDIT.m_student = STR.cn(SessionData.getgStudentName(_req, myCon.SS)).trim();
			myCon.AUDIT.m_studenttype = STR.cn(SessionData.getStudentData(_req, myCon.SS).StudentType).trim();
			myCon.AUDIT.m_campus = STR.cn(SessionData.getSchool(_req, myCon.SS)).trim();
			myCon.AUDIT.m_pagenum = SessionData.getgpageNumber(myCon.SS);
			myCon.AUDIT.connect();

			String ufile = "";
			String retscript = "";
			ERR err = new ERR();
			
			int org = SessionData.getOrg_Uid(_req, myCon.SS);
			if (org > 0)
			{
				ufile = getUploadFile(_req, myCon, org, err);
				if (!ufile.equals(""))
				{
					SessionData.putUploadFilename(myCon.SS, ufile);
					SessionData.putDatabaseSession(_req, myCon.SS, false);
					if (isImage())
					{
						String wh = getImageWidthHeight(ufile);
						retscript = "<script>window.parent.uploadComplete(" + wh + ");</script>";
					}
					else
					{
						retscript = "<script>window.parent.uploadComplete();</script>";
					}
				}
				else
				{
					String msg = err.geterror();
					if (msg.equals(""))
					{
						msg = "File upload failed";
					}
					Logs.error("File upload failed: " + msg);
					retscript = "<script>alert('" + msg + "');</script>";
				}
			}
			else
			{
				Logs.error("Invalid org: " + org);
				retscript = "<script>alert('File upload failed');</script>";
			}

			try
			{
				// we will return HTML
				_res.setDateHeader ("Expires", 0);
				_res.setHeader("Cache-Control","no-cache, no-store, must-revalidate");
				_res.setContentType("text/html; charset=UTF-8"); 
				
				// this servlet is meant to be called from a separate form on a page
				// which is multipart/form-data and whose target is an iframe
				// the page is expected to have a javascript function called "uploadComplete"
				// to do whatever the page wants to do after the file has been uploaded
				_res.getWriter().print(retscript);
				_res.getWriter().flush();
				_res.getWriter().close();
			}
			catch (Exception e)
			{
				Logs.exception(e, "org=", String.valueOf(org), ", ufile=", ufile);
			}
		}
		else
		{
			Logs.error("Invalid session");
		}
	}
	catch (Exception e)
	{
		Logs.exception(e, "uploadfile");
	}
	finally
	{
		myCon.STATS.perfservlet("dopost-end");
		myCon.AUDIT.disconnect();
	}
}

String getImageWidthHeight(String ufile)
{
	String wh = "";
	int width=0;
	int height=0;
	String suffix = STR.getFileExtension(ufile);
	Iterator<ImageReader> itera = ImageIO.getImageReadersBySuffix(suffix);
	if (itera.hasNext())
	{
		ImageReader reader = itera.next();
		try
		{
			ImageInputStream stream = new FileImageInputStream(new File(ufile));
			reader.setInput(stream);
			width = reader.getWidth(reader.getMinIndex());
			height = reader.getHeight(reader.getMinIndex());
			if (width>600)
			{
				int sw = width;
				width=600;
				float ratio = (float)width/(float) sw ;
				float sh = height*ratio;
				height=(int)sh;
			}
			wh = String.valueOf(width) + "," + String.valueOf(height);
		}
		catch (IOException e) 
		{
			Logs.error(e.getMessage()); 
		}
		finally 
		{            
			reader.dispose();        
		}
	}
	else 
	{        
		Logs.error(("No reader found for given format: " + suffix));    
	}
	return(wh);
}

}
