package com.esped.apps.US;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
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
import com.esped.lib.Logs;
import com.esped.lib.STR;
import com.esped.login.SessionData;

public class UploadDat extends Upload {

	private String sAppName = "";
	private Vector<String> vAllowedExtensions = new Vector<String>();

	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		sAppName = this.getClass().getName();
		vAllowedExtensions.add("txt");
	}

	public String getUploadFile(HttpServletRequest _req, AppVars _cons, int org, ERR err) {
		String ufile = "";
		DiskFileItemFactory factory = new DiskFileItemFactory();
		ServletFileUpload SFU = new ServletFileUpload(factory);
		List<FileItem> items = null;
		String fname = "";
		try {
			// don't use this - causes a http connection reset
			// TODO: might want to set this to a really high value
			// SFU.setFileSizeMax(maxUploadSize());
			items = SFU.parseRequest(_req);
			if (items != null) {
				for (FileItem fi : items) {
					if (!fi.isFormField()) {
						fname = fi.getName();
						long fsize = fi.getSize();
						String ext = STR.getFileExtension(fname);
						if (ext.equals("dat"))
							ext = "txt";
						if (isAllowedExtension(ext)) {
							// if we've already uploaded a file in this session,
							// use the same name
							// to prevent maliciously uploading tons of files to
							// the server
							// just need to change the file extension to match
							// the incoming file name
							ufile = SessionData.getUploadFilename(_cons.SS);
							if (ufile.equals("")) {
								ufile = generateUniqueUploadFile(org);
							}
							ufile = STR.changeFileExtension(ufile, ext);
							// make sure the file is under the correct size
							if (fsize > maxUploadSize()) {
								err.adderror("Maximum upload file size exceeded.");
								Logs.error("Maximum upload file size exceeded");
								FILE.deleteFile(ufile);
								ufile = "";
							} else {
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
								if (isMatchingMimeType(realExtension, ext)) {
									_cons.AUDIT.auditLog(sAppName, "Upload File",
											"client=" + fname + ", server=" + ufile + ", size=" + dd.length());
								} else {
									err.adderror("File content does not match file extension.");
									Logs.error("File content does not match file extension: real=" + realExtension
											+ " file=" + ext);
									FILE.deleteFile(ufile);
									ufile = "";
								}
							}
							// only allow one file upload
							break;
						} else {
							err.adderror("Upload file extension not allowed");
							Logs.error("Upload file extension not allowed: " + ext);
							FILE.deleteFile(ufile);
							ufile = "";
							break;
						}
					}
				}
			}
		} catch (Exception e) {
			Logs.exception(e, "org=", String.valueOf(org), ", fname=", fname, ", ufile=", ufile);
			ufile = "";
		}
		return (ufile);
	}

	public void service(HttpServletRequest _req, HttpServletResponse _res) throws ServletException, IOException {
		uploadfile(_req, _res);
	}

	public void doPost(HttpServletRequest _req, HttpServletResponse _res) throws ServletException, IOException {
		uploadfile(_req, _res);
	}

	public int maxUploadSize() {
		return (100000000);
	}

	public boolean isAllowedExtension(String ext) {
		boolean isAllowed = vAllowedExtensions.contains(ext.toLowerCase());
		return (isAllowed);
	}

}
