package edu.csula.datascience.aquisition;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.mongodb.BasicDBObject;

public class deathSource extends mongoDB implements Source<String> {

	private static final int BUFFER_SIZE = 4096;

	public boolean hasNext1() {
		// TODO Auto-generated method stub
		return false;
	}

	public Collection<String> next1() {
		// TODO Auto-generated method stub
		return null;
	}

	/* Download Resources From Kaggle */
	public void downloadDeathRecords(String path) throws IOException {
		URL url = new URL(path);

		HttpURLConnection con = (HttpURLConnection) url.openConnection();

		// Check for errors
		int responseCode = con.getResponseCode();
		InputStream inputStream;
		if (responseCode == HttpURLConnection.HTTP_OK) {
			System.out.println("OK");
			inputStream = con.getInputStream();
		} else {
			inputStream = con.getErrorStream();
		}

		OutputStream output = new FileOutputStream("Source.zip");
		// Process the response
		BufferedReader reader;
		String line = null;

		byte[] buffer = new byte[8 * 1024]; // Or whatever
		int bytesRead;
		while ((bytesRead = inputStream.read(buffer)) > 0) {
			output.write(buffer, 0, bytesRead);
		}

		output.close();
		inputStream.close();
		String zipFilePath = "C:\\Users\\Public.DESKTOP-I3193S1\\git\\CS594-Data-Sciences-Spring-2016\\dataScience\\Source.zip";
		String destDirectory = "C:\\Users\\Public.DESKTOP-I3193S1\\git\\CS594-Data-Sciences-Spring-2016\\dataScience\\Source";

		unzip(zipFilePath, destDirectory);
		String csvFile = "C:\\Users\\Public.DESKTOP-I3193S1\\git\\CS594-Data-Sciences-Spring-2016\\dataScience\\Source\\DeathRecords.csv";
		BufferedReader br = null;
		String line1 = "";
		String cvsSplitBy = ",";

		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line1 = br.readLine()) != null) {

				BasicDBObject document = new BasicDBObject();
				// use comma as separator
				String[] Agetype = line1.split(cvsSplitBy);

				document.put("Month of Date", Agetype[7]);
				document.put("Sex ", Agetype[8]);
				document.put("Age", Agetype[10]);
				document.put("Place of Death", Agetype[14]);
				document.put("Maratile Status", Agetype[15]);
				document.put("Year", Agetype[17]);
				document.put("Mannar of Death", Agetype[19]);

				collection.insert(document);
				System.out.println("Agetype [MonthofDate= " + Agetype[7] + " , Sex=" + Agetype[8] + ", Age ="
						+ Agetype[10] + ",PlaceofDeath= " + Agetype[14] + ", Maratile Status = " + Agetype[15]
						+ ",year = " + Agetype[17] + ", Mannar of Death = " + Agetype[19] + "] ");

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		System.out.println("Done");
	}

	/* Download Resources From www.data.gov And Github User Content */
	public void downloadDeathRecords(String path1, String path2, String path3) throws IOException {
		URL url1 = new URL(path1);
		URL url2 = new URL(path2);
		URL url3 = new URL(path3);

		HttpURLConnection con1 = (HttpURLConnection) url1.openConnection();
		HttpURLConnection con2 = (HttpURLConnection) url2.openConnection();
		HttpURLConnection con3 = (HttpURLConnection) url3.openConnection();

		// Check for errors
		int responseCode1 = con1.getResponseCode();
		int responseCode2 = con2.getResponseCode();
		int responseCode3 = con3.getResponseCode();
		InputStream inputStream;
		if (responseCode1 == HttpURLConnection.HTTP_OK && responseCode2 == HttpURLConnection.HTTP_OK
				&& responseCode3 == HttpURLConnection.HTTP_OK) {
			System.out.println("OK");
			inputStream = con1.getInputStream();
			inputStream = con2.getInputStream();
			inputStream = con3.getInputStream();

		} else {
			inputStream = con1.getErrorStream();
			inputStream = con2.getErrorStream();
			inputStream = con3.getErrorStream();
		}

		OutputStream output = new FileOutputStream("death.csv");
		// Process the response
		BufferedReader reader;
		String line = null;

		byte[] buffer = new byte[8 * 1024]; // Or whatever
		int bytesRead;
		while ((bytesRead = inputStream.read(buffer)) > 0) {
			output.write(buffer, 0, bytesRead);
		}

		output.close();
		inputStream.close();

	}

	public static void unzip(String zipFilePath, String destDirectory) throws IOException {
		File destDir = new File(destDirectory);
		if (!destDir.exists()) {
			destDir.mkdir();
		}
		ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
		ZipEntry entry = zipIn.getNextEntry();
		// iterates over entries in the zip file
		while (entry != null) {
			String filePath = destDirectory + File.separator + entry.getName();
			if (!entry.isDirectory()) {
				// if the entry is a file, extracts it
				extractFile(zipIn, filePath);
			} else {
				// if the entry is a directory, make the directory
				File dir = new File(filePath);
				dir.mkdir();
			}
			zipIn.closeEntry();
			entry = zipIn.getNextEntry();
		}
		zipIn.close();
	}

	private static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
		byte[] bytesIn = new byte[BUFFER_SIZE];
		int read = 0;
		while ((read = zipIn.read(bytesIn)) != -1) {
			bos.write(bytesIn, 0, read);
		}
		bos.close();
	}

	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	public Collection<String> next() {
		// TODO Auto-generated method stub
		return null;
	}

	public void downloadDeathRecords() {
		// TODO Auto-generated method stub

	}

}
