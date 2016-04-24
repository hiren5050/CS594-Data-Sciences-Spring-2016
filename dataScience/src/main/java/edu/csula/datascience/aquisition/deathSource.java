package edu.csula.datascience.aquisition;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class deathSource {

	private static final int BUFFER_SIZE = 4096;

	public static void main(String args[]) {
		try {
			download();
		} catch (Exception E) {
			System.out.println("error");
		}
	}

	public static void download() throws IOException {
		URL url = new URL(
				"https://kaggle2.blob.core.windows.net/datasets/28/32/DeathRecords.zip?sv=2012-02-12&se=2016-04-24T22%3A59%3A33Z&sr=b&sp=r&sig=0MJiLWzfuwHCNHaRPZN4imtJwYVL%2FSGzAVGt7eVVcdU%3D");
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
		String zipFilePath = "C:\\cs594\\workspace\\data_science\\Source.zip";
		String destDirectory = "C:/cs594/workspace/data_science/source";

		unzip(zipFilePath, destDirectory);

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

}
