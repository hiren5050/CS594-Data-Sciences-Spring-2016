package edu.cs594.collection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Collector extends Database {

	public static void main(String[] args) {

		Collector obj = new Collector();
		obj.run();

	}

	public void run() {

		String csvFile = "H:/CS594(Data Science)/DeathRecords(1)/DeathRecords.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				BasicDBObject document = new BasicDBObject();
				// use comma as separator
				String[] Agetype = line.split(cvsSplitBy);
				

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

}
