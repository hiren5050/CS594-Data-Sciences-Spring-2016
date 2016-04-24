package edu.cs594.collection;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;


public class Database {
	
	MongoClient mongo = null;
	DB db = null;
	DBCollection collection = null;
	
	public Database()
	{
		try{
			mongo = new MongoClient("localhost",27017);
			db = mongo.getDB("Collector");
			collection = db.getCollection("data");
		}
		catch (MongoException e)
		{
			e.printStackTrace();
		}
	}

}
