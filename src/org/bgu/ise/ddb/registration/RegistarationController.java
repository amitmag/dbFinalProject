package org.bgu.ise.ddb.registration;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import org.bson.Document;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.time.*;

import com.mongodb.client.MongoCursor;

import java.util.ArrayList;
import java.util.List;

import java.io.IOException;
import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController {

	/**
	 * The function checks if the username exist, in case of positive answer
	 * HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT, else
	 * insert the user to the system and set to HttpStatus in HttpServletResponse
	 * HttpStatus.OK
	 * 
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method = { RequestMethod.POST })
	public void registerNewUser(@RequestParam("username") String username, @RequestParam("password") String password,
			@RequestParam("firstName") String firstName, @RequestParam("lastName") String lastName,
			HttpServletResponse response) {
		System.out.println(username + " " + password + " " + lastName + " " + firstName);
		// :TODO your implementation
		// Creating a Mongo client
		try {
			if (isExistUser(username)) {
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			} else {
				MongoClient mongoClient = new MongoClient("localhost", 27017);
				MongoDatabase db = mongoClient.getDatabase("projectDB");
				MongoCollection<Document> collection = db.getCollection("USERS");
				Document document = new Document("username", username).append("password", password)
						.append("firstName", firstName).append("lastName", lastName)
						.append("RegistrationDate", new Date());
				collection.insertOne(document);
				System.out.println("Document inserted successfully");
				mongoClient.close();
				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
			}
		} catch (Exception e) {
			System.out.println(e);
		}

	}

	/**
	 * The function returns true if the received username exist in the system
	 * otherwise false
	 * 
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method = { RequestMethod.GET })
	public boolean isExistUser(@RequestParam("username") String username) throws IOException {
		System.out.println(username);
		boolean result = true;
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("projectDB");
			MongoCollection<Document> collection = db.getCollection("USERS");
			Document myDoc = collection.find(eq("username", username)).first();
			if (myDoc == null) {
				mongoClient.close();
				return false;
			}

		} catch (Exception e) {
			mongoClient.close();
			System.out.println(e);
		}
		return result;
	}

	/**
	 * The function returns true if the received username and password match a
	 * system storage entry, otherwise false
	 * 
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method = { RequestMethod.POST })
	public boolean validateUser(@RequestParam("username") String username, @RequestParam("password") String password)
			throws IOException {
		System.out.println(username + " " + password);
		boolean result = false;
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("projectDB");
			MongoCollection<Document> collection = db.getCollection("USERS");
			Document myDoc = collection.find(eq("username", username)).first();
			if (myDoc == null) {
				mongoClient.close();
				return false;
			} else {
				mongoClient.close();
				return myDoc.get("password").equals(password);
			}
		} catch (Exception e) {
			mongoClient.close();
			System.out.println(e);
		}
		return result;
	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * 
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "get_number_of_registred_users", method = { RequestMethod.GET })
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException {
		System.out.println(days + "");
		int result = 0;
		// :TODO your implementation
		MongoClient mongoClient = null;
		try {
			Date startDate = new Date();
			startDate.setDate(startDate.getDate() - days);
			startDate.setHours(0);
			startDate.setMinutes(0);
			startDate.setSeconds(0);
			mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("projectDB");
			MongoCollection<Document> collection = db.getCollection("USERS");
			FindIterable<Document> iterDoc = collection.find(gt("RegistrationDate", startDate));
			Iterator it = iterDoc.iterator();
			while (it.hasNext()) {
				it.next();
				result++;
			}
		} catch (Exception e) {
			mongoClient.close();
			System.out.println(e);
		}
		return result;

	}

	/**
	 * The function retrieves all the users
	 * 
	 * @return
	 */
	@RequestMapping(value = "get_all_users", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public User[] getAllUsers() {
		ArrayList<User> users = new ArrayList<>();
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("projectDB");
			MongoCollection<Document> collection = db.getCollection("USERS");
			FindIterable<Document> iterDoc = collection.find();
			int i = 1;
			Iterator it = iterDoc.iterator();
			while (it.hasNext()) {
				Document docUser = (Document) it.next();
				User user = new User((String) docUser.get("username"), (String) docUser.get("firstName"),
						(String) docUser.get("lastName"));
				users.add(user);
			}

		} catch (Exception e) {
			mongoClient.close();
			System.out.println(e);
		}
		User[] usersArray = new User[users.size()];
		users.toArray(usersArray);
		return usersArray;
	}
}
