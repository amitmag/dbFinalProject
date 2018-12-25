/**
 *
 */
package org.bgu.ise.ddb.history;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

import com.mongodb.client.model.Sorts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author Alex
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController {

    /**
     * The function inserts to the system storage triple(s)(username, title,
     * timestamp). The timestamp - in ms since 1970 Advice: better to insert the
     * history into two structures( tables) in order to extract it fast one with the
     * key - username, another with the key - title
     *
     * @param username
     * @param title
     * @param response
     */
    @RequestMapping(value = "insert_to_history", method = {RequestMethod.GET})
    public void insertToHistory(@RequestParam("username") String username, @RequestParam("title") String title,
                                HttpServletResponse response) {
        System.out.println(username + " " + title);
        try {
            // Create Mongo Client
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            MongoDatabase db = mongoClient.getDatabase("projectDB");

            // Create history collection
            MongoCollection<Document> collection = db.getCollection("HISTORY");

            //Insert to history only if the title and username exist in the DB
            if (isExistTitle(title) && isExistUser(username)) {
                // Create history document
                Document historyDoc = new Document("TITLE", title).append("USERNAME", username).append("TIMESTAMP",
                        new Date().getTime());

                // Insert document
                collection.insertOne(historyDoc);
                System.out.println("Document inserted successfully");

                // Close DB connection and send OK message to the user
                mongoClient.close();
                HttpStatus status = HttpStatus.OK;
                response.setStatus(status.value());
            }
            // If title of username not exist in the DB, send error to the user
            else {
                HttpStatus status = HttpStatus.CONFLICT;
                response.setStatus(status.value());
            }

            // Close DB connection
            mongoClient.close();

        } catch (Exception e) {
            HttpStatus status = HttpStatus.CONFLICT;
            response.setStatus(status.value());
            System.out.println(e);
            mongoClient.close();
        }
    }

    /**
     * The function retrieves users' history The function return array of pairs
     * <title,viewtime> sorted by VIEWTIME in descending order
     *
     * @param username
     * @return
     */
    @RequestMapping(value = "get_history_by_users", headers = "Accept=*/*", method = {
            RequestMethod.GET}, produces = "application/json")
    @ResponseBody
    @org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
    public HistoryPair[] getHistoryByUser(@RequestParam("entity") String username) {
        ArrayList<HistoryPair> hisroyPairs = new ArrayList<>();
        MongoClient mongoClient = null;
        try {
            // Create Mongo Client
            mongoClient = new MongoClient("localhost", 27017);
            MongoDatabase db = mongoClient.getDatabase("projectDB");

            // Create history collection
            MongoCollection<Document> collection = db.getCollection("HISTORY");

            // Find documents according to condition
            FindIterable<Document> iterDoc = collection.find(eq("USERNAME", username))
                    .sort(new BasicDBObject("TIMESTAMP", -1));
            Iterator it = iterDoc.iterator();

            // Create history result collection
            while (it.hasNext()) {
                Document docHist = (Document) it.next();
                HistoryPair hp = new HistoryPair((String) docHist.get("TITLE"),
                        new Date((long) docHist.get("TIMESTAMP")));
                hisroyPairs.add(hp);
            }

            // Close DB connection
            mongoClient.close();

        } catch (Exception e) {
            mongoClient.close();
            System.out.println(e);
        }
        // Convert list to array
        HistoryPair[] HistoryPairsArray = new HistoryPair[hisroyPairs.size()];
        hisroyPairs.toArray(HistoryPairsArray);
        return HistoryPairsArray;
    }

    /**
     * The function retrieves items' history The function return array of pairs
     * <username,viewtime> sorted by VIEWTIME in descending order
     *
     * @param title
     * @return
     */
    @RequestMapping(value = "get_history_by_items", headers = "Accept=*/*", method = {
            RequestMethod.GET}, produces = "application/json")
    @ResponseBody
    @org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
    public HistoryPair[] getHistoryByItems(@RequestParam("entity") String title) {
        ArrayList<HistoryPair> hisroyPairs = new ArrayList<>();
        MongoClient mongoClient = null;
        try {
            // Create Mongo Client
            mongoClient = new MongoClient("localhost", 27017);
            MongoDatabase db = mongoClient.getDatabase("projectDB");

            // Create history collection
            MongoCollection<Document> collection = db.getCollection("HISTORY");

            // Find documents according to condition
            FindIterable<Document> iterDoc = collection.find(eq("TITLE", title))
                    .sort(new BasicDBObject("TIMESTAMP", -1));

            // Create history result collection
            Iterator it = iterDoc.iterator();
            while (it.hasNext()) {
                Document docHist = (Document) it.next();
                HistoryPair hp = new HistoryPair((String) docHist.get("USERNAME"),
                        new Date((long) docHist.get("TIMESTAMP")));
                hisroyPairs.add(hp);
            }

            // Close DB connection
            mongoClient.close();
        } catch (Exception e) {
            mongoClient.close();
            System.out.println(e);
        }

        // Convert list to array
        HistoryPair[] HistoryPairsArray = new HistoryPair[hisroyPairs.size()];
        hisroyPairs.toArray(HistoryPairsArray);
        return HistoryPairsArray;
    }

    /**
     * The function retrieves all the users that have viewed the given item
     *
     * @param title
     * @return
     */
    @RequestMapping(value = "get_users_by_item", headers = "Accept=*/*", method = {
            RequestMethod.GET}, produces = "application/json")
    @ResponseBody
    @org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
    public User[] getUsersByItem(@RequestParam("title") String title) {
        List<String> usersName = new ArrayList<>();
        ArrayList<User> users = new ArrayList<>();
        MongoClient mongoClient = null;
        try {
            // Create Mongo Client
            mongoClient = new MongoClient("localhost", 27017);
            MongoDatabase db = mongoClient.getDatabase("projectDB");

            // Create history collection
            MongoCollection<Document> collection = db.getCollection("HISTORY");

            // Find document with the titles
            FindIterable<Document> iterDoc = collection.find(eq("TITLE", title));
            Iterator it = iterDoc.iterator();
            while (it.hasNext()) {
                Document docHist = (Document) it.next();
                usersName.add((String) docHist.get("USERNAME"));
            }
            // Get users collection
            MongoCollection<Document> collUsers = db.getCollection("USERS");

            // Create user for each username
            for (String username : usersName) {
                Document myDoc = collUsers.find(eq("username", username)).first();
                User user = new User((String) myDoc.get("username"), (String) myDoc.get("firstName"),
                        (String) myDoc.get("lastName"));
                users.add(user);
            }

            // Close DB connection
            mongoClient.close();

        } catch (Exception e) {
            mongoClient.close();
            System.out.println(e);
        }

        // Convert list to array
        User[] userArr = new User[users.size()];
        users.toArray(userArr);
        return userArr;
    }

    /**
     * The function calculates the similarity score using Jaccard similarity
     * function: sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|, where U(i)
     * is the set of usernames which exist in the history of the item i.
     *
     * @param title1
     * @param title2
     * @return
     */
    @RequestMapping(value = "get_items_similarity", headers = "Accept=*/*", method = {
            RequestMethod.GET}, produces = "application/json")
    @ResponseBody
    public double getItemsSimilarity(@RequestParam("title1") String title1, @RequestParam("title2") String title2) {
        double ret = 0.0;

        // Get users array for items
        User[] userForTitle1 = getUsersByItem(title1);
        User[] userForTitle2 = getUsersByItem(title2);

        // Get union and intersection size
        int unionSize = getUnionSize(userForTitle1, userForTitle2);
        int intersectionSize = getIntersectionSize(userForTitle1, userForTitle2);

        // If union is zero, return 0
        if (unionSize == 0)
            return ret;
        ret = (double) intersectionSize / unionSize;
        return ret;
    }

    /**
     * Return the union size of 2 arrays
     * @param arr1
     * @param arr2
     * @return
     */
    private int getUnionSize(User[] arr1, User[] arr2) {
        HashSet<String> users = new HashSet<>();
        // Add all usernames to Set
        for (User user : arr1) {
            users.add(user.getUsername());
        }
        // Add only distinct values to array
        for (User user : arr2) {
            if (!users.contains(user.getUsername()))
                users.add(user.getUsername());
        }
        return users.size();
    }

    /**
     * Return the intersection size of 2 arrays
     * @param arr1
     * @param arr2
     * @return
     */
    private int getIntersectionSize(User[] arr1, User[] arr2) {
        HashSet<String> users = new HashSet<>();
        int intersectionSize = 0;
        // Add all usernames to Set
        for (User user : arr1) {
            users.add(user.getUsername());
        }
        // Check for common values
        for (User user : arr2) {
            if (users.contains(user.getUsername()))
                intersectionSize++;
        }
        return intersectionSize;
    }

    // Check if user exist in DB
    private boolean isExistUser(String username) {
        boolean result = true;
        MongoClient mongoClient = null;
        try {
            // Create Mongo Client
            mongoClient = new MongoClient("localhost", 27017);
            MongoDatabase db = mongoClient.getDatabase("projectDB");
            MongoCollection<Document> collection = db.getCollection("USERS");
            Document myDoc = collection.find(eq("username", username)).first();
            if (myDoc == null) {
                mongoClient.close();
                return false;
            }

            // Close DB connection
            mongoClient.close();

        } catch (Exception e) {
            mongoClient.close();
            System.out.println(e);
        }
        return result;
    }

    // Check if movie exist in DB
    private boolean isExistTitle(String title) {
        boolean result = true;
        MongoClient mongoClient = null;
        try {
            // Create Mongo Client
            mongoClient = new MongoClient("localhost", 27017);
            MongoDatabase db = mongoClient.getDatabase("projectDB");
            MongoCollection<Document> collection = db.getCollection("MEDIAITEMS");
            Document myDoc = collection.find(eq("TITLE", title)).first();
            if (myDoc == null) {
                mongoClient.close();
                return false;
            }

            // Close DB connection
            mongoClient.close();

        } catch (Exception e) {
            mongoClient.close();
            System.out.println(e);
        }
        return result;
    }

}
