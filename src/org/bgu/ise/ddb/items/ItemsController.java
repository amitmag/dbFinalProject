/**
 * 
 */
package org.bgu.ise.ddb.items;
import oracle.jdbc.OracleTypes;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	Connection conn;
	final String username = "amitmag";
	final String password = "abcd";
	final String connectionURL = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle";
	private final String driver = "oracle.jdbc.driver.OracleDriver";

	public void Connect() {
		try {
			Class.forName(this.driver);
			conn = DriverManager.getConnection(this.connectionURL, this.username, this.password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void fileToDataBase(String file_path) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file_path));
			String newLine;
			while ((newLine = br.readLine()) != null) {
				String[] lineValues = newLine.split(",");
				String title = lineValues[0];
				String prodYear = lineValues[1];
				insertToDataBase(title, prodYear);
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public void fileToNoSqlDataBase(String file_path) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file_path));
			String newLine;
			while ((newLine = br.readLine()) != null) {
				String[] lineValues = newLine.split(",");
				String title = lineValues[0];
				String prodYear = lineValues[1];
				insertIntoNoSql(new MediaItems(title, Integer.parseInt(prodYear)));
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void insertToDataBase(String title, String prodYear) {
		if (conn == null) {
			System.out.println("im here!");
			this.Connect();
		}
			
		String query = "INSERT INTO MEDIAITEMS (TITLE,PROD_YEAR) VALUES(?,?)";
		try (PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, title);
			pstmt.setString(2, prodYear);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private List<MediaItems> createMediaList() {
		List<MediaItems> ans = new ArrayList<>();
		if (this.conn == null) {
			Connect();
		}
		String query = "SELECT * FROM MEDIAITEMS ORDER BY MID ASC"; // query
		try (PreparedStatement ps = conn.prepareStatement(query)) {// compiling query in the DB
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				ans.add(new MediaItems(rs.getString(2), rs.getInt(3)));
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ans;
	}

	/**
	 * The function copy all the items(title and production year) from the Oracle
	 * table MediaItems to the System storage. The Oracle table and data should be
	 * used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method = { RequestMethod.GET })
	public void fillMediaItems(HttpServletResponse response) {
		System.out.println("was here");
		ArrayList<MediaItems> media = (ArrayList<MediaItems>) createMediaList();
		for (MediaItems mediaItem : media) {
			System.out.println("here123412413");
			insertIntoNoSql(mediaItem);
		}

		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	private void insertIntoNoSql(MediaItems mediaItem) {
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("projectDB");
			MongoCollection<Document> collection = db.getCollection("MEDIAITEMS");
			Document document = new Document("TITLE", mediaItem.getTitle()).append("YEAR", mediaItem.getProdYear());
			collection.insertOne(document);
			System.out.println("Document inserted successfully");
			mongoClient.close();
			HttpStatus status = HttpStatus.OK;
		} catch (Exception e) {
			System.out.println(e);
		}

	}

	/**
	 * The function copy all the items from the remote file, the remote file have
	 * the same structure as the films file from the previous assignment. You can
	 * assume that the address protocol is http
	 * 
	 * @throws IOException
	 */
	@RequestMapping(value = "fill_media_items_from_url", method = { RequestMethod.GET })
	public void fillMediaItemsFromUrl(@RequestParam("url") String urladdress, HttpServletResponse response)
			throws IOException {
		System.out.println(urladdress);
		InputStream is = new URL(urladdress).openStream();
		FileOutputStream fos = new FileOutputStream("moviesURL.csv");
		int i = IOUtils.copy(is, fos);
		System.out.println(i);
		fileToNoSqlDataBase("moviesURL.csv");
	
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	/**
	 * The function retrieves from the system storage N items, order is not
	 * important( any N items)
	 * 
	 * @param topN
	 *            - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public MediaItems[] getTopNItems(@RequestParam("topn") int topN) {
		ArrayList<MediaItems> mediaItems = new ArrayList<>();
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("projectDB");
			MongoCollection<Document> collection = db.getCollection("MEDIAITEMS");
			FindIterable<Document> iterDoc = collection.find().limit(topN);
			int i = 1;
			Iterator it = iterDoc.iterator();
			while (it.hasNext()) {
				Document docMediaItem = (Document) it.next();
				MediaItems mi = new MediaItems((String) docMediaItem.get("TITLE"), (int) docMediaItem.get("YEAR"));
				mediaItems.add(mi);
			}

		} catch (Exception e) {
			mongoClient.close();
			System.out.println(e);
		}
		MediaItems[] miArray = new MediaItems[mediaItems.size()];
		mediaItems.toArray(miArray);
		return miArray;
	}

}
