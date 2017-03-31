package databaseMongoDB;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCursor;

/**
 * @author NAPAT PAOPONGPAIBUL This source code was used in my senior project
 *         2016 for Education purpose ONLY
 * @description
 */
public class SSHTunnel {
	// forwarding ports
	private static final String LOCAL_HOST = "localhost";
	private static final String REMOTE_HOST = "127.0.0.1";
	private static final Integer LOCAL_PORT = 27018;
	private static final Integer REMOTE_PORT = 27017; // Default mongodb port

	// ssh connection info
	private static final String SSH_USER = "root";
	private static final String SSH_PASSWORD = "pokemont05";
	private static final String SSH_HOST = "seniorproject-api.tk";
	private static final Integer SSH_PORT = 22;

	private static Session SSH_SESSION;

	public static void main(String[] args) {
		try {
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			SSH_SESSION = null;
			SSH_SESSION = jsch.getSession(SSH_USER, SSH_HOST, SSH_PORT);
			SSH_SESSION.setPassword(SSH_PASSWORD);
			SSH_SESSION.setConfig(config);
			SSH_SESSION.connect();
			SSH_SESSION.setPortForwardingL(LOCAL_PORT, REMOTE_HOST, REMOTE_PORT);

			MongoClient mongoClient = new MongoClient(LOCAL_HOST, LOCAL_PORT);
			mongoClient.setReadPreference(ReadPreference.nearest());
			MongoCursor<String> dbNames = mongoClient.listDatabaseNames().iterator();
			while (dbNames.hasNext()) {
				System.out.println(dbNames.next());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				SSH_SESSION.delPortForwardingL(LOCAL_PORT);
			} catch (JSchException e) {
				e.printStackTrace();
			}
			SSH_SESSION.disconnect();
		}

	}

}
