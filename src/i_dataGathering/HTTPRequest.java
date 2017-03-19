package i_dataGathering;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * @author NAPAT PAOPONGPAIBUL
 * This source code was used in my senior project 2016 for Education purpose ONLY
 * @description 
 */
public class HTTPRequest {
	private static final Logger logger = LogManager.getLogger(HTTPRequest.class);
	private static final String USER_AGENT = "Mozilla/5.0";

	public static String sendGet(String url) throws IOException {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("GET");
		// request header
		con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		logger.info("Establish connection to " + url);
		logger.info("Response code : " + responseCode);

		// receive file
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
		String inputLine;
		String response = "";

		while ((inputLine = in.readLine()) != null) {
			response = response + inputLine + "\n";
		}
		in.close();
		return response;
	}

	private static InputStream getFileUsingCookie(String url, String cookieList) throws IOException {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("GET");
		// request header
		con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("cookie", cookieList);

		int responseCode = con.getResponseCode();
		logger.info("Establish connection to " + url);
		logger.info("Response code : " + responseCode);

		// receive file
		InputStream in = new BufferedInputStream(con.getInputStream());
		// receive HTML content
		// BufferedReader in_html = new BufferedReader(new
		// InputStreamReader(con.getInputStream(), "UTF-8"));
		// String inputLine;
		// String response = "";
		//
		// while ((inputLine = in_html.readLine()) != null) {
		// response = response + inputLine + "\n";
		// }
		// System.out.println(response);
		return in;
	}

	private static void saveFileFromInputSteam(InputStream in, String fileName) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buf = new byte[1024];
			int n = 0;
			while (-1 != (n = in.read(buf))) {
				out.write(buf, 0, n);
			}
			out.close();
			in.close();
			byte[] response = out.toByteArray();

			FileOutputStream fos = new FileOutputStream(fileName);
			fos.write(response);
			fos.close();
		} catch (Exception e) {
			logger.error("Cannot save input steam to target file");
			e.printStackTrace();
		}
	}

	public static void getExcelFileFromMaybank(String url, String fileName, String cookieList) throws Exception {
		try {
			InputStream in = getFileUsingCookie(url, cookieList);
			saveFileFromInputSteam(in, fileName);
		} catch (Exception e) {
			logger.error("Cannot received any file from http://sse.maybank-ke.co.th/");
			e.printStackTrace();
		}
		String everything = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			everything = sb.toString();
			br.close();
		} catch (Exception e) {
			logger.error("Cannot read file from http://sse.maybank-ke.co.th/");
			e.printStackTrace();
		}
		if (everything == "" || everything.contains("You have not logged in yet")) {
			logger.error("Cannot connect to http://sse.maybank-ke.co.th/ with current cookie");
			throw new Exception("Cannot connect to http://sse.maybank-ke.co.th/ with current cookie");
		} else {
			logger.info("Completed save file to " + fileName);
		}
	}
}
