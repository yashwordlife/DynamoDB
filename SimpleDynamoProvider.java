package edu.buffalo.cse.cse486586.simpledynamo;



import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;
import android.view.InputQueue;

import org.w3c.dom.Text;

public class SimpleDynamoProvider extends ContentProvider {
	public ArrayList<String> sortedPortList = new ArrayList<String>(Arrays.asList(new String[] {"5554","5556","5558","5560","5562"}));
	public String myPort, nodeId;
	public Uri mUri;
	public ConcurrentMap<String, ArrayList<String>> singleQueryResult = new ConcurrentHashMap<String, ArrayList<String>>();
	public Map<String, ArrayList<String>> multipleQueryResult = new HashMap<String, ArrayList<String>>();
	public Set<String> listOfLocalInsertedKeys = new HashSet<String>();
	public Map<String,ArrayList<String>> insertSuccess = new HashMap<String, ArrayList<String>>();
	public int queryAllCounter = 0;
	String colNames[] = {"key", "value"};
	public String inQuery = "Out";


	static final int SERVER_PORT = 10000;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stu
		if (selection.equals("@")) {
			// If selection is @, tterate through each of the local AVD key value pairs and delete them all
			for (String key : listOfLocalInsertedKeys) {
				String keyHash = null;
				try {
					keyHash = genHash(key);
					List<String> successors = getSuccessors(keyHash);
					for (String successor : successors)
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "D;"+ key, successor);

				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
			listOfLocalInsertedKeys.clear();
		}
		// If the selection string is *, remove all the local keys and remote keys
		// and notify other AVDs to remove all the keys
		else if (selection.equals("*")) {
			listOfLocalInsertedKeys.clear();
			for (String port : sortedPortList)
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DALL;"+ myPort, port);
		}
		// If the selection string is the key, remove the key locally and inform other AVDs to remove
		// from their remote keys
		else {
			if (listOfLocalInsertedKeys.contains(selection)) listOfLocalInsertedKeys.remove(selection);
			try {
				String keyHash = genHash(selection);
				List<String> successors = getSuccessors(keyHash);
				for (String successor : successors)
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "D;"+ selection, successor);

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		// The following code implements the insert functionality
		// Reference : PA2A Code
		Log.d("Insert called", values.toString());
		String key = values.get("key").toString();
		String value = values.get("value").toString();
		String keyHash = null;
		try {
			// Generate the SHA-1 hash of the key
			// Reference : PA3
			// The following code computes all the three successors for the key and notifies them to
			// insert and waits for atleast two acknowledgements from the successors
			keyHash = genHash(key);
			Log.d("Insert Q", keyHash);
			List<String> successors = getSuccessors(keyHash);
			List<String> insertAcks = new ArrayList<String>();
			synchronized (insertAcks) {
				while (insertAcks.size() <= 1) {
					for (String successor : successors) {
						String ack = getAnswer("IRQ;" + key + "-" + value + ";" + myPort, successor);
						if (ack != null && ack.equals("InsertSuccess")) insertAcks.add(ack);
					}
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return uri;
	}

	public List<String> getSuccessors(String keyHash) throws NoSuchAlgorithmException {
		/*
		The following code computes all the three successors for a particular key and returns
		the list of ports
		 */
		int sucIndex = 0;
		for (int i = 0; i < sortedPortList.size(); i++) {
			if (keyHash.compareTo(genHash(sortedPortList.get(sortedPortList.size()-1))) > 0 ||
			keyHash.compareTo(genHash(sortedPortList.get(0))) < 0 ) {
				sucIndex = 0; break;
			}
			else if (keyHash.compareTo(genHash(sortedPortList.get(i))) > 0 &&
					keyHash.compareTo(genHash(sortedPortList.get(i+1))) <= 0) {
				sucIndex = i+1; break;
			}
		}
		List<String> tempList = new ArrayList<String>();
		tempList.add(sortedPortList.get(sucIndex));
		tempList.add(sortedPortList.get((sucIndex + 1) % sortedPortList.size()));
		tempList.add(sortedPortList.get((sucIndex + 2) % sortedPortList.size()));
		Log.d("Successor list",keyHash+":"+Arrays.toString(tempList.toArray()));
		return tempList;
	}
	public void setUpServerAndPort() throws NoSuchAlgorithmException {

		/*
		 * Calculate the port number that this AVD listens on.
		 * Reference : PA1 Code
		 */
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)));
		// Generate the node id from emulator port,
		// e.g. If emulator port = 11108, node id = genHash(11108/2), i.e. genHash(5554)
		nodeId = genHash(String.valueOf(Integer.parseInt(myPort) / 2));
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		/*
		 * Create a server socket as well as a thread (AsyncTask) that listens on the server
		 * port.
		 *
		 * AsyncTask is a simplified thread construct that Android provides.
		 * http://developer.android.com/reference/android/os/AsyncTask.html
		 * Reference : PA1
		 */
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(SERVER_PORT);
		} catch (IOException e) {
			e.printStackTrace();
			Log.e("Unable to create server",myPort);
		}
		new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Socket clientConnection = null;
			DataInputStream dataInputStream = null;
			DataOutputStream dataOutputStream = null;
				while (true) {
					try {
						/*
						 * The following code listens for incoming connections and accepts if a connection attempt is made
						 * and reads the received string data and sends
						 * then sends an acknowledgement back to the client
						 * References :
						 * 1. https://stackoverflow.com/questions/7384678/how-to-create-socket-connection-in-android
						 * 2. https://stackoverflow.com/questions/17440795/send-a-string-instead-of-byte-through-socket-in-java
						 * 3. https://developer.android.com/reference/android/os/AsyncTask
						 * 4. PA1 Code
						 */

						clientConnection = serverSocket.accept();
						dataInputStream = new DataInputStream(clientConnection.getInputStream());
						String readString = dataInputStream.readUTF();
						String ack = "Server Ack";
						if (readString.split(";")[0].equals("QSS")) {
							// If the message received from client is Single query key
							// retrieve the value for the key and reply back to the requesting
							// AVD
							String requestedKey = readString.split(";")[1];
							String valAndVersion = retrieveFromFile(requestedKey);
							if (valAndVersion != null) {
								ack = valAndVersion;
							}
							else
								ack = requestedKey;
						}
						else if (readString.split(";")[0].equals("IRQ")) {
							// If the message received from client is Insert,
							// insert locally into the AVD
							String[] msgParts = readString.split(";")[1].split("-");
							processInsert(msgParts[0],msgParts[1]);
							ack = "InsertSuccess";
						}
						Log.d("Server read", readString);
						dataOutputStream = new DataOutputStream(clientConnection.getOutputStream());
						dataOutputStream.writeUTF(ack);
						processMessage(readString);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						try {
							if (clientConnection != null) clientConnection.close();
							if (dataInputStream != null) dataInputStream.close();
							if (dataOutputStream != null) dataOutputStream.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
		}

		protected void onProgressUpdate(String... strings) {
			return;
		}

		public void processMessage(String message) {
			// The following code processes the message at the server
			String[] parts = message.split(";");
			String messageType = parts[0];
			String messageContent = parts[1];
			Log.d("RECEIVED:", message);
			if (messageType.equals("QALL")) {
				// If the message type is QALL - query all, send all the key val pairs to the
				// requesting AVD
				String requestedPort = parts[1];
				if (listOfLocalInsertedKeys.size() == 0) {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QALLREP;Invalid-Invalid:0", requestedPort);
				}
				else {
					List<String> listOfKVV = new ArrayList<String>();
					for (String key : listOfLocalInsertedKeys) {
						String valVer = retrieveFromFile(key);
						if (valVer != null) listOfKVV.add(key+"-"+ valVer);
					}
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QALLREP;" + TextUtils.join(",", listOfKVV), requestedPort);
				}
			}
			else if (messageType.equals("QALLREP")) {
				processMultipleQueryResponse(messageContent);
			}
			else if (messageType.equals("D")) {
				if (listOfLocalInsertedKeys.contains(parts[1])) listOfLocalInsertedKeys.remove(parts[1]);
			}
			else if (messageType.equals("DALL")) {
				listOfLocalInsertedKeys.clear();
			}
			else if (messageType.equals("COPYALL")) {
				processCopyAllRequest(parts[1]);
			}
			else if (messageType.equals("COPYALLREP")) {
				try {
					processCopyAllResponse(messageContent);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}

		}
		public synchronized void processCopyAllRequest(String requestedPort) {
			// Creates a response by retrieving all the local key value pairs and replies back to the
			// requesting AVD
				List<String> listOfKVV = new ArrayList<String>();
				if (listOfLocalInsertedKeys.size() == 0) {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "COPYALLREP;Invalid-Invalid:0", requestedPort);
				} else {
					for (String key : listOfLocalInsertedKeys) {
						String valVer = retrieveFromFile(key);
						if (valVer != null) listOfKVV.add(key + "-" + valVer);
					}
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "COPYALLREP;" + TextUtils.join(",", listOfKVV), requestedPort);
				}
		}
		public synchronized void processCopyAllResponse(String messageContent) throws NoSuchAlgorithmException {
			// When all the key value pairs are retrieved from the other AVDS, insert
			// the ones local to it by calculating the hash of each key
			if (messageContent.contains(",")) {
				String[] listOfKVV = messageContent.split(",");
				for (String kVV : listOfKVV) {
					copyInsert(kVV);
				}
			}
			else {
				copyInsert(messageContent);
			}
		}
		public synchronized void copyInsert(String messageContent) throws NoSuchAlgorithmException {
			/*
			The following code computes the hash of each key and checks if the local AVD is
			one of the successors, and copies the insert by setting the same version of key-val
			that was received from other AVDs
			 */
			String []parts = messageContent.split("-");
			String key = parts[0];
			String[] valVersion = parts[1].split(":");
			String val = valVersion[0];
			int version = Integer.parseInt(valVersion[1]);
			String keyHash = genHash(key);
			List<String> listOfSuc = getSuccessors(keyHash);
			if (!key.equals("Invalid") && listOfSuc.contains(myPort)) {
				if (listOfLocalInsertedKeys.contains(key)) {
					String valAndVer = retrieveFromFile(key);
					int savedVersion = Integer.parseInt(valAndVer.split(":")[1]);
					if (version > savedVersion) insertIntoFile(key,val,version);
				}
				else {
					insertIntoFile(key, val, version);
					listOfLocalInsertedKeys.add(key);
				}
			}
		}
		public synchronized void processInsert(String key, String value) {
			insertIntoFile(key, value, -1);
			listOfLocalInsertedKeys.add(key);
		}
		public synchronized void processMultipleQueryResponse(String messageContent) {
			ArrayList<String> tempList = multipleQueryResult.get(myPort);
			tempList.add(messageContent);
			multipleQueryResult.put(myPort, tempList);
		}
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
 	@Override
	public boolean onCreate() {
		Collections.sort(sortedPortList, new HashComparator());
		try {
			setUpServerAndPort();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		// TODO Auto-generated method stub


		// Checks if AVD was alive before this, if it was, copy all key val pairs from other AVDS
		// that belong to the local AVD
		if (retrieveFromFile("Ihavebeenalive") != null) {copyAll();}
		else insertIntoFile("Ihavebeenalive","Yes",-1);
		return true;
	}
	public void copyAll() {
		for (String port : sortedPortList)
			if (!port.equals(myPort)) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"COPYALL;"+myPort, port);

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		Log.d("Query function called", selection);
		MatrixCursor cursor = new MatrixCursor(colNames);
		if (selection.equals("@")) {
			for (String key : listOfLocalInsertedKeys) {
					List<String> results = new ArrayList<String>();
					synchronized (results) {
						while (results.size() <= 2) {
							try {
								List<String> successors = getSuccessors(genHash(key));
								for (String successor : successors) {
									String query = "QSS;" + key;
									String queryAnswer = getAnswer(query, successor);
									if (queryAnswer != null && !queryAnswer.equals(key))
										results.add(queryAnswer);
								}
							} catch (NoSuchAlgorithmException e) {
								e.printStackTrace();
							}
						}
					}
					String val = findHighestVersionValue(results);
					String res[] = {key, val};
					Log.d("Single QBlock Over",key+":"+val);
					cursor.addRow(res);
			}
		}
		else if (selection.equals("*")) {
			String uniqueQueryAllId = myPort;
			for (String port : sortedPortList)
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QALL;"+ uniqueQueryAllId, port);
			ArrayList<String> temp = new ArrayList<String>();
			multipleQueryResult.put(uniqueQueryAllId, temp);
			while (multipleQueryResult.get(uniqueQueryAllId).size() < 5) {

			}
			Map<String,String> allResults = new HashMap();
			getQueryAllResults(allResults, uniqueQueryAllId);
			for (String key : allResults.keySet()) {
				String []res = {key, allResults.get(key)};
				cursor.addRow(res);
			}
		}
		else {
				List<String> results = new ArrayList<String>();
				synchronized (results) {
					while (results.size() <= 1) {
						try {
							List<String> successors = getSuccessors(genHash(selection));
							for (String successor : successors) {
								String query = "QSS;" + selection;
								String queryAnswer = getAnswer(query, successor);
								if (queryAnswer != null && !queryAnswer.equals(selection))
									results.add(queryAnswer);
							}
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						}
					}
				}
				String val = findHighestVersionValue(results);
				String res[] = {selection, val};
				Log.d("Single QBlock Over",selection+":"+val);
				cursor.addRow(res);
		}
			// TODO Auto-generated method stub
		return cursor;
	}
	public synchronized String findHighestVersionValue(List<String> results) {
		List<String> tempList = new ArrayList<String>();
		for (String val : results) tempList.add(val);
		Collections.sort(tempList, new VersionComparator());
		return tempList.get(0).split(":")[0];
	}
	public synchronized void getQueryAllResults(Map<String,String> allResults, String uniqueQueryAllId) {
		List<String> listOfKeyValVersions = multipleQueryResult.get(uniqueQueryAllId);
		Map<String,Integer> versionKeyMap = new HashMap<String, Integer>();
		for (String keyValVersion : listOfKeyValVersions) {
		    if (keyValVersion.contains(",")) {
		        String[] listOfKVV = keyValVersion.split(",");
		        for (String kVV : listOfKVV) {
		            addToResult(allResults, kVV,versionKeyMap);
                }
            }
            else {
                addToResult(allResults, keyValVersion,versionKeyMap);
            }

		}
	}
	public String getAnswer(String msgToSend, String port) {
		String receivedFromServer = null;
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port)*2);
			socket.setSoTimeout(1500);
			Log.d("Sending to" + port, msgToSend);
			DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
			dataOutputStream.writeUTF(msgToSend);
			DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
			receivedFromServer = dataInputStream.readUTF();
			socket.close();
			Log.d("Sent and Received", receivedFromServer);

		} catch (UnknownHostException e) {
			Log.e( "Unknown Host Exception", e.toString());
			return null;
		} catch (IOException e) {
			Log.e("FAILED:" + port, "ClientTask socket IOException" + e);
			return null;
		}
		return receivedFromServer;
	}
	public void  addToResult(Map<String,String> allResults, String keyValVersion, Map<String,Integer> versionKeyMap) {
        String []parts = keyValVersion.split("-");
        String key = parts[0];
        Log.d("QueryALLKey",key);
        String[] valVersion = parts[1].split(":");
        String val = valVersion[0];
        int version = Integer.parseInt(valVersion[1]);
        if (!key.equals("Invalid")) {
            if (allResults.get(key) != null) {
                int savedVersion = versionKeyMap.get(key);
                if (version > savedVersion) allResults.put(key, val);
            }
            else  {
            	allResults.put(key, val);
            	versionKeyMap.put(key, version);
			}
        }

    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
	private synchronized void insertIntoFile(String key, String value, int version) {
		/*
		 * The following code sets the filename of the file as the key and the content of the file
		 * as the value and writes to the file
		 * Reference :
		 * https://developer.android.com/training/data-storage/files#WriteInternalStorage
		 * PA1 Code
		 * Versioning :
		 * Versioning is implemented in the way that if the key already exists in the internal
		 * storage then update the value of key and the version, otherwise set version as 1
		 */
		String savedValue = retrieveFromFile(key);
		int versionNo = 1;
		if (savedValue != null) {
			String[] vals = savedValue.split(":");
			versionNo = Integer.parseInt(vals[1]) + 1;
		}
		else if (version != -1) {
			versionNo = version;
		}
		FileOutputStream outputStream;
		try {
			outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
			String valToSave = value+":"+versionNo;
			outputStream.write(valToSave.getBytes());
			outputStream.close();
			Log.d("Insert Success", key + ":" + value+":"+versionNo);
		} catch (Exception e) {
			Log.e("FileOutputCreation", "Error");
		}
	}

	private synchronized String retrieveFromFile(String key) {
		/*
		 * The following code reads the content of the file and creates a cursor by setting key as
		 * filename and content of the file as the value
		 * Reference :
		 * https://developer.android.com/reference/android/database/MatrixCursor#addRow(java.lang.Object[])
		 */
		FileInputStream inputStream;
		String value = null;
		byte[] bArray = new byte[128];
		try {
			inputStream = getContext().openFileInput(key);
			int readCount = inputStream.read(bArray);
			if (readCount != -1) value = new String(bArray, 0, readCount);
			inputStream.close();
		} catch (IOException e) {
			Log.e("File read Error", key);
		}
		return value;
	}
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			/*
			 * TODO: Fill in your client code that sends out a message.
			 *
			 * The following code writes string data to the server and reads the acknowledgement from the server
			 * Reference :
			 * 1. https://stackoverflow.com/questions/17440795/send-a-string-instead-of-byte-through-socket-in-java
			 * 2. PA1 Code
			 */
			String msgToSend = msgs[0];
			String port = msgs[1];

			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(port)*2);
				Log.d("Sending to" + port, msgToSend);
				DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
				dataOutputStream.writeUTF(msgToSend);
				DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
				String receivedFromServer = dataInputStream.readUTF();
				socket.close();
				Log.d("Sent to" + port, msgToSend);


			} catch (UnknownHostException e) {
				Log.e( "Unknown Host Exception", e.toString());
			} catch (IOException e) {
				Log.e("FAILED:" + port, "ClientTask socket IOException" + e);
				handleFailure(port, msgToSend);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
		public synchronized void handleFailure(String port, String msgToSend) {
			Log.e("In Failure to", port);
			String[] msgParts = msgToSend.split(";");
			if (msgParts[0].equals("QALL")) {
				ArrayList<String> list = multipleQueryResult.get(myPort);
				list.add("Invalid-Invalid:0");
				multipleQueryResult.put(myPort, list);
			}
		}
	}
	private class HashComparator implements Comparator<String> {
		// The following code is used for lexicographical comparison of hash values
		@Override
		public int compare(String lhs, String rhs) {
			try {
				return genHash(lhs).compareTo(genHash(rhs));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			return 0;
		}

		@Override
		public boolean equals(Object object) {
			return false;
		}
	}
	private class VersionComparator implements Comparator<String> {
		@Override
		public int compare(String lhs, String rhs) {
			try {
				return Integer.parseInt(rhs.split(":")[1]) - Integer.parseInt(lhs.split(":")[1]);
			}
			catch (Exception e) {
				e.printStackTrace();
				Log.d("Error",lhs+":"+rhs);
				return 0;
			}
		}

		@Override
		public boolean equals(Object object) {
			return false;
		}
	}
}
