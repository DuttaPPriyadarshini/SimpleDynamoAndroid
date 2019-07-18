package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;


import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private static String[] successors = new String[2];
	private static String predecessor = null;
	private static String myId = null;

	static final int SERVER_PORT = 10000;
	private static String portStr;
	private String failPort =null;
	ArrayList<String> sList = new ArrayList();
	HashMap sMap = new HashMap();
	private Boolean recovery = false;


	private final Uri mUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket socket = serverSockets[0];
			System.out.println("Recovery S " +recovery);
			while(recovery)
			{  System.out.println("Recovery insert"+recovery);
				try {
					Thread.sleep(25);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			while (true) {
				System.out.println("Server Async Task"+recovery);
				try {

					Socket socketconnection = socket.accept();
					System.out.println("Server Async Task socket accept");
					DataInputStream inputstream = new DataInputStream(socketconnection.getInputStream());
					System.out.println("Node  request recieved ");
					String reqReceived = inputstream.readUTF();

					String[] reqDet = reqReceived.split(":");
					String reqType = reqDet[0];
					if (reqType.equals("Insert")) {

						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(reqDet[1], Context.MODE_PRIVATE);
						outputStream.write(reqDet[2].getBytes());
						outputStream.close();
						Log.v("insert", reqDet[1]+":::"+reqDet[2]);
						DataOutputStream outputStreamAll = new DataOutputStream(socketconnection.getOutputStream());
						outputStreamAll.writeUTF("ACK");

					}
					else if(reqType.equals("QueryRecieve"))
					{   try {
						FileInputStream inputStreamU;
						inputStreamU = getContext().openFileInput(reqDet[1]);
						InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						StringBuffer stringBuffer = new StringBuffer();
						String line;
						while ((line = bufferedReader.readLine()) != null) {
							stringBuffer.append(line);
							Object[] mRow = new Object[2];
							mRow[0] = reqDet[1];
							mRow[1] = stringBuffer;
							DataOutputStream outputStream = new DataOutputStream(socketconnection.getOutputStream());
							outputStream.writeUTF(mRow[0]+"#"+mRow[1]);
						}
					}
					catch (FileNotFoundException e)
					{
						try{
							Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									2 * Integer.parseInt(String.valueOf(sMap.get(successors[0]))));

							DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
							opInsert.writeUTF("QueryRecieve:" + reqDet[1]);
							DataInputStream inInsert = new DataInputStream(succSocket.getInputStream());
							String resultS = inInsert.readUTF();
							succSocket.close();
							String[] sl = resultS.split("#");
							Object[] mRow = new Object[2];
							mRow[0] = reqDet[1];
							mRow[1] = sl[1];
							DataOutputStream outputStream = new DataOutputStream(socketconnection.getOutputStream());
							outputStream.writeUTF(mRow[0]+"#"+mRow[1]);


						}
						catch (IOException e1)
						{
							System.out.println("error");
							try{
								Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										2 * Integer.parseInt(String.valueOf(sMap.get(successors[1]))));

								DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
								opInsert.writeUTF("QueryRecieve:" + reqDet[1]);
								DataInputStream inInsert = new DataInputStream(succSocket.getInputStream());
								String resultS = inInsert.readUTF();
								succSocket.close();
								String[] sl = resultS.split("#");
								Object[] mRow = new Object[2];
								mRow[0] = reqDet[1];
								mRow[1] = sl[1];
								DataOutputStream outputStream = new DataOutputStream(socketconnection.getOutputStream());
								outputStream.writeUTF(mRow[0]+"#"+mRow[1]);


							}
							catch (IOException e2)
							{
								System.out.println("error");
							}
						}
					}


					}
					else if(reqType.equals("Query"))
					{
						String[] fileList = getContext().fileList();
						String resultMerge ="";
						for (int f = 0; f < fileList.length; f++) {
							FileInputStream inputStreamU;


							try {
								System.out.println("File name :  " + fileList[f]);
								inputStreamU = getContext().openFileInput(fileList[f]);
								InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
								BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
								StringBuffer stringBuffer = new StringBuffer();
								String line;
								while ((line = bufferedReader.readLine()) != null) {
									stringBuffer.append(line);
								}

								Object[] mRow = new Object[2];
								mRow[0] = fileList[f];
								mRow[1] = stringBuffer;
								resultMerge=resultMerge+mRow[0]+"#"+mRow[1]+":";
								inputStreamU.close();
							} catch (Exception e) {
								Log.e(TAG, "File read failed");
							}


						}
						DataOutputStream outputStream = new DataOutputStream(socketconnection.getOutputStream());
						outputStream.writeUTF(resultMerge);
					}
					else if(reqType.equals("Delete"))
					{
						String[] fileList=getContext().fileList();
						for(int f=0;f<=fileList.length;f++)
						{
							getContext().deleteFile(fileList[f]);
						}
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput("Restart", Context.MODE_PRIVATE);
						outputStream.write("Restart".getBytes());
						outputStream.close();
					}
					else if (reqType.equals("DeleteInv"))
					{
						getContext().deleteFile(reqDet[1]);
					}
					else if (reqType.equals("DeleteI"))
					{
						getContext().deleteFile(reqDet[1]);
					}
					else if (reqType.equals("RemovePort"))
					{   System.out.println("remove req "+reqDet[1]);
						failPort=reqDet[1];
					}
					else if(reqType.equals("AlivePort")){
						System.out.println("alive req "+reqDet[1]);
						if(failPort!=null && failPort.equals(reqDet[1]))
						{
							failPort=null;
						}}
					else if (reqType.equals("GetMissed"))
					{   //failPort=null;
						String[] fileList = getContext().fileList();
						String resultMerge ="";
						for (int f = 0; f < fileList.length; f++) {
							FileInputStream inputStreamU;


							try {
								System.out.println("File name :  " + fileList[f]);
								inputStreamU = getContext().openFileInput(fileList[f]);
								InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
								BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
								StringBuffer stringBuffer = new StringBuffer();
								String line;
								while ((line = bufferedReader.readLine()) != null) {
									stringBuffer.append(line);
								}

								Object[] mRow = new Object[2];
								mRow[0] = fileList[f];
								String[] valueOnly = (stringBuffer.toString()).split("%");
								mRow[1] = stringBuffer;
								if(valueOnly[1].equals(reqDet[1]))
									resultMerge=resultMerge+mRow[0]+"#"+mRow[1]+":";
								inputStreamU.close();
							} catch (Exception e) {
								Log.e(TAG, "File read failed");
							}


						}
						DataOutputStream outputStreamRecovery = new DataOutputStream(socketconnection.getOutputStream());
						outputStreamRecovery.writeUTF(resultMerge);
					}
					inputstream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}


			}
		}
	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String tempFail = failPort;

		try {

			if(selection.equals("@"))//Delete all local files
			{
				String[] fileList=getContext().fileList();
				for(int f=0;f<=fileList.length;f++)
				{
					getContext().deleteFile(fileList[f]);
				}
				FileOutputStream outputStream;
				outputStream = getContext().openFileOutput("Restart", Context.MODE_PRIVATE);
				outputStream.write("Restart".getBytes());
				outputStream.close();
			}
			else if(selection.equals("*"))//Delete all files stored in Ring DTH
			{   //Delete local first
				String[] fileList=getContext().fileList();
				for(int f=0;f<fileList.length;f++)
				{
					Boolean delete = getContext().deleteFile(fileList[f]);
					System.out.println(delete);
				}

				//Delete other files in Ring DTH
				ArrayList tempList = new ArrayList();
				tempList.addAll(sList);
				tempList.remove(myId);
				for (int i = 0; i < tempList.size(); i++) {
					//Query files in Ring DTH
					try {
						Socket querySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								2 * Integer.parseInt(String.valueOf(sMap.get(tempList.get(i)))));

						if (querySocket.isConnected()) {
							DataOutputStream outputStreamQM = new DataOutputStream(querySocket.getOutputStream());
							outputStreamQM.writeUTF("Delete:" + "*");
						}
					}
					catch (IOException e)
					{
						failPort=String.valueOf(sMap.get(tempList.get(i)));
					}
				}
				FileOutputStream outputStream;
				outputStream = getContext().openFileOutput("Restart", Context.MODE_PRIVATE);
				outputStream.write("Restart".getBytes());
				outputStream.close();

			}
			else {
				String hashKey=genHash(selection);
				//Calculate Ring Position
				ArrayList tempListDelete = new ArrayList();
				tempListDelete.addAll(sList);
				tempListDelete.add(hashKey);
				Collections.sort(tempListDelete);
				int posI = tempListDelete.indexOf(hashKey);

				System.out.println("Position "+posI+" "+tempListDelete.size()+" "+sMap.get(myId)+" "+myId+" "+hashKey);

				if((posI==(tempListDelete.size()-1) && tempListDelete.get(0).equals(myId)))//My location
				{

					for(int i=0;i<2;i++)
					{   try {
						Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								2 * Integer.parseInt(String.valueOf(sMap.get(successors[i]))));
						succSocket.setSoTimeout(1000);
						DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
						opInsert.writeUTF("DeleteI:" + selection);
						//Thread.sleep(3000);
					}
					catch (IOException e)
					{
						failPort= String.valueOf(sMap.get(successors[i]));
					}
					}
					getContext().deleteFile(selection);
				}
				else if (posI!=5 && tempListDelete.get(posI + 1).equals(myId))
				{

					for(int i=0;i<2;i++)
					{
						try {
							Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									2 * Integer.parseInt(String.valueOf(sMap.get(successors[i]))));
							succSocket.setSoTimeout(1000);
							DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
							opInsert.writeUTF("DeleteI:" + selection);
							//Thread.sleep(3000);
						}
						catch (IOException e)
						{
							failPort= String.valueOf(sMap.get(successors[i]));
						}
					}

					getContext().deleteFile(selection);
				}
				else
				{
					String portNDelete;
					if(posI==(tempListDelete.size()-1)){
						portNDelete = (String) sMap.get(tempListDelete.get(1));}
					else{
						portNDelete = (String) sMap.get(tempListDelete.get(posI + 1));}
					try {
						Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								2 * Integer.parseInt(portNDelete));
						DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
						opInsert.writeUTF("DeleteInv:" + selection);
					}
					catch (IOException e)
					{
						failPort= portNDelete;
					}
					if(failPort!=null && failPort.equals(portNDelete))
					{
						String portDNRecieve;
						if (posI == (tempListDelete.size() - 1)) {
							portDNRecieve = (String) sMap.get(tempListDelete.get(1));
						} else if(posI == (tempListDelete.size() - 2))
						{
							portDNRecieve = (String) sMap.get(tempListDelete.get(0));
						}
						else {
							portDNRecieve = (String) sMap.get(tempListDelete.get(posI + 2));
						}
						if(!(portDNRecieve.equals(sMap.get(myId)))) {
							try {
								Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										2 * Integer.parseInt(portNDelete));
								DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
								opInsert.writeUTF("DeleteInv:" + selection);
							} catch (IOException e) {
								System.out.println("Double failure " + portDNRecieve + " " + portNDelete);
							}
						}
						else
							getContext().deleteFile(selection);


					}

				}


			}

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(failPort!=null && !tempFail.equals(failPort))
		{
			int faultyPort = Integer.parseInt(failPort);
			String faultPortHash = (String) sMap.get(faultyPort);
			ArrayList faultRemoval = new ArrayList();
			faultRemoval.addAll(sList);
			faultRemoval.remove(faultPortHash);
			for (int f = 0; f < faultRemoval.size(); f++) {
				try{
					Socket faultSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							2 * Integer.parseInt(String.valueOf(sMap.get(faultRemoval.get(f)))));
					DataOutputStream opFailed = new DataOutputStream(faultSocket.getOutputStream());
					opFailed.writeUTF("RemovePort:" + (faultyPort));
					faultSocket.close();
				}
				catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

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
		//System.out.println("insert");
		String filename = values.get(KEY_FIELD).toString();
		String string = values.get(VALUE_FIELD).toString();
		String cvtoInsert = filename+":"+string;

		new insertAsync().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, cvtoInsert);

		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		Context context = getContext();


		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		System.out.println(portStr);
		recovery = true;

		try {
			sMap.put(genHash("5554"),"5554");
			sMap.put(genHash("5556"),"5556");
			sMap.put(genHash("5558"),"5558");
			sMap.put(genHash("5560"),"5560");
			sMap.put(genHash("5562"),"5562");
			sList.add(genHash("5554"));
			sList.add(genHash("5556"));
			sList.add(genHash("5558"));
			sList.add(genHash("5560"));
			sList.add(genHash("5562"));
			Collections.sort(sList);
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			System.out.println("On create :::::::::::::::::::");
			myId= genHash(portStr);
			int position = sList.indexOf(myId);
			System.out.println("Position "+position+" "+sList.get(position));
			if(!(position==4)) {
				if(position==0)
					predecessor = sList.get(4);
				else
					predecessor = sList.get(position-1);

				successors[0] = sList.get(position + 1);
				if(position==3)
					successors[1] = sList.get(0);
				else
					successors[1] = sList.get(position + 2);
			}
			else //Last node
			{
				predecessor = sList.get(position - 1);
				successors[0] = sList.get(0);
				successors[1] = sList.get(1);
			}
			System.out.println("Location"+sMap.get(myId)+"  "+  sMap.get(successors[0])+"   "+sMap.get(successors[1]));
			Boolean restart=true;
			//Setup unique key for recovery
			try{
				FileInputStream inputStreamU;
				inputStreamU = getContext().openFileInput("Restart");}
			catch (FileNotFoundException e)
			{
				recovery=false;
				restart=false;
				System.out.println("Insert restart");
				FileOutputStream outputStream;
				outputStream = getContext().openFileOutput("Restart", Context.MODE_PRIVATE);
				outputStream.write("Restart".getBytes());
				outputStream.close();
			}
			if(restart) {
				System.out.println("on create --> recovery");
				recovery=true;
				new RecoveryAsync().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, null);
				//recovery=false;
			}




		}
		catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return false;
	}



	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		/*
		 * Reference: https://developer.android.com/reference/android/database/MatrixCursor#MatrixCursor(java.lang.String[])
		 * Reference: https://developer.android.com/reference/android/database/MatrixCursor#addRow(java.lang.Object[])
		 */

		String tempFail = failPort;
		Context context = getContext();
		try {
			System.out.println("Query: " + selection);

			if (selection.equals("@"))//Query all local files
			{   System.out.println("Queryentered: " + selection);
				String[] fileList = context.fileList();
				Cursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
				for (int f = 0; f < fileList.length; f++) {
					FileInputStream inputStreamU;
					try {
						System.out.println("File name :  " + fileList[f]);
						if(!fileList[f].equals("Restart")){
							inputStreamU = getContext().openFileInput(fileList[f]);
							InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
							BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
							StringBuffer stringBuffer = new StringBuffer();
							String line;
							while ((line = bufferedReader.readLine()) != null) {
								stringBuffer.append(line);
							}

							Object[] mRow = new Object[2];
							mRow[0] = fileList[f];
							String[] valueOnly = stringBuffer.toString().split("%");
							mRow[1] = valueOnly[0];
							System.out.println("QueryResult@ : " + mRow[0] + " " + mRow[1]);
							((MatrixCursor) matrixCursor).addRow(mRow);
							inputStreamU.close();}
					} catch (Exception e) {
						Log.e(TAG, "File read failed");
					}
					Log.v("query", selection);

				}
				return matrixCursor;

			} else if (selection.equals("*"))//Query all files stored in Ring DTH
			{
				String globalFile = "";
				FileInputStream inputStreamU;
				String[] fileList = getContext().fileList();
				HashMap keyValue = new HashMap();
				Cursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
				ArrayList tempList = new ArrayList();
				tempList.addAll(sList);
				tempList.remove(myId);
				if(failPort!=null)
					tempList.remove(genHash(failPort));
				for (int i = 0; i < tempList.size(); i++) {
					//Query files in Ring DTH
					try {
						Socket querySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								2 * Integer.parseInt(String.valueOf(sMap.get(tempList.get(i)))));

						if (querySocket.isConnected()) {
							DataOutputStream outputStreamQM = new DataOutputStream(querySocket.getOutputStream());
							outputStreamQM.writeUTF("Query:" + "*");
							DataInputStream inputStreamQM = new DataInputStream(querySocket.getInputStream());
							globalFile = inputStreamQM.readUTF();
							querySocket.close();
							String[] spiltG = globalFile.split(":");
							for (int s = 0; s < spiltG.length; s++) {
								System.out.println(spiltG[s]);
								Object[] mRow = new Object[2];
								String[] splitKV = spiltG[s].split("#");
								mRow[0] = splitKV[0];
								mRow[1] = splitKV[1];
								if (!mRow.equals("Restart"))
									keyValue.put(mRow[0], mRow[1]);
							}
						}
					}
					catch (IOException e)
					{
						failPort= String.valueOf(sMap.get(tempList.get(i)));
					}

				}
				for (int f = 0; f < fileList.length; f++) {

					try {
						System.out.println("File name :  " + fileList[f]);
						inputStreamU = getContext().openFileInput(fileList[f]);
						InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						StringBuffer stringBuffer = new StringBuffer();
						String line;
						while ((line = bufferedReader.readLine()) != null) {
							stringBuffer.append(line);
						}

						Object[] mRow = new Object[2];
						mRow[0] = fileList[f];
						mRow[1] = stringBuffer;
						if(!mRow.equals("Restart"))
							keyValue.put(mRow[0], mRow[1]);

					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				for (Object i : keyValue.keySet()) {
					// search  for value
					//StringBuffer values = (StringBuffer) keyValue.get(i);
					//System.out.println("Key = " + i + ", Value = " + values);
					Object[] mRow = new Object[2];

					mRow[0] = i;
					String[] valueOnly = keyValue.get(i).toString().split("%");
					mRow[1] = valueOnly[0];
					((MatrixCursor) matrixCursor).addRow(mRow);
				}
				return matrixCursor;
			} else {


				Cursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
				//Calculate Ring Position
				String hashKey=genHash(selection);
				ArrayList tempList = new ArrayList();
				tempList.addAll(sList);
				tempList.add(hashKey);
				Collections.sort(tempList);
				int posI = tempList.indexOf(hashKey);
				if((posI==(tempList.size()-1) && tempList.get(0).equals(myId)))//My location
				{
					if(recovery)
					{System.out.println("Query my Recovery");
						try{
							Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									2 * Integer.parseInt(String.valueOf(sMap.get(successors[0]))));

							DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
							opInsert.writeUTF("QueryRecieve:" + selection);
							DataInputStream inInsert = new DataInputStream(succSocket.getInputStream());
							String resultR = inInsert.readUTF();
							Object[] mRow = new Object[2];
							String[] splitKV = resultR.split("#");
							mRow[0] = splitKV[0];
							String[] valueOnly = splitKV[1].split("%");
							mRow[1] = valueOnly[0];

							((MatrixCursor) matrixCursor).addRow(mRow);
							return matrixCursor;

						}
						catch (IOException e1)
						{
							System.out.println("error");
						}
					}
					FileInputStream inputStreamU;
					inputStreamU = getContext().openFileInput(selection);
					InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					StringBuffer stringBuffer = new StringBuffer();
					String line;
					while ((line = bufferedReader.readLine()) != null) {
						stringBuffer.append(line);
					}

					Object[] mRow = new Object[2];
					mRow[0] = selection;
					String[] valueOnly = stringBuffer.toString().split("%");
					mRow[1] = valueOnly[0];
					((MatrixCursor) matrixCursor).addRow(mRow);
					inputStreamU.close();
					return matrixCursor;
				}
				else if (posI!=5 && tempList.get(posI + 1).equals(myId))
				{
					if(recovery)
					{  System.out.println("Query my Recovery");
						try{
							Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									2 * Integer.parseInt(String.valueOf(sMap.get(successors[0]))));

							DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
							opInsert.writeUTF("QueryRecieve:" + selection);
							DataInputStream inInsert = new DataInputStream(succSocket.getInputStream());
							String resultR = inInsert.readUTF();
							Object[] mRow = new Object[2];
							String[] splitKV = resultR.split("#");
							mRow[0] = splitKV[0];
							String[] valueOnly = splitKV[1].split("%");
							mRow[1] = valueOnly[0];

							((MatrixCursor) matrixCursor).addRow(mRow);
							return matrixCursor;

						}
						catch (IOException e1)
						{
							System.out.println("error");
						}
					}
					FileInputStream inputStreamU;
					inputStreamU = getContext().openFileInput(selection);
					InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					StringBuffer stringBuffer = new StringBuffer();
					String line;
					while ((line = bufferedReader.readLine()) != null) {
						stringBuffer.append(line);
					}

					Object[] mRow = new Object[2];
					mRow[0] = selection;
					String[] valueOnly = stringBuffer.toString().split("%");
					mRow[1] = valueOnly[0];
					((MatrixCursor) matrixCursor).addRow(mRow);
					inputStreamU.close();
					return matrixCursor;
				}
				else
				{   System.out.println("Failed Port "+failPort);
					String portRecieve;
					String result = null;
					if(posI==(tempList.size()-1)){
						portRecieve = (String) sMap.get(tempList.get(0));}
					else{
						portRecieve = (String) sMap.get(tempList.get(posI + 1));}
					System.out.println("Port "+portRecieve);
					try {
						Socket recieveSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								2 * Integer.parseInt(portRecieve));
						System.out.println("Port after socket "+portRecieve);
						recieveSocket.setSoTimeout(1000);
						if (recieveSocket.isConnected()) {
							DataOutputStream opQuery = new DataOutputStream(recieveSocket.getOutputStream());
							opQuery.writeUTF("QueryRecieve:" + selection);
							DataInputStream inQuery = new DataInputStream(recieveSocket.getInputStream());
							result = inQuery.readUTF();
							recieveSocket.close();
						}
					}
					catch (IOException e)
					{   System.out.println("Exception failure "+portRecieve);
						failPort = portRecieve;
					}

					if(failPort!=null && failPort.equals(portRecieve) && result==null) {
						String portNRecieve;
						if (posI == (tempList.size() - 1)) {
							portNRecieve = (String) sMap.get(tempList.get(1));
						} else if (posI == (tempList.size() - 2))
						{
							portNRecieve = (String) sMap.get(tempList.get(0));
						}
						else
						{
							portNRecieve = (String) sMap.get(tempList.get(posI + 2));
						}
						System.out.println("Port second "+portNRecieve+" "+posI);
						if(!portNRecieve.equals(sMap.get(myId))){
							try {
								Socket deliverNSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										2 * Integer.parseInt(portNRecieve));
								DataOutputStream opNQuery = new DataOutputStream(deliverNSocket.getOutputStream());
								opNQuery.writeUTF("QueryRecieve:" + selection);
								DataInputStream inQuery = new DataInputStream(deliverNSocket.getInputStream());
								result = inQuery.readUTF();
								deliverNSocket.close();
							}
							catch (IOException e)
							{
								try{
									Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
											2 * Integer.parseInt(String.valueOf(sMap.get(successors[0]))));

									DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
									opInsert.writeUTF("QueryRecieve:" + selection);
									DataInputStream inInsert = new DataInputStream(succSocket.getInputStream());
									result = inInsert.readUTF();

								}
								catch (IOException e1)
								{
									System.out.println("error");
								}
							}
						}
						else
						{   try {
							FileInputStream inputStreamU;
							inputStreamU = getContext().openFileInput(selection);
							InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
							BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
							StringBuffer stringBuffer = new StringBuffer();
							String line;
							while ((line = bufferedReader.readLine()) != null) {
								stringBuffer.append(line);
							}
							result = selection + "#" + stringBuffer;
						}

						catch (FileNotFoundException e)
						{
							try{
								Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										2 * Integer.parseInt(String.valueOf(sMap.get(successors[0]))));

								DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
								opInsert.writeUTF("QueryRecieve:" + selection);
								DataInputStream inInsert = new DataInputStream(succSocket.getInputStream());
								result = inInsert.readUTF();

							}
							catch (IOException e1)
							{
								System.out.println("error");
							}
						}

						}
					}





					Object[] mRow = new Object[2];
					String[] splitKV = result.split("#");
					mRow[0] = splitKV[0];
					String[] valueOnly = splitKV[1].split("%");
					mRow[1] = valueOnly[0];

					((MatrixCursor) matrixCursor).addRow(mRow);
					return matrixCursor;


				}
			}
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Not Found "+selection);
		}
		catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if(failPort!=null && !tempFail.equals(failPort)) {
			int faultyPort = Integer.parseInt(failPort);
			String faultPortHash = (String) sMap.get(faultyPort);
			ArrayList faultRemoval = new ArrayList();
			faultRemoval.addAll(sList);
			faultRemoval.remove(faultPortHash);
			for (int f = 0; f < faultRemoval.size(); f++) {
				try {
					Socket faultSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							2 * Integer.parseInt(String.valueOf(sMap.get(faultRemoval.get(f)))));
					DataOutputStream opFailed = new DataOutputStream(faultSocket.getOutputStream());
					opFailed.writeUTF("RemovePort:" + (faultyPort));
					faultSocket.close();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}

		return null;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	private class insertAsync extends AsyncTask<String, Void,Void> {
		@Override
		protected Void doInBackground(String ...cv) {
			System.out.println("recovery status "+recovery);
			while(recovery)
			{  System.out.println("Recovery insert async");
				try {
					Thread.sleep(25);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			String tempFail = failPort;
			try {


				System.out.println("failed"+failPort+" "+myId+" "+successors[0]+" "+predecessor);
				String[] splitcv = cv[0].split(":");
				String filename= splitcv[0];
				String string=splitcv[1];
				String hashKey = genHash(filename);
				//Calculate Ring Position
				ArrayList tempList = new ArrayList();
				tempList.addAll(sList);
				tempList.add(hashKey);
				Collections.sort(tempList);
				int posI = tempList.indexOf(hashKey);
				System.out.println("Insert Port of Entry "+filename+" "+posI);
				if((posI==(tempList.size()-1) && tempList.get(0).equals(myId)))
				{Log.v("insertmine", filename+" "+posI);
					for(int i=0;i<2;i++) {
						//if (!(failPort!=null && (String.valueOf(sMap.get(successors[i]))).equals(failPort))) {
						try {
							Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									2 * Integer.parseInt(String.valueOf(sMap.get(successors[i]))));

							System.out.println("Porttt" + succSocket.getPort());
							succSocket.setSoTimeout(1000);
							if (succSocket.isConnected()) {
								DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
								opInsert.writeUTF("Insert:" + cv[0] + "%" + portStr);
								//DataInputStream in1Insert = new DataInputStream(succSocket.getInputStream());
								//String result = in1Insert.readUTF();
								succSocket.close();
							}
						}
						catch (IOException e)
						{
							failPort= String.valueOf(sMap.get(successors[i]));
						}

						//}
					}
					FileOutputStream outputStream;
					outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
					outputStream.write((string+"%"+portStr).getBytes());
					outputStream.close();
					Log.v("insert", filename+":::"+string);

				}
				else if (posI!=5 && tempList.get(posI + 1).equals(myId))
				{Log.v("insertmine", filename+" "+posI);
					for(int i=1;i>=0;i--) {

						//if (!(failPort!=null && (String.valueOf(sMap.get(successors[i]))).equals(failPort))) {
						try {
							Socket succSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									2 * Integer.parseInt(String.valueOf(sMap.get(successors[i]))));
							succSocket.setSoTimeout(1000);
							if (succSocket.isConnected()) {
								DataOutputStream opInsert = new DataOutputStream(succSocket.getOutputStream());
								opInsert.writeUTF("Insert:" + cv[0] + "%" + portStr);
								//DataInputStream in2Insert = new DataInputStream(succSocket.getInputStream());
								//String result = in2Insert.readUTF();
								succSocket.close();
							}
						}
						catch(IOException e) //Failure mechanism
						{
							failPort = String.valueOf(sMap.get(successors[i]));
						}

						//}
					}
					FileOutputStream outputStream;
					outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
					outputStream.write((string+"%"+portStr).getBytes());
					outputStream.close();
					Log.v("insert", filename+":::"+string);
				}
				else //Handle passing inserts and failure mechanism insert
				{
					String[] portDeliver= new String[3];
					Log.v("insertdiff", filename+" "+posI);
					if(posI==(tempList.size()-1)){
						portDeliver[0] = (String) sMap.get(tempList.get(0));
						portDeliver[1] = (String) sMap.get(tempList.get(1));
						portDeliver[2] = (String) sMap.get(tempList.get(2));
					}
					else if(posI==(tempList.size()-2)) {
						portDeliver[0] = (String) sMap.get(tempList.get(tempList.size() - 1));
						portDeliver[1] = (String) sMap.get(tempList.get(0));
						portDeliver[2] = (String) sMap.get(tempList.get(1));
					}
					else if(posI==(tempList.size()-3)) {
						portDeliver[0] = (String) sMap.get(tempList.get(tempList.size() - 2));
						portDeliver[1] = (String) sMap.get(tempList.get(tempList.size() - 1));
						portDeliver[2] = (String) sMap.get(tempList.get(0));
					}
					else{
						portDeliver[0] = (String) sMap.get(tempList.get(posI+1));
						portDeliver[1] = (String) sMap.get(tempList.get(posI+2));
						portDeliver[2] = (String) sMap.get(tempList.get(posI+3));
					}

					for(int i = 2; i>=0;i--) {
						if(!portDeliver[i].equals(sMap.get(myId)))
						{
							//if(!(failPort!=null && failPort.equals(portDeliver[i])))
							//{
							try {

								Socket deliverSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										2 * Integer.parseInt(portDeliver[i]));
								deliverSocket.setSoTimeout(1000);
								if (deliverSocket.isConnected()) {
									DataOutputStream opInsert = new DataOutputStream(deliverSocket.getOutputStream());
									opInsert.writeUTF("Insert:" + cv[0]+ "%" + portDeliver[0]);
									//DataInputStream in3Insert = new DataInputStream(deliverSocket.getInputStream());
									//String result = in3Insert.readUTF();
									deliverSocket.close();
									//Log.v("Resultfromins", result);
								}
							} catch (IOException e) {
								failPort = portDeliver[i];
							}
							//}
						}
						else
						{
							FileOutputStream outputStream;
							outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
							outputStream.write((string+"%"+portDeliver[0]).getBytes());
							outputStream.close();
						}

					}
				}

			}

			catch (NoSuchAlgorithmException e)
			{
				e.printStackTrace();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch (Exception e)
			{
				System.out.println("Port +++ " +portStr+" "+e);
				Log.e(TAG, "File write failed");
			}

			if(failPort!=null && !tempFail.equals(failPort))
			{
				int faultyPort = Integer.parseInt(failPort);
				String faultPortHash = (String) sMap.get(faultyPort);
				ArrayList faultRemoval = new ArrayList();
				faultRemoval.addAll(sList);
				faultRemoval.remove(faultPortHash);
				for (int f = 0; f < faultRemoval.size(); f++) {
					try{
						Socket faultSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								2 * Integer.parseInt(String.valueOf(sMap.get(faultRemoval.get(f)))));
						DataOutputStream opFailed = new DataOutputStream(faultSocket.getOutputStream());
						opFailed.writeUTF("RemovePort:" + (faultyPort));
						faultSocket.close();
					}
					catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
			}

			return null;
		}
	}

	private class RecoveryAsync extends AsyncTask<String, Void,Void> {

		@Override
		protected Void doInBackground(String... strings) {
			try {
				System.out.println("Recovery");
				recovery = true;




				//Get latest
				System.out.println("connection "+successors[0]);
				Socket successorRecSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						2 * Integer.parseInt(String.valueOf(sMap.get(successors[0]))));

				DataOutputStream opNInsert = new DataOutputStream(successorRecSocket.getOutputStream());
				opNInsert.writeUTF("GetMissed:"+portStr);
				DataInputStream inNInsert = new DataInputStream(successorRecSocket.getInputStream());
				String resultN = inNInsert.readUTF();
				successorRecSocket.close();
				System.out.println("Returned "+resultN);
				try {
					String[] spiltG = resultN.split(":");

					for (int s = 0; s < spiltG.length; s++) {

						String[] splitKV = spiltG[s].split("#");
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(splitKV[0], Context.MODE_PRIVATE);
						outputStream.write(splitKV[1].getBytes());
						outputStream.close();
					}
				}
				catch (FileNotFoundException e) {
					System.out.println("No Files");
					//Delete all store new
					String[] fileList = getContext().fileList();
					for (int f = 0; f < fileList.length; f++) {
						FileInputStream inputStreamU;
						try {
							System.out.println("File name :  " + fileList[f]);
							inputStreamU = getContext().openFileInput(fileList[f]);
							InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
							BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
							StringBuffer stringBuffer = new StringBuffer();
							String line;
							while ((line = bufferedReader.readLine()) != null) {
								stringBuffer.append(line);
							}

							Object[] mRow = new Object[2];
							mRow[0] = fileList[f];
							String[] valueOnly = (stringBuffer.toString()).split("%");
							mRow[1] = stringBuffer;
							if (!mRow[0].equals("Restart") && valueOnly[1].equals(portStr))
								getContext().deleteFile((String) mRow[0]);
							inputStreamU.close();
						} catch (FileNotFoundException e1) {
							e1.printStackTrace();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}


				int posinRing = sList.indexOf(myId);

				//Predecessors2

				String cord2 = null;
				if(posinRing==1)
					cord2 = (String) sMap.get(sList.get(0));
				else if (posinRing==0)
					cord2 = (String) sMap.get(sList.get(sList.size()-1));
				else
					cord2 = (String) sMap.get(sList.get(posinRing-1));
				Socket p2RecSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						2 * Integer.parseInt(cord2));

				DataOutputStream opNInsert2 = new DataOutputStream(p2RecSocket.getOutputStream());
				opNInsert2.writeUTF("GetMissed:"+cord2);
				DataInputStream inNInsert2 = new DataInputStream(p2RecSocket.getInputStream());
				String resultN2 = inNInsert2.readUTF();
				p2RecSocket.close();
				System.out.println("Returned "+resultN2);
				try{
					String[] spiltG2 = resultN2.split(":");
					for (int s = 0; s < spiltG2.length; s++) {

						String[] splitKV = spiltG2[s].split("#");
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(splitKV[0], Context.MODE_PRIVATE);
						outputStream.write(splitKV[1].getBytes());
						outputStream.close();
					}}
				catch (FileNotFoundException e)
				{
					System.out.println("No Files");
					//Delete all store new
					String[] fileList = getContext().fileList();
					for (int f = 0; f < fileList.length; f++) {
						FileInputStream inputStreamU;
						try {
							System.out.println("File name :  " + fileList[f]);
							inputStreamU = getContext().openFileInput(fileList[f]);
							InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
							BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
							StringBuffer stringBuffer = new StringBuffer();
							String line;
							while ((line = bufferedReader.readLine()) != null) {
								stringBuffer.append(line);
							}

							Object[] mRow = new Object[2];
							mRow[0] = fileList[f];
							String[] valueOnly = (stringBuffer.toString()).split("%");
							mRow[1] = stringBuffer;
							if (!mRow[0].equals("Restart") && valueOnly[1].equals(cord2))
								getContext().deleteFile((String) mRow[0]);
							inputStreamU.close();
						} catch (FileNotFoundException e1) {
							e1.printStackTrace();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}

				}
				//Predecessors1

				String cord = null;
				if(posinRing==1)
					cord = (String) sMap.get(sList.get(sList.size()-1));
				else if (posinRing==0)
					cord = (String) sMap.get(sList.get(sList.size()-2));
				else
					cord = (String) sMap.get(sList.get(posinRing-2));
				Socket p1RecSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						2 * Integer.parseInt(cord));

				DataOutputStream opNInsert1 = new DataOutputStream(p1RecSocket.getOutputStream());
				opNInsert1.writeUTF("GetMissed:"+cord);
				DataInputStream inNInsert1 = new DataInputStream(p1RecSocket.getInputStream());
				String resultN1 = inNInsert1.readUTF();
				p1RecSocket.close();
				System.out.println("Returned "+resultN1);
				try {
					String[] spiltG1 = resultN1.split(":");
					for (int s = 0; s < spiltG1.length; s++) {

						String[] splitKV = spiltG1[s].split("#");
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(splitKV[0], Context.MODE_PRIVATE);
						outputStream.write(splitKV[1].getBytes());
						outputStream.close();
					}
				}
				catch (FileNotFoundException e)
				{
					System.out.println("No Files");

					//Delete all store new
					String[] fileList = getContext().fileList();
					for (int f = 0; f < fileList.length; f++) {
						FileInputStream inputStreamU;
						try {
							System.out.println("File name :  " + fileList[f]);
							inputStreamU = getContext().openFileInput(fileList[f]);
							InputStreamReader inputStreamReader = new InputStreamReader(inputStreamU);
							BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
							StringBuffer stringBuffer = new StringBuffer();
							String line;
							while ((line = bufferedReader.readLine()) != null) {
								stringBuffer.append(line);
							}

							Object[] mRow = new Object[2];
							mRow[0] = fileList[f];
							String[] valueOnly = (stringBuffer.toString()).split("%");
							mRow[1] = stringBuffer;
							if (!mRow[0].equals("Restart") && valueOnly[1].equals(cord))
								getContext().deleteFile((String) mRow[0]);
							inputStreamU.close();
						} catch (FileNotFoundException e1) {
							e1.printStackTrace();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}






			} catch (IOException e) {
				e.printStackTrace();
			}
			recovery=false;
			return null;
		}
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



}