package com.project.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ClientUtil implements Runnable {

	private Socket socket;
	BufferedReader rd;
	BufferedWriter wr;
	String username;
	public static List<ClientUtil> clientList = new ArrayList<>();

	public ClientUtil(Socket client) {
		try {
			socket = client;
			rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			wr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			String username = rd.readLine();
			this.username = username;
			System.out.println(username + " joined.");
			clientList.add(this);
			// Show other clients that user has connected
			sendMessageToOtherClients(username + " joined.");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sendMessageToOtherClients(String message) {
		try {
			// System.out.println("sendMessageToOtherClients: " + message);
			for (ClientUtil c : clientList) {
				if (c != this) {
					c.wr.write(message);
					c.wr.newLine();
					c.wr.flush();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		try {

			while (socket.isConnected()) {
				String message = rd.readLine();
				System.out.println("Message: " + message);

				// Show message to other clients
				sendMessageToOtherClients(message);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
