package com.project.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Scanner;

public class Client {

	private Socket socket;
	BufferedReader rd;
	BufferedWriter wr;
	String username;

	public Client(Socket socket, String username) {

		try {
			this.socket = socket;
			this.username = username;
			rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			wr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			// System.out.println("Username connected: " + username);
		} catch (Exception e) {

		}
	}

	public void sendMessage() {

		try {
			wr.write(username);
			wr.newLine();
			wr.flush();

			Scanner scanner = new Scanner(System.in);

			while (socket.isConnected()) {

				String str = scanner.nextLine();
				wr.write(username + ": " + str);
				wr.newLine();
				wr.flush();
			}

		} catch (Exception e) {

		}
	}

	public void receiveMessages() {

		Runnable r = () -> {
			try {

				while (socket.isConnected()) {

					String message = rd.readLine();
					System.out.println(message);

				}
			} catch (Exception e) {

			}
		};

		new Thread(r).start();

	}

	public static void main(String[] args) {

		try {

			System.out.println("Enter your username: ");
			Scanner scanner = new Scanner(System.in);
			String username = scanner.nextLine();
			Socket socket = new Socket("localhost", 9999);
			Client client = new Client(socket, username);
			client.receiveMessages();
			client.sendMessage();
			

		} catch (Exception e) {

		}

	}

}
