package com.project.server;

import java.net.ServerSocket;
import java.net.Socket;

import com.project.client.ClientUtil;

public class ChatServer {

	private ServerSocket socket;

	public ChatServer(ServerSocket serverSocket) {
		socket = serverSocket;

	}

	public static void main(String[] args) {
		try {
			ServerSocket serverSocket = new ServerSocket(9999);
			ChatServer server = new ChatServer(serverSocket);
			System.out.println("Server started.");
			server.runChatServer();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void runChatServer() {
		try {
			
			while(!socket.isClosed()) {
				Socket client = socket.accept();
				System.out.println("Client connected.");
				ClientUtil chatClient = new ClientUtil(client);
				Thread t = new Thread(chatClient);
				t.start();
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
