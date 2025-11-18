package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;
import java.io.*;
import java.net.*;

public class FileServer {

    private final FileSystemManager fileSystem;
    private final int serverPort;

    public FileServer(int port, String diskFilePath, int totalSize) throws Exception {
        this.serverPort = port;
        this.fileSystem = new FileSystemManager(diskFilePath, totalSize);
    }

    public void startServer() throws Exception {
        ServerSocket serverSocket = new ServerSocket(serverPort);
        System.out.println("Server is listening on port " + serverPort);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(new ClientHandler(clientSocket, fileSystem)).start();
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket socket;
        private final FileSystemManager fileSystem;

        public ClientHandler(Socket socket, FileSystemManager fileSystem) {
            this.socket = socket;
            this.fileSystem = fileSystem;
        }

        @Override
        public void run() {
            try (BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter output = new PrintWriter(socket.getOutputStream(), true)) {

                String clientRequest;
                while ((clientRequest = input.readLine()) != null) {
                    String[] commandParts = clientRequest.trim().split(" ", 3);
                    if (commandParts.length == 0) {
                        output.println("ERROR: Malformed command");
                        continue;
                    }

                    String command = commandParts[0].toUpperCase();
                    try {
                        switch (command) {
                            case "CREATE":
                                if (commandParts.length < 2) {
                                    output.println("ERROR: Malformed command");
                                    break;
                                }
                                fileSystem.createFile(commandParts[1]);
                                output.println("OK");
                                break;
                            case "READ":
                                if (commandParts.length < 2) {
                                    output.println("ERROR: Malformed command");
                                    break;
                                }
                                byte[] data = fileSystem.readFile(commandParts[1]);
                                output.println("OK " + new String(data));
                                break;
                            case "WRITE":
                                if (commandParts.length < 3) {
                                    output.println("ERROR: Malformed command");
                                    break;
                                }
                                fileSystem.writeFile(commandParts[1], commandParts[2].getBytes());
                                output.println("OK");
                                break;
                            case "DELETE":
                                if (commandParts.length < 2) {
                                    output.println("ERROR: Malformed command");
                                    break;
                                }
                                fileSystem.deleteFile(commandParts[1]);
                                output.println("OK");
                                break;
                            case "LIST":
                                String[] files = fileSystem.listFiles();
                                output.println("OK " + String.join(",", files));
                                break;
                            case "QUIT":
                                output.println("OK closing");
                                return;
                            default:
                                output.println("ERROR: Unknown command");
                        }
                    } catch (Exception e) {
                        output.println("ERROR: " + e.getMessage());
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try { socket.close(); } catch (IOException ignored) {}
            }
        }
    }
}
