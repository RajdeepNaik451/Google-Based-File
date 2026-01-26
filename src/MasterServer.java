import java.net.ServerSocket;
import java.net.Socket;

public class MasterServer {

    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(8080);
        System.out.println("Master started on port 8080");

        while (true) {
            Socket client = serverSocket.accept();
            new Thread(new MasterHandler(client)).start();
        }
    }
}
