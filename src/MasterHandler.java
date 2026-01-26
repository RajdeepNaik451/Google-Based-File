import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MasterHandler implements Runnable {

    // File → chunk list
    private static final Map<String, List<String>> fileToChunks = new HashMap<>();

    // Lock for metadata safety
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Chunk ID generator
    private static int chunkCounter = 0;

    private final Socket socket;

    public MasterHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            Message request = (Message) in.readObject();
            System.out.println("Received: " + request.type);

            switch (request.type) {

                case CREATE_FILE:
                    handleCreateFile(request, out);
                    break;

                default:
                    out.writeObject("Unsupported request type");
                    out.flush();
            }

            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void handleCreateFile(Message request, ObjectOutputStream out)
            throws IOException {

        lock.writeLock().lock(); // 🔒 WRITE LOCK
        try {
            if (fileToChunks.containsKey(request.fileName)) {
                out.writeObject("File already exists");
            } else {
                // Allocate initial chunk
                String chunkId = generateChunkId();
                List<String> chunks = new ArrayList<>();
                chunks.add(chunkId);

                fileToChunks.put(request.fileName, chunks);

                out.writeObject("File created with chunk: " + chunkId);
            }
            out.flush();
        } finally {
            lock.writeLock().unlock(); // 🔓
        }
    }

    private static synchronized String generateChunkId() {
        return "chunk_" + (++chunkCounter);
    }
}





