import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MasterHandler implements Runnable {

    // File → chunk list
    private static final Map<String, List<String>> fileToChunks = new HashMap<>();

    // chunkId -> list of chunkserver addresses
    private static final Map<String, List<String>> chunkToServers = new HashMap<>();

    private static final List<String> chunkServers = List.of("localhost:6001", "localhost:6002", "localhost:6003");

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

                case GET_CHUNKS:
                    handleGetChunks(request, out);
                    break;

                default:
                    Message error = new Message();
                    error.type = request.type;
                    error.fileName = request.fileName;
                    error.chunkList = null;

                    out.writeObject(error);
                    out.flush();
            }

            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void handleCreateFile(Message request, ObjectOutputStream out)
            throws IOException {

        lock.writeLock().lock();
        try {
            Message response = new Message();
            response.type = RequestType.CREATE_FILE;
            response.fileName = request.fileName;

            if (fileToChunks.containsKey(request.fileName)) {
                response.chunkList = null; // indicates already exists
            } else {
                String chunkId = generateChunkId();
                List<String> chunks = new ArrayList<>();
                chunks.add(chunkId);
                fileToChunks.put(request.fileName, chunks);

                response.chunkList = chunks;
            }

            out.writeObject(response);
            out.flush();

        } finally {
            lock.writeLock().unlock();
        }
    }

    private void allocateChunk(String chunkId) {
        List<String> replicas = new ArrayList<>();
        replicas.add(chunkServers.get(chunkCounter % chunkServers.size()));
        replicas.add(chunkServers.get((chunkCounter + 1) % chunkServers.size()));

        chunkToServers.put(chunkId, replicas);
    }

    private static synchronized String generateChunkId() {
        return "chunk_" + (++chunkCounter);
    }

    private void handleGetChunks(Message request, ObjectOutputStream out)
            throws IOException {

        lock.readLock().lock();
        try {
            Message response = new Message();
            response.type = RequestType.GET_CHUNKS;
            response.fileName = request.fileName;
            response.chunkList = fileToChunks.get(request.fileName);

            out.writeObject(response);
            out.flush();

        } finally {
            lock.readLock().unlock();
        }
    }
}





