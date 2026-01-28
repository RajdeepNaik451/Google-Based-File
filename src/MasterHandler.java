import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MasterHandler implements Runnable {

    // File → list of chunk IDs
    private static final Map<String, List<String>> fileToChunks = new HashMap<>();

    // Chunk ID → list of chunkserver addresses
    private static final Map<String, List<String>> chunkToServers = new HashMap<>();

    // Registered chunkservers (host:port)
    private static final List<String> chunkServers = new ArrayList<>();

    // Metadata lock
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Chunk ID counter
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

                case REGISTER_CHUNKSERVER:
                    handleRegisterChunkServer(request, out);
                    break;

                default:
                    Message error = new Message();
                    error.type = request.type;
                    error.fileName = request.fileName;
                    error.chunkList = null;
                    error.chunkServerList = null;

                    out.writeObject(error);
                    out.flush();
            }

            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //CREATE FILE
    private void handleCreateFile(Message request, ObjectOutputStream out)
            throws IOException {

        lock.writeLock().lock();
        try {
            Message response = new Message();
            response.type = RequestType.CREATE_FILE;
            response.fileName = request.fileName;

            // File already exists OR no chunkservers available
            if (fileToChunks.containsKey(request.fileName) || chunkServers.isEmpty()) {
                response.chunkList = null;
                response.chunkServerList = null;
            } else {
                String chunkId = generateChunkId();
                allocateChunk(chunkId);

                List<String> chunks = new ArrayList<>();
                chunks.add(chunkId);
                fileToChunks.put(request.fileName, chunks);

                response.chunkList = chunks;
                response.chunkServerList = chunkToServers.get(chunkId);
            }

            out.writeObject(response);
            out.flush();

        } finally {
            lock.writeLock().unlock();
        }
    }

    //GET CHUNKS
    private void handleGetChunks(Message request, ObjectOutputStream out)
            throws IOException {

        lock.readLock().lock();
        try {
            Message response = new Message();
            response.type = RequestType.GET_CHUNKS;
            response.fileName = request.fileName;
            response.chunkList = fileToChunks.get(request.fileName);

            if (response.chunkList != null && !response.chunkList.isEmpty()) {
                response.chunkServerList =
                        chunkToServers.get(response.chunkList.get(0));
            } else {
                response.chunkServerList = null;
            }

            out.writeObject(response);
            out.flush();

        } finally {
            lock.readLock().unlock();
        }
    }

    //REGISTER CHUNKSERVER
    private void handleRegisterChunkServer(Message request, ObjectOutputStream out)
            throws IOException {

        lock.writeLock().lock();
        try {
            if (request.chunkServerList != null && !request.chunkServerList.isEmpty()) {
                String server = request.chunkServerList.get(0);
                if (!chunkServers.contains(server)) {
                    chunkServers.add(server);
                    System.out.println("Registered ChunkServer: " + server);
                }
            }

            Message response = new Message();
            response.type = RequestType.REGISTER_CHUNKSERVER;

            out.writeObject(response);
            out.flush();

        } finally {
            lock.writeLock().unlock();
        }
    }

    //CHUNK ALLOCATION
    private void allocateChunk(String chunkId) {
        List<String> replicas = new ArrayList<>();

        if (chunkServers.size() == 1) {
            replicas.add(chunkServers.get(0));
        } else {
            replicas.add(chunkServers.get(chunkCounter % chunkServers.size()));
            replicas.add(chunkServers.get((chunkCounter + 1) % chunkServers.size()));
        }

        chunkToServers.put(chunkId, replicas);
    }


    //CHUNK ID GENERATOR
    private static synchronized String generateChunkId() {
        return "chunk_" + (++chunkCounter);
    }
}
