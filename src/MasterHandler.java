import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MasterHandler implements Runnable {

    // METADATA
    private static final Map<String, List<String>> fileToChunks = new HashMap<>();
    private static final Map<String, List<String>> chunkToServers = new HashMap<>();
    private static final List<String> chunkServers = new ArrayList<>();
    private static final Map<String, Long> lastHeartbeat = new HashMap<>();

    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private static final int REPLICATION_FACTOR = 3;
    private static int chunkCounter = 0;

    private final Socket socket;

    private static final int CHUNK_SIZE = 64 * 1024; // 64 KB

    private static final Map<String, Long> fileSizes = new HashMap<>();
    private static final Map<String, String> fileTypes = new HashMap<>();
    private static final Set<String> directories = new HashSet<>();

    // FAILURE MONITOR
    static {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    long now = System.currentTimeMillis();

                    lock.writeLock().lock();
                    Iterator<String> it = lastHeartbeat.keySet().iterator();

                    while (it.hasNext()) {
                        String server = it.next();
                        long last = lastHeartbeat.get(server);

                        if (now - last > 10000) {
                            System.out.println("ChunkServer FAILED: " + server);
                            it.remove();
                            chunkServers.remove(server);

                            for (List<String> replicas : chunkToServers.values()) {
                                replicas.remove(server);
                            }
                        }
                    }
                    lock.writeLock().unlock();

                } catch (Exception ignored) {}
            }
        }).start();
    }

    public MasterHandler(Socket socket) {
        this.socket = socket;
    }

    // MAIN HANDLER
    @Override
    public void run() {
        try {
            // 🔥 INPUT FIRST (prevents stream header deadlock)
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Message request = (Message) in.readObject();

            // ========= ONE-WAY CONTROL MESSAGES =========
            if (request.type == RequestType.HEARTBEAT) {
                handleHeartbeat(request);
                socket.close();
                return;
            }

            if (request.type == RequestType.REGISTER_CHUNKSERVER) {
                handleRegisterChunkServer(request);
                socket.close();
                return;
            }

            // ========= REQUEST–RESPONSE RPC =========
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            switch (request.type) {

                case CREATE_FILE -> handleCreateFile(request, out);
                case GET_CHUNKS -> handleGetChunks(request, out);
                case APPEND -> handleAppend(request, out);
                case CREATE_DIRECTORY -> {
                    handleCreateDirectory(request);
                    socket.close();
                    return;
                }


                default -> {
                    Message error = new Message();
                    error.type = request.type;
                    out.writeObject(error);
                    out.flush();
                }
            }

            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // CREATE FILE
    private void handleCreateFile(Message req, ObjectOutputStream out)
            throws IOException {

        lock.writeLock().lock();
        try {
            Message res = new Message();
            res.type = RequestType.CREATE_FILE;
            res.fileName = req.fileName;

            if (fileToChunks.containsKey(req.fileName)) {
                res.chunkList = null;
            } else {

                int chunkCount =
                        (int) Math.ceil((double) req.fileSize / CHUNK_SIZE);

                List<String> chunks = new ArrayList<>();

                for (int i = 0; i < chunkCount; i++) {
                    String chunkId = generateChunkId();
                    allocateChunk(chunkId);
                    chunks.add(chunkId);
                }

                fileToChunks.put(req.fileName, chunks);
                fileSizes.put(req.fileName, req.fileSize);
                fileTypes.put(req.fileName, req.fileType);

                res.chunkList = chunks;
            }

            out.writeObject(res);
            out.flush();

        } finally {
            lock.writeLock().unlock();
        }
    }
    // CREATE DIRECTORY
    private void handleCreateDirectory(Message req) {

        lock.writeLock().lock();
        try {
            directories.add(req.fileName);
            System.out.println("Directory created: " + req.fileName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // GET CHUNKS
    private void handleGetChunks(Message req, ObjectOutputStream out) throws IOException {
        lock.readLock().lock();
        try {
            Message res = new Message();
            res.type = RequestType.GET_CHUNKS;
            res.fileName = req.fileName;
            res.chunkList = fileToChunks.get(req.fileName);

            if (res.chunkList != null && !res.chunkList.isEmpty()) {
                res.chunkServerList = chunkToServers.get(res.chunkList.get(0));
            }

            out.writeObject(res);
            out.flush();
        } finally {
            lock.readLock().unlock();
        }
    }

    // REGISTER CHUNKSERVER (ONE-WAY)
    private void handleRegisterChunkServer(Message req) {
        lock.writeLock().lock();
        try {
            String server = req.chunkServerList.get(0);

            if (!chunkServers.contains(server)) {
                chunkServers.add(server);
                lastHeartbeat.put(server, System.currentTimeMillis());
                System.out.println("Registered ChunkServer: " + server);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // HEARTBEAT (ONE-WAY)
    private void handleHeartbeat(Message req) {
        lock.writeLock().lock();
        try {
            lastHeartbeat.put(req.chunkServerList.get(0), System.currentTimeMillis());
        } finally {
            lock.writeLock().unlock();
        }
    }

    // APPEND
    private void handleAppend(Message req, ObjectOutputStream out) throws IOException {
        lock.writeLock().lock();
        try {
            List<String> chunks = fileToChunks.get(req.fileName);

            Message res = new Message();
            res.type = RequestType.APPEND;

            if (chunks != null && !chunks.isEmpty()) {
                String lastChunk = chunks.get(chunks.size() - 1);
                res.chunkList = List.of(lastChunk);
                res.chunkServerList = chunkToServers.get(lastChunk);
            }

            out.writeObject(res);
            out.flush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // CHUNK ALLOCATION
    private void allocateChunk(String chunkId) {
        List<String> replicas = new ArrayList<>();
        int n = Math.min(REPLICATION_FACTOR, chunkServers.size());

        for (int i = 0; i < n; i++) {
            replicas.add(chunkServers.get((chunkCounter + i) % chunkServers.size()));
        }

        chunkToServers.put(chunkId, replicas);
    }

    // CHUNK ID GENERATOR
    private static synchronized String generateChunkId() {
        return "chunk_" + (++chunkCounter);
    }
}
