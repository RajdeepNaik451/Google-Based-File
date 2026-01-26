import java.io.Serializable;

public class Message implements Serializable {
    public RequestType type;
    public String fileName;
    public byte[] data;
}
