package client;

import java.util.HashMap;
import java.util.Map;

public class MessageTypeMap {
    private static final Map<MessageType, Byte> MessageMap = new HashMap<>();
    public MessageTypeMap() {
        MessageMap.put(MessageType.FREE, (byte) 0x00000000);
        MessageMap.put(MessageType.BUSY, (byte) 0x00010000);
        MessageMap.put(MessageType.DATA, (byte) 0x00100000);
        MessageMap.put(MessageType.SENDING, (byte) 0x00110000);
        MessageMap.put(MessageType.DONE_SENDING, (byte) 0x01000000);
        MessageMap.put(MessageType.DATA_SHORT, (byte) 0x01010000);
        MessageMap.put(MessageType.END, (byte) 0x01100000);
        MessageMap.put(MessageType.HELLO, (byte) 0x01110000);
        MessageMap.put(MessageType.TOKEN_ACCEPTED, (byte) 0x10000000);
        MessageMap.put(MessageType.TOKEN_REJECTED, (byte) 0x10010000);
    }
    public static byte get(MessageType type) {
        return MessageMap.get(type);
    }

}
