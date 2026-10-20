import java.nio.ByteBuffer;

public interface Header {
    public static Header parse(ByteBuffer buffer) {
        byte header = buffer.get();
        if ((header & 0b10000000) == 0b10000000) {
            // Is heartbeat or token passing
            int sourceId = (header & 0b01110000) >>> 4;
            int nonce = header & 0b00001111;
            if (nonce == 0xF) {
                // Is token passing
                header = buffer.get();
                int nextHop = header & 0b11100000 >>> 5;
                int destId = header & 0b00011100 >>> 2;
                return new TokenHeader(
                    sourceId,
                    nonce,
                    nextHop,
                    destId
                );
            } else {
                // Is heartbeat
                header = buffer.get();
                boolean isCollision = (header & 0b10000000) == 0b10000000;
                int bitmap = header & 0b01111111;
                return new HeartbeatHeader(
                    sourceId,
                    nonce,
                    isCollision,
                    bitmap
                );
            }
        }
        boolean isAck = ((header & 0b00000001) == 0b00000001) ? true : false;
        int sourceId = (header & 0b01110000) >>> 4;
        int destId = (header & 0b00001110) >>> 1;
        header = buffer.get();
        boolean isLastFragment = ((header & 0b10000000) >>> 7) == 1;
        int nextHop = (header & 0b01110000) >>> 4;
        int sequenceNumber = header & 0b00001111;
        return new PayloadHeader(
            isAck,
            sourceId,
            destId,
            isLastFragment,
            nextHop,
            sequenceNumber
        );
    }
    Header copy();
    int assemble();
    boolean isAck();
    int getSourceId();
    void setSourceId(int sourceId);

}
