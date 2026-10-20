import java.nio.ByteBuffer;

/**
 * Base interface for all protocol headers (2 bytes each).
 *
 * Wire layout (MSB of first byte = packet type discriminator):
 *   MSB = 0 : PayloadHeader
 *   MSB = 1 : Control (Heartbeat or Token)
 *     nonce == 0xF or 0xE -> TokenHeader
 *     otherwise           -> HeartbeatHeader
 */
public interface Header {

    int HEADER_SIZE = 2;

    int assemble();
    Header copy();
    boolean isAck();
    int getSourceId();
    void setSourceId(int sourceId);

    /**
     * Parse a header from the first two bytes of {@code buffer}. The buffer's
     * position is advanced by {@link #HEADER_SIZE} bytes.
     */
    static Header parse(ByteBuffer buffer) {
        int b0 = buffer.get() & 0xFF;
        int b1 = buffer.get() & 0xFF;
        int raw = (b0 << 8) | b1;

        boolean isControl = (b0 & 0x80) != 0;
        int sourceId = (b0 & 0x70) >>> 4;

        if (isControl) {
            int nonce = b0 & 0x0F;
            if (nonce == 0xF || nonce == 0xE) {
                int nextHop = (b1 & 0xE0) >>> 5;
                int destId  = (b1 & 0x1C) >>> 2;
                return new TokenHeader(sourceId, nonce, nextHop, destId);
            } else {
                boolean collision = (b1 & 0x80) != 0;
                int bitmap = b1 & 0x7F;
                return new HeartbeatHeader(sourceId, nonce, collision, bitmap);
            }
        } else {
            int destId        = (b0 & 0x0E) >>> 1;
            boolean isAck     = (b0 & 0x01) != 0;
            boolean isLast    = (b1 & 0x80) != 0;
            int nextHop       = (b1 & 0x70) >>> 4;
            int sequenceNo    = b1 & 0x0F;
            return new PayloadHeader(isAck, sourceId, destId, isLast, nextHop, sequenceNo);
        }
    }
}
