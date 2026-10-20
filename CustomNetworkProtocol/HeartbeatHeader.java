/**
 * Heartbeat header (MSB = 1, nonce not in {0xE, 0xF}).
 *
 * Byte 0: [1 | src(3) | nonce(4)]
 * Byte 1: [isCollision(1) | bitmap(7)]
 */
public class HeartbeatHeader implements Header {
    private int sourceId;
    private int nonce;
    private boolean isCollision;
    private int bitmap;

    public HeartbeatHeader(int sourceId, int nonce, boolean isCollision, int bitmap) {
        this.sourceId = sourceId;
        this.nonce = nonce;
        this.isCollision = isCollision;
        this.bitmap = bitmap;
    }

    @Override
    public int assemble() {
        int out = 0;
        out |= 1 << 15;
        out |= (sourceId & 0x07) << 12;
        out |= (nonce    & 0x0F) << 8;
        out |= (isCollision ? 1 : 0) << 7;
        out |= (bitmap & 0x7F);
        return out & 0xFFFF;
    }

    @Override
    public Header copy() {
        return new HeartbeatHeader(sourceId, nonce, isCollision, bitmap);
    }

    @Override public boolean isAck() { return false; }

    @Override public int getSourceId() { return sourceId; }
    @Override public void setSourceId(int sourceId) { this.sourceId = sourceId; }

    public int getNonce() { return nonce; }
    public void setNonce(int nonce) { this.nonce = nonce; }

    public boolean isCollision() { return isCollision; }
    public void setCollision(boolean c) { this.isCollision = c; }

    public int getBitmap() { return bitmap; }
    public void setBitmap(int bitmap) { this.bitmap = bitmap; }
}
