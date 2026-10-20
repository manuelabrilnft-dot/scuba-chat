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
    public Header copy() {
        return new HeartbeatHeader(sourceId, nonce, isCollision, bitmap);
    }

    @Override
    public int assemble() {
        int output = 0;
        output = (1 << 15) | output;
        output = (sourceId << 12) | output;
        output = (nonce << 8) | output;
        output = ((isCollision ? 1 : 0) << 7) | output;
        output = bitmap | output;
        System.out.println(Integer.toBinaryString(output));
        return output;
    }

    @Override
    public boolean isAck() {
        return false;
    }

    @Override
    public int getSourceId() {
        return sourceId;
    }

    @Override
    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public int getNonce() {
        return nonce;
    }

    public void setNonce(int nonce) {
        this.nonce = nonce;
    }

    public boolean isCollision() {
        return isCollision;
    }

    public void isCollision(boolean isCollision) {
        this.isCollision = isCollision;
    }

    public int getBitmap() {
        return bitmap;
    }

    public void setBitmap(int bitmap) {
        this.bitmap = bitmap;
    }
}
