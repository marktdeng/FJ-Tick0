package uk.ac.cam.mtd36.fjava.tick0;

import java.io.IOException;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;

public class FileBlock {
    private FileBuffer currentBuffer;
    private FileBuffer nextBuffer;
    private final String file;
    private final long startLoc;
    private long loc;
    private final long blockSize;
    private int bufSize;

    public FileBlock(String file, int bufSize, long blockSize, long startLoc) throws IOException {
        this.file = file;
        this.startLoc = startLoc;
        this.bufSize = bufSize;
        this.blockSize = blockSize;
        this.loc = 0;
        nextBuffer = new FileBuffer(file, bufSize, startLoc);
        this.loc += bufSize;
        updateBuffer();
    }

    private void updateBuffer() throws IOException {
        if (loc + bufSize >= blockSize) {
            bufSize = toIntExact(blockSize - loc);
            currentBuffer = nextBuffer;
            loc = blockSize;
        } else {
            currentBuffer = nextBuffer;
            nextBuffer = new FileBuffer(file, bufSize, startLoc + loc);
            loc += bufSize;
        }
    }

    public int peek() {
        return currentBuffer.peek();
    }

    public int pop() throws IOException {
        int result = currentBuffer.pop();
        if (!currentBuffer.hasRemaining()){
            updateBuffer();
        }
        return result;
    }

    public boolean hasRemaining(){
        return !(loc >= blockSize && !currentBuffer.hasRemaining());
    }
}
