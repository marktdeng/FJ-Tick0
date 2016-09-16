package uk.ac.cam.mtd36.fjava.tick0;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;

public class FileBlock {
    private FileBuffer currentBuffer;
    private FileBuffer nextBuffer;
    private final String file;
    private final long startLoc;
    private long loc = 0;
    private final long blockSize;
    private int bufSize;
    private int previous = -2147483648;
    private int count = 0;
    private int id;

    public FileBlock(String file, int bufSize, long blockSize, long startLoc, int id) throws IOException {
        this.file = file;
        this.startLoc = startLoc;
        this.blockSize = blockSize;
        this.id = id;
        if (bufSize >= blockSize){
            this.bufSize = toIntExact(blockSize);
        } else {
            this.bufSize = bufSize;
        }
        System.out.println("NEW BLOCK: BUF: " + this.bufSize + " BLOCK: " + this.blockSize + " START: " + startLoc + " ID: " + id);
        nextBuffer = new FileBuffer(this.file, this.bufSize, this.startLoc);
        this.loc += this.bufSize;
        updateBuffer();
    }

    private void updateBuffer() throws IOException {
        if (loc + bufSize > blockSize) {
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
        count++;
        if (!currentBuffer.hasRemaining() && !(loc >= blockSize)){
            updateBuffer();
        }
        if (previous > result){
            System.out.println("PREVIOUS: " + previous + " RESULT: " + result + " LOCATION: " + (count + loc - bufSize) + " ID: " + id);
            throw new NotImplementedException();
        } else {
            previous = result;
        }
        return result;
    }

    public boolean hasRemaining() throws IOException{
        return !(loc >= blockSize && !currentBuffer.hasRemaining());
    }
}
