package uk.ac.cam.mtd36.fjava.tick0;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;

public class FileBuffer {
    private AsynchronousFileChannel afc;
    private ByteBuffer buf;
    private Future future;
    private boolean isReady = false;

    public FileBuffer(String file, int bufSize, long startLoc) throws IOException {
        if (bufSize % 4 != 0){
            throw new
        }
        this.afc = AsynchronousFileChannel.open(Paths.get(file), READ);
        this.buf = ByteBuffer.allocate(bufSize);
        this.future = afc.read(buf, startLoc);
    }

    private void waitReady(){
        if (!isReady) {
            while (!future.isDone()) {
                //Waiting
            }
            isReady = true;
        }
    }

    public int peek(){
        waitReady();
        buf.asIntBuffer().mark();
        int result = buf.asIntBuffer().get();
        buf.asIntBuffer().reset();
        return result;
    }

    public int pop(){
        waitReady();
        return buf.asIntBuffer().get();
    }

    public boolean hasRemaining() {
        return buf.hasRemaining();
    }

}
