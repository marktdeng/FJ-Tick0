package uk.ac.cam.mtd36.fjava.tick0;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;

public class FileBuffer {
    private AsynchronousFileChannel afc;
    private ByteBuffer buf;
    private IntBuffer ibuf;
    private Future future;
    private boolean isReady = false;

    public FileBuffer(String file, int bufSize, long startLoc) throws IOException {
        if (bufSize % 4 != 0){
            System.exit(1);
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
            buf.flip();
            ibuf = buf.asIntBuffer();
            buf = null;
            try {
                afc.close();
            } catch (IOException e){
                e.printStackTrace();
                System.exit(1);
            }
            isReady = true;
        }
    }

    public int peek(){
        waitReady();
        ibuf.mark();
        int result = ibuf.get();
        ibuf.reset();
        return result;
    }

    public int pop(){
        waitReady();
        return ibuf.get();
    }

    public int[] getAll(){
        waitReady();
        return ibuf.array();
    }

    public boolean hasRemaining() {
        if (!isReady){
            return true;
        } else {
            System.out.println("REMAINING BUFFER: " + ibuf.remaining());
            return ibuf.hasRemaining();
        }
    }
}
