package uk.ac.cam.mtd36.fjava.tick0;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;

public class FileWriter {
    private AsynchronousFileChannel afc;
    private Future result = null;
    private ByteBuffer writeBuf;
    private ByteBuffer storeBuf;
    private int bufSize;
    private long loc = 0;
    private final long fileSize;

    public FileWriter(String file, int bufSize, long fileSize) throws IOException {
        this.bufSize = bufSize;
        this.fileSize = fileSize;
        this.afc = AsynchronousFileChannel.open(Paths.get(file));
        storeBuf = ByteBuffer.allocate(bufSize);
        writeBuf = ByteBuffer.allocate(bufSize);
    }

    public void add(int i){
        System.out.println(i);
        storeBuf.asIntBuffer().put(i);
        if (!storeBuf.hasRemaining()){
            write();
        }
    }

    public void addArray(int[] i){
        for (int j: i) {
            this.add(j);
        }
    }

    private void write(){
        if (loc != 0){
            while (!result.isDone()){
                //Waiting
            }
        }
        if (loc >= fileSize){
            return;
        }
        storeBuf.flip();
        writeBuf.clear();

        ByteBuffer tmp = storeBuf;
        storeBuf = writeBuf;
        writeBuf = tmp;

        result = afc.write(writeBuf, loc);

        if (loc + bufSize >= fileSize){
            bufSize = toIntExact(fileSize - loc);
            storeBuf = ByteBuffer.allocate(bufSize);
        }
        loc += bufSize;
    }
}
