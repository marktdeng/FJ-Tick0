/*package uk.ac.cam.mtd36.fjava.tick0;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.io.BufferedWriter;
import java.nio.file.Paths;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;
import static java.nio.file.StandardOpenOption.WRITE;

public class FileWriter {
    private BufferedWriter writer;
    private Future result = null;
    private ByteBuffer writeBuf;
    private ByteBuffer storeBuf;
    private IntBuffer istoreBuf;
    private int bufSize;
    private long loc = 0;
    private final long fileSize;
    private int written = 0;

    public FileWriter(String file, int bufSize, long fileSize) throws IOException {
        this.bufSize = bufSize;
        this.fileSize = fileSize;
        this.writer = new BufferedWriter(new Writer())
        storeBuf = ByteBuffer.allocate(bufSize);
        writeBuf = ByteBuffer.allocate(bufSize);
        istoreBuf = storeBuf.asIntBuffer();
    }

    public void add(int i){
        System.out.println(i);
        istoreBuf.put(i);
        written++;
        if (!istoreBuf.hasRemaining()){
            write();
        }
    }

    public void addArray(int[] i){
        for (int j: i) {
            this.add(j);
        }
    }

    private void write(){
        System.out.println("WRITE");
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

        System.out.println("WRITING: " + loc + "/" + fileSize);

        if (loc + bufSize >= fileSize){
            bufSize = toIntExact(fileSize - loc);
            storeBuf = ByteBuffer.allocate(bufSize);
        }

        loc += bufSize;


        istoreBuf = storeBuf.asIntBuffer();

    }

    public void close() throws IOException{
        afc.close();
    }

    public void printRemaining(){
        System.out.println("WRITTEN: " + written);
        System.out.println("REMAINING: " + istoreBuf.remaining());
        System.out.println("CAPACITY: " + istoreBuf.capacity());
    }
}
*/