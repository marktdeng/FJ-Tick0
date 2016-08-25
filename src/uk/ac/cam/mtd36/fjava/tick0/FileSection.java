package uk.ac.cam.mtd36.fjava.tick0;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;

import static java.nio.file.StandardOpenOption.READ;

public class FileSection {
    private int bufsize;
    private long blocksize;
    private long filesize;
    private boolean offset;
    private ByteBuffer buf;
    private Future future;
    private AsynchronousFileChannel rafc;
    private long location;
    private int[] st;
    private int stloc;
    private int previous;


    public FileSection(int bufsize, long blocksize, boolean offset, String f) throws IOException, EOFException {
        this.rafc = AsynchronousFileChannel.open(Paths.get(f), READ);
        this.filesize = this.rafc.size();
        System.out.println(filesize);
        location = (offset ? blocksize : 0);
        if (location + bufsize >= filesize){
            this.bufsize = toIntExact(filesize - location);
            if (bufsize <= 0) {
                throw new EOFException();
            }
        } else {
            this.bufsize = bufsize;
        }
        this.blocksize = blocksize;
        this.offset = offset;

        buf = ByteBuffer.allocate(this.bufsize);

        future = rafc.read(buf, location);
        read();
    }

    private void read() throws EOFException, IOException {
        while (!future.isDone()) {
            //Waiting
        }
        buf.flip();
        st = new int[bufsize / 4];
        buf.asIntBuffer().get(st);
        stloc = 0;
        buf.clear();
        location += bufsize;
        if ( (location + 1 / blocksize) % 2 == (offset ? 1 : 0) ){
            location += blocksize;
        }
        if (location >= filesize){
            throw new EOFException();
        } else if (location + bufsize >= filesize){
            bufsize = toIntExact(filesize - location);
            if (bufsize <= 0) {
                rafc.close();
                throw new EOFException();
            }
            buf = ByteBuffer.allocate(bufsize);
        }
        future = rafc.read(buf, location);
    }

    public int pop() throws EOFException, IOException {
        int result = st[stloc++];
        if (stloc >= bufsize/4){
            read();
        }
        if (previous != 0 && previous > result){
            System.out.println(previous + " - " + result);
            throw new IllegalStateException();
        }
        previous = result;
        System.out.println(result);
        return result;
    }

    public int peek() {
        return st[stloc];
    }

}
