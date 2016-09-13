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
    private long blocksize, filesize, location;
    private boolean offset, newblock;
    private ByteBuffer buf;
    private Future future;
    private AsynchronousFileChannel rafc;
    private int[] st;
    private int stloc;
    private int previous;


    public FileSection(int bufsize, long blocksize, boolean offset, String f) throws IOException, EOFException, EndBlockException {
        this.rafc = AsynchronousFileChannel.open(Paths.get(f), READ);
        this.filesize = this.rafc.size();
        location = (offset ? blocksize : 0);
        if (location + bufsize >= filesize){
            bufsize = toIntExact(filesize - location);
            if (bufsize <= 0) {
                throw new EOFException();
            }
        }
        this.bufsize = bufsize;
        this.blocksize = blocksize;
        this.offset = offset;

        buf = ByteBuffer.allocate(this.bufsize);

        future = rafc.read(buf, location);
        read();
    }

    public void read() throws EOFException, EndBlockException, IOException {
        if (newblock){
            newblock = false;
            throw new EndBlockException();
        }
        if (bufsize <= 0) {
            rafc.close();
            throw new EOFException();
        }
        while (!future.isDone()) {
            //Waiting
        }
        buf.flip();
        st = new int[bufsize / 4];
        buf.asIntBuffer().get(st);
        stloc = 0;
        buf.clear();

        location += bufsize;
        if ( (location / blocksize) % 2 == (offset ? 0 : 1) ){
            location += blocksize;
            newblock = true;
        }
        if (location > filesize){
            rafc.close();
            throw new EOFException();
        } else if (location + bufsize >= filesize){
            bufsize = toIntExact(filesize - location);

            buf = ByteBuffer.allocate(bufsize);
        }
        future = rafc.read(buf, location);

    }

    public int pop() throws EOFException, IOException, EndBlockException {
        int result = st[stloc++];
        System.out.println(stloc);
        if (stloc >= bufsize/4){
            read();
        }
        if (stloc != 1 && previous != 0 && previous > result){
            System.out.println(previous + " - " + result);
            System.out.println(location);
            throw new IllegalStateException();
        }
        previous = result;
        return result;
    }

    public int peek() {
        int result = st[stloc];
        return result;
    }

}
