package uk.ac.cam.mtd36.fjava.tick0;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;
import static java.nio.file.StandardOpenOption.READ;

public class FileBlock {
    private AsynchronousFileChannel afc;
    private FileBlock next = null;
    private ByteBuffer buf;
    private Future future;
    private long startlocation, readlocation, locationinblock, blocksize;
    private int bufsize;
    private int[] st;
    private int stloc = 0;
    private boolean endblock = false;
    private boolean isdone = false;
    private boolean ready = false;

    private void setNext(FileBlock next) {
        this.next = next;
    }

    public boolean isdone() {
        return isdone;
    }

    public long getStartlocation() {
        return startlocation;
    }

    public FileBlock(String file, int buffersize, long blocksize, long startlocation) throws IOException, EndBlockException {
        this.afc = AsynchronousFileChannel.open(Paths.get(file), READ);
        this.startlocation = startlocation;
        this.readlocation = startlocation;
        this.bufsize = buffersize;
        this.blocksize = blocksize;
        this.locationinblock = 0;
    }

    public long getBlocksize() {
        return blocksize;
    }

    public void PrepareBlock() throws IOException, EndBlockException{
        if (!ready) {
            buf = ByteBuffer.allocate(bufsize);
            future = afc.read(buf, readlocation);
            readlocation += bufsize;
            read();
        }
    }

    private void read() throws IOException, EndBlockException{
        if (endblock){
            isdone = true;
            if (next == null){
                afc.close();
                throw new EndBlockException();
            } else {
                afc.close();
                next.PrepareBlock();
                throw new EndBlockException();
            }
        }
        while (!future.isDone()) {
            //Waiting
        }
        buf.flip();
        st = new int[bufsize / 4];
        buf.asIntBuffer().get(st);
        if (st == null){
            System.out.println("ERROR");
        }
        stloc = 0;
        buf.clear();
        readlocation += bufsize;
        locationinblock += bufsize;
        if (locationinblock >= blocksize){
            endblock = true;
        } else {

            if (locationinblock + bufsize >= blocksize){
                bufsize = toIntExact(blocksize - readlocation);
                if (bufsize <= 0){
                    endblock = true;
                }
                buf = ByteBuffer.allocate(bufsize);
            }
            future = afc.read(buf, readlocation);
        }
    }

    public int pop() throws IOException, EndBlockException {
        if (stloc >= bufsize/4){
            read();
        }
        int result = st[stloc++];
        return result;
    }

    public int peek() throws IOException, EndBlockException{
        if (stloc >= bufsize/4){
            read();
        }
        try {
            int result = st[stloc];
            return result;
        } catch (NullPointerException n){
            n.printStackTrace();
            return 0;
        }
    }

    public static FileBlock[] getblocks(long blocksize, int buffersize, long filesize, String file) throws IOException, EndBlockException {
        ArrayList<FileBlock> blocks = new ArrayList<>();
        long size;
        FileBlock previous1, previous2;
        FileBlock newblock;

        size = Math.min(blocksize, filesize);
        newblock = new FileBlock(file, buffersize, size, 0);
        blocks.add(newblock);
        previous2 = newblock;

        size = Math.min(blocksize, filesize - blocksize);
        newblock = new FileBlock(file, buffersize, size, blocksize);
        blocks.add(newblock);
        previous1 = newblock;

        for (long i = blocksize * 2; i < filesize; i+= blocksize){
            size = Math.min(blocksize, filesize-i);
            newblock = new FileBlock(file, buffersize, size, i);
            blocks.add(newblock);

            previous2.setNext(newblock);
            previous2 = previous1;
            previous1 = newblock;

        }
        blocks.get(0).PrepareBlock();
        blocks.get(1).PrepareBlock();
        return blocks.toArray(new FileBlock[blocks.size()]);
    }

}
