package uk.ac.cam.mtd36.fjava.tick0;

import java.util.Comparator;

public class FileStreamComparator implements Comparator<FileStream> {
    public int compare(FileStream a, FileStream b){
        return a.peek() - b.peek();
    }
}
