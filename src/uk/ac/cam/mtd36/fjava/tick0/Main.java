package uk.ac.cam.mtd36.fjava.tick0;

import java.util.Arrays;
import java.util.LinkedList;

public class Main {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        //int filenum = 16;
        //ExternalSort.sort("test-suite/test" + filenum + "a.dat", "test-suite/test" + filenum + "b.dat");

        for (int i=1; i<=17; i++) {
            System.out.println("FILE " + i);
            ExternalSort.sort("test-suite/test" + i + "a.dat", "test-suite/test" + i + "b.dat");
            System.out.println(ExternalSort.checkSum("test-suite/test" + i + "a.dat"));
        }

        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);
    }

}