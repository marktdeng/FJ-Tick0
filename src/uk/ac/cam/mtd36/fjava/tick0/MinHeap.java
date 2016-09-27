package uk.ac.cam.mtd36.fjava.tick0;

public class MinHeap {

    private int size = 0;
    private FileStream[] streams;

    public MinHeap(int size) {
        streams = new FileStream[size];
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void add(FileStream x) {
        streams[size] = x;
        heapifyUp(size);
        size++;
    }

    public FileStream pop() {
        FileStream min = streams[0];
        streams[0] = streams[size - 1];

        size--;
        if (size > 0) {
            heapify(0);
        }

        return min;
    }

    private void swap(int a, int b) {
        FileStream tmp = streams[a];
        streams[a] = streams[b];
        streams[b] = tmp;
    }

    private void heapifyUp(int i) {
        if (i > 0) {
            int parent = (i - 1) / 2;

            if (streams[parent].peek() > streams[i].peek()) {
                swap(i, parent);
                heapifyUp(parent);
            }
        }
    }

    private void heapify(int i) {
        int left = 2 * i + 1;
        int right = 2 * i + 2;
        int smallest;

        if (left >= size && right >= size) {
            return;
        } else if (right >= size) {
            smallest = left;
        } else {
            if (streams[left].peek() <= streams[right].peek()) {
                smallest = left;
            } else {
                smallest = right;
            }
        }

        if (streams[i].peek() > streams[smallest].peek()) {
            swap(i, smallest);
            heapify(smallest);
        }
    }
}