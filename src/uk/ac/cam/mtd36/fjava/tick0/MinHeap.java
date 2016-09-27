package uk.ac.cam.mtd36.fjava.tick0;

public class MinHeap {

    private int size = 0;
    private FileStream[] streams;

    public MinHeap(int size){
        streams = new FileStream[size];
    }

    public boolean isEmpty(){
        return size == 0;
    }

    public void add(FileStream x){
        streams[size] = x;
        heapifyUp(size);
        size++;
    }

    public FileStream pop(){
        FileStream min = streams[0];
        streams[0] = streams[size-1];

        size--;
        if (size > 0){
            heapify(0);
        }

        return min;
    }

    private void swap(int a, int b){
        FileStream temp = streams[a];
        streams[a] = streams[b];
        streams[b] = temp;
    }

    private void heapifyUp(int i){
        if (i > 0){
            int parent = (i-1)/2;

            if (streams[parent].peek() > streams[i].peek()){
                swap(i, parent);
                heapifyUp(parent);
            }
        }
    }

    private void heapify(int i){
        int leftIndex = 2*i + 1;
        int rightIndex = 2*i + 2;
        int smallestIndex;

        if (leftIndex >= size && rightIndex >= size){
            return;
        } else if (rightIndex >= size){
            smallestIndex = leftIndex;
        } else {
            if (streams[leftIndex].peek() <= streams[rightIndex].peek()){
                smallestIndex = leftIndex;
            } else {
                smallestIndex = rightIndex;
            }
        }

        if (streams[i].peek() > streams[smallestIndex].peek()){
            swap(i, smallestIndex);
            heapify(smallestIndex);
        }
    }
}