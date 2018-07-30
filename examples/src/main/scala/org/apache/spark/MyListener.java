package org.apache.spark;

public class MyListener implements CleanerListener{
    public MyListener(SparkContext sc) {
        this.sc = sc;
    }

    private SparkContext sc =null;
    @Override
    public void rddCleaned(int rddId) {

    }

    @Override
    public void shuffleCleaned(int shuffleId) {

    }

    @Override
    public void broadcastCleaned(long broadcastId) {

    }

    @Override
    public void accumCleaned(long accId) {

    }

    @Override
    public void checkpointCleaned(long rddId) {

    }

    public void setListener() {
        sc.cleaner().get().attachListener(this);
    }

    public void main(String[] args){
        MyListener listener =new MyListener(new SparkContext());
        listener.setListener();
    }
}
