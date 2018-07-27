package org.apache.spark;

public class myListener implements CleanerListener{
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
}
