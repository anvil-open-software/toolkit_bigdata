package com.dematic.labs.toolkit.cassandra;

import com.datastax.driver.core.Cluster;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public final class EmbeddedCassandraRule extends ExternalResource {
    private TemporaryFolder storageDir = new TemporaryFolder();

    private Cluster cluster;

    @Override
    protected void before() throws Throwable {
        // create the dir and set property
        storageDir.create();
        System.setProperty("cassandra.storagedir", storageDir.getRoot().getPath());
        // start cassandra
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
        // configure the client connection to the cluster
        cluster = Cluster.builder().addContactPoints(EmbeddedCassandraServerHelper.getHost()).
                withPort(EmbeddedCassandraServerHelper.getNativeTransportPort()).build();
    }

    public void dropKeySpace() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    public Cluster getCluster() {
        return cluster;
    }

    @Override
    protected void after() {
        // delete the dir
        try {
            storageDir.delete();
        } finally { // embedded server is shutdown automatically using shutdown hooks
            // close the client
            cluster.close();
        }
    }
}
