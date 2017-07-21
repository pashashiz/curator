package com.ps;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class CuratorTest {

    private CuratorFramework client;

    @Before
    public void connect() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
        client.start();
        if (client.checkExists().forPath("/test") != null)
            client.delete().deletingChildrenIfNeeded().forPath("/test");
        client.create().forPath("/test", new byte[0]);
    }

    @Test
    public void CRUD() throws Exception {
        client.create().forPath("/test/hello", "hello pavlo".getBytes());
        Stat stat = client.checkExists().forPath("/test/hello");
        assertThat("created node should exist", stat, notNullValue());
        byte[] data1 = client.getData().forPath("/test/hello");
        assertThat("node data should be created", new String(data1), is("hello pavlo"));
        client.setData().withVersion(stat.getVersion()).forPath("/test/hello", "hello randy".getBytes());
        byte[] data2 = client.getData().forPath("/test/hello");
        assertThat("node data should be modified", new String(data2), is("hello randy"));
        client.delete().forPath("/test/hello");
        assertThat("node should be deleted", client.checkExists().forPath("/test/hello"), nullValue());
    }

    @Test
    public void watcher() throws Exception {
        CountDownLatch changed = new CountDownLatch(1);
        client.create().forPath("/test/watched", "data".getBytes());
        client.checkExists().usingWatcher((CuratorWatcher) event -> {
            assertThat("watched node should be updated", event.getPath(), is("/test/watched"));
            changed.countDown();
        }).forPath("/test/watched");
        client.setData().forPath("/test/watched", "updated".getBytes());
        assertThat("watched node should be updated",
                changed.await(5, TimeUnit.SECONDS), is(true));
    }

    @After
    public void close() {
        client.close();
    }
}
