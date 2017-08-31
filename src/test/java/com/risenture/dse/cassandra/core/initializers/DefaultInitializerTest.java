package com.risenture.dse.cassandra.core.initializers;

import com.datastax.driver.core.Host;
import com.risenture.dse.exception.InitializationException;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by bacholaa on 8/31/17.
 */
public class DefaultInitializerTest {

    private static final String CLUSTER = "cluster01";
    private static final String CONTACT_POINTS = "127.0.0.1, 127.0.0.2";
    @Before
    public void setup() {

    }

    @Test(expected = InitializationException.class)
    public void testInit_WhenContactPointsNull_ShouldThrowException() {
        DefaultInitializer initializer = new DefaultInitializer(null, CLUSTER);
    }

    @Test(expected = InitializationException.class)
    public void testInit_WhenClusterNameNull_ShouldThrowException() {
        DefaultInitializer initializer = new DefaultInitializer(CONTACT_POINTS, null);
    }

    @Test
    public void testGetContactPoints_WhenValidContactPoints_ShouldReturnList() throws Exception {
        DefaultInitializer initializer = new DefaultInitializer(CONTACT_POINTS, CLUSTER);
        List<InetSocketAddress> inetSocketAddressList = initializer.getContactPoints();

        assertNotNull(inetSocketAddressList);
        assertEquals(2, inetSocketAddressList.size());
    }

    @Test
    public void testGetInitialListeners_ShouldSetupSuccessfully() {
        DefaultInitializer initializer = new DefaultInitializer(CONTACT_POINTS, CLUSTER);
        List<Host.StateListener> listeners = initializer.getInitialListeners();
        assertNotNull(listeners);
        assertNotEquals(0, listeners.size());
    }

}
