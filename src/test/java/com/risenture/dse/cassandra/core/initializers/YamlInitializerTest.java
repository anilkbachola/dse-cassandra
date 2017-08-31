package com.risenture.dse.cassandra.core.initializers;

import com.risenture.dse.exception.InitializationException;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by bacholaa on 8/31/17.
 */
public class YamlInitializerTest {

    @Before
    public void setup() {

    }

    @Test(expected = InitializationException.class)
    public void testInitialize_WhenConfigFileNotFound_ShouldThrowException() {
        YamlInitializer yamlInitializer = new YamlInitializer("test.yml");
    }

    @Test()
    public void testInitialize_WhenConfigFileNotProvided_ShouldSearchForDefaultFile() {
        YamlInitializer yamlInitializer = new YamlInitializer();
        assertNotNull(yamlInitializer);
    }

    @Test()
    public void testGetContactPoints_WhenConfigFileFound_ShouldReturnListFromConfigFile() {
        YamlInitializer yamlInitializer = new YamlInitializer();
        List<InetSocketAddress> inetAddressList = yamlInitializer.getContactPoints();
        assertNotNull(inetAddressList);
        assertEquals(1, inetAddressList.size());
        assertEquals("localhost", inetAddressList.get(0).getHostName());
    }
}
