package com.risenture.dse.cassandra.core;

import com.risenture.dse.cassandra.core.initializers.YamlInitializer;
import com.risenture.dse.exception.InitializationException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by bacholaa on 8/31/17.
 */
public class CassandraContextTest {

    @Before
    public void setup() {

    }

    @Test(expected = InitializationException.class)
    public void testInitializer_WhenInitializerNull_ShouldThrowException(){
        CassandraContext context = new CassandraContext(null);
    }

}
