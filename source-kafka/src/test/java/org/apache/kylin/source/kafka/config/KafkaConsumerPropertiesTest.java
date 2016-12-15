package org.apache.kylin.source.kafka.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaConsumerPropertiesTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testLoadKafkaConfig() {
        KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties.getInstanceFromEnv();
        assertFalse(kafkaConsumerProperties.getProperties().containsKey("acks"));
        assertTrue(kafkaConsumerProperties.getProperties().containsKey("session.timeout.ms"));
        assertEquals("30000", kafkaConsumerProperties.getProperties().getProperty("session.timeout.ms"));
    }

}
