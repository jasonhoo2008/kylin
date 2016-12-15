/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.source.kafka.config;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigCannotInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConsumerProperties {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerProperties.class);

    private volatile Properties properties = new Properties();

    public static final String KAFKA_CONSUMER_PROPERTIES_FILE = "kylin-kafka-consumer.properties";

    // static cached instances
    private static KafkaConsumerProperties ENV_INSTANCE = null;

    public static KafkaConsumerProperties getInstanceFromEnv() {
        synchronized (KafkaConsumerProperties.class) {
            if (ENV_INSTANCE == null) {
                try {
                    KafkaConsumerProperties config = new KafkaConsumerProperties();
                    config.properties = loadKafkaConsumerProperties();

                    logger.info("Initialized a new KafkaConsumerProperties from getInstanceFromEnv : " + System.identityHashCode(config));
                    ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KafkaConsumerProperties ", e);
                }
            }
            return ENV_INSTANCE;
        }
    }

    public Properties getProperties(){
        Properties result = new Properties();
        result.putAll(properties);
        return result;
    }

    public InputStream getHadoopJobConfInputStream() throws IOException {
        File kafkaProperties = getKafkaConsumerPropertiesFile();
        return FileUtils.openInputStream(kafkaProperties);
    }

    private static Properties loadKafkaConsumerProperties() {
        File propFile = getKafkaConsumerPropertiesFile();
        if (propFile == null || !propFile.exists()) {
            logger.error("fail to locate " + KAFKA_CONSUMER_PROPERTIES_FILE);
            throw new RuntimeException("fail to locate " + KAFKA_CONSUMER_PROPERTIES_FILE);
        }
        Properties conf = new Properties();
        try {
            FileInputStream is = new FileInputStream(propFile);
            conf.load(is);
            IOUtils.closeQuietly(is);

            File propOverrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
            if (propOverrideFile.exists()) {
                FileInputStream ois = new FileInputStream(propOverrideFile);
                Properties propOverride = new Properties();
                propOverride.load(ois);
                IOUtils.closeQuietly(ois);
                conf.putAll(propOverride);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return conf;
    }

    private static File getKafkaConsumerPropertiesFile() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String kylinConfHome = System.getProperty(KylinConfig.KYLIN_CONF);
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF=" + kylinConfHome);
            return getKafkaConsumerPropertiesFile(kylinConfHome);
        }

        logger.warn("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = kylinConfig.getKylinHome();
        if (StringUtils.isEmpty(kylinHome))
            throw new KylinConfigCannotInitException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");

        String path = kylinHome + File.separator + "conf";
        return getKafkaConsumerPropertiesFile(path);
    }

    private static File getKafkaConsumerPropertiesFile(String path) {
        if (path == null) {
            return null;
        }

        return new File(path, KAFKA_CONSUMER_PROPERTIES_FILE);
    }
}
