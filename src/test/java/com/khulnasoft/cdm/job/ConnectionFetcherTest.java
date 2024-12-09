/*
 * Copyright KhulnaSoft, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.khulnasoft.cdm.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.khulnasoft.cdm.cql.CommonMocks;
import com.khulnasoft.cdm.data.PKFactory;
import com.khulnasoft.cdm.properties.IPropertyHelper;
import com.khulnasoft.cdm.properties.KnownProperties;

public class ConnectionFetcherTest extends CommonMocks {

    @Mock
    IPropertyHelper propertyHelper;

    @Mock
    private SparkConf conf;

    private ConnectionFetcher cf;

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        commonSetupWithoutDefaultClassVariables();
        MockitoAnnotations.openMocks(this);

        cf = new ConnectionFetcher(propertyHelper);
    }

    @Test
    public void getConnectionDetailsOrigin() {
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_HOST)).thenReturn("origin_host");
        when(propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_HOST)).thenReturn("target_host");
        ConnectionDetails cd = cf.getConnectionDetails(PKFactory.Side.ORIGIN);
        assertEquals("origin_host", cd.host());
    }

    @Test
    public void getConnectionDetailsTarget() {
        when(propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_HOST)).thenReturn("origin_host");
        when(propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_HOST)).thenReturn("target_host");
        ConnectionDetails cd = cf.getConnectionDetails(PKFactory.Side.TARGET);
        assertEquals("target_host", cd.host());
    }

}
