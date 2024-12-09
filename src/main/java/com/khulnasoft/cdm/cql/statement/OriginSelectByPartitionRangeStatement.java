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
package com.khulnasoft.cdm.cql.statement;

import java.math.BigInteger;

import com.khulnasoft.cdm.cql.EnhancedSession;
import com.khulnasoft.cdm.feature.Featureset;
import com.khulnasoft.cdm.feature.OriginFilterCondition;
import com.khulnasoft.cdm.properties.IPropertyHelper;
import com.khulnasoft.cdm.properties.KnownProperties;
import com.khulnasoft.cdm.properties.PropertyHelper;
import com.khulnasoft.oss.driver.api.core.cql.BoundStatement;
import com.khulnasoft.oss.driver.api.core.cql.PreparedStatement;

public class OriginSelectByPartitionRangeStatement extends OriginSelectStatement {
    public OriginSelectByPartitionRangeStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);
    }

    @Override
    public BoundStatement bind(Object... binds) {
        if (null == binds || binds.length != 2 || !(binds[0] instanceof BigInteger)
                || !(binds[1] instanceof BigInteger))
            throw new RuntimeException("Expected 2 not-null binds of type BigInteger, got " + binds.length);

        BigInteger min = (BigInteger) binds[0];
        BigInteger max = (BigInteger) binds[1];

        PreparedStatement preparedStatement = prepareStatement();
        // random partitioner uses BigInteger, the normal partitioner uses long
        return preparedStatement
                .bind(cqlTable.hasRandomPartitioner() ? min : min.longValueExact(),
                        cqlTable.hasRandomPartitioner() ? max : max.longValueExact())
                .setConsistencyLevel(cqlTable.getReadConsistencyLevel()).setPageSize(cqlTable.getFetchSizeInRows());
    }

    @Override
    protected String whereBinds() {
        String partitionKey = PropertyHelper
                .asString(cqlTable.getPartitionKeyNames(true), KnownProperties.PropertyType.STRING_LIST).trim();
        return "TOKEN(" + partitionKey + ") >= ? AND TOKEN(" + partitionKey + ") <= ?";
    }

    @Override
    protected String buildStatement() {
        StringBuilder sb = new StringBuilder(super.buildStatement());
        sb.append(((OriginFilterCondition) cqlTable.getFeature(Featureset.ORIGIN_FILTER)).getFilterCondition());
        sb.append(" ALLOW FILTERING");
        return sb.toString();
    }
}
