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

import java.util.concurrent.CompletionStage;

import com.khulnasoft.cdm.cql.EnhancedSession;
import com.khulnasoft.cdm.data.EnhancedPK;
import com.khulnasoft.cdm.data.PKFactory;
import com.khulnasoft.cdm.data.Record;
import com.khulnasoft.cdm.properties.IPropertyHelper;
import com.khulnasoft.cdm.properties.KnownProperties;
import com.khulnasoft.cdm.properties.PropertyHelper;
import com.khulnasoft.oss.driver.api.core.cql.AsyncResultSet;
import com.khulnasoft.oss.driver.api.core.cql.BoundStatement;
import com.khulnasoft.oss.driver.api.core.cql.ResultSet;
import com.khulnasoft.oss.driver.api.core.cql.Row;

public class TargetSelectByPKStatement extends BaseCdmStatement {
    public TargetSelectByPKStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);
        this.statement = buildStatement();
    }

    public Record getRecord(EnhancedPK pk) {
        BoundStatement boundStatement = bind(pk);
        if (null == boundStatement)
            return null;

        ResultSet resultSet = session.getCqlSession().execute(boundStatement);
        if (null == resultSet)
            return null;

        Row row = resultSet.one();
        if (null == row)
            return null;

        return new Record(pk, null, row);
    }

    public CompletionStage<AsyncResultSet> getAsyncResult(EnhancedPK pk) {
        BoundStatement boundStatement = bind(pk);
        if (null == boundStatement)
            return null;
        return session.getCqlSession().executeAsync(boundStatement);
    }

    private BoundStatement bind(EnhancedPK pk) {
        BoundStatement boundStatement = prepareStatement().bind()
                .setConsistencyLevel(cqlTable.getReadConsistencyLevel());

        boundStatement = session.getPKFactory().bindWhereClause(PKFactory.Side.TARGET, pk, boundStatement, 0);
        return boundStatement;
    }

    private String buildStatement() {
        return "SELECT "
                + PropertyHelper.asString(cqlTable.getColumnNames(true), KnownProperties.PropertyType.STRING_LIST)
                + " FROM " + cqlTable.getKeyspaceTable() + " WHERE "
                + session.getPKFactory().getWhereClause(PKFactory.Side.TARGET);
    }
}