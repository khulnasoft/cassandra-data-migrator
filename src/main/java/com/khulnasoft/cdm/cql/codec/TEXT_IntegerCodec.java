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
package com.khulnasoft.cdm.cql.codec;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

import com.khulnasoft.cdm.properties.PropertyHelper;
import com.khulnasoft.oss.driver.api.core.ProtocolVersion;
import com.khulnasoft.oss.driver.api.core.type.DataType;
import com.khulnasoft.oss.driver.api.core.type.DataTypes;
import com.khulnasoft.oss.driver.api.core.type.codec.TypeCodecs;
import com.khulnasoft.oss.driver.api.core.type.reflect.GenericType;

public class TEXT_IntegerCodec extends AbstractBaseCodec<Integer> {

    public TEXT_IntegerCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
    }

    @Override
    public @NotNull GenericType<Integer> getJavaType() {
        return GenericType.INTEGER;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(Integer value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            String stringValue = value.toString();
            return TypeCodecs.TEXT.encode(stringValue, protocolVersion);
        }
    }

    @Override
    public Integer decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        String stringValue = TypeCodecs.TEXT.decode(bytes, protocolVersion);
        return Integer.parseInt(stringValue);
    }

    @Override
    public @NotNull String format(Integer value) {
        return TypeCodecs.INT.format(value);
    }

    @Override
    public Integer parse(String value) {
        return value == null ? null : Integer.parseInt(value);
    }
}
