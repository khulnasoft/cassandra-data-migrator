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

public class ASCII_BLOBCodec extends AbstractBaseCodec<ByteBuffer> {

    public ASCII_BLOBCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
    }

    @Override
    public @NotNull GenericType<ByteBuffer> getJavaType() {
        return GenericType.BYTE_BUFFER;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.ASCII;
    }

    @Override
    public ByteBuffer encode(ByteBuffer value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            String stringVal = new String(value.array());
            return TypeCodecs.ASCII.encode(stringVal, protocolVersion);
        }
    }

    @Override
    public ByteBuffer decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        String stringValue = TypeCodecs.ASCII.decode(bytes, protocolVersion);
        return ByteBuffer.wrap(stringValue.getBytes());
    }

    @Override
    public @NotNull String format(ByteBuffer value) {
        String stringVal = new String(value.array());
        return TypeCodecs.ASCII.format(stringVal);
    }

    @Override
    public ByteBuffer parse(String value) {
        return ByteBuffer.wrap(value.getBytes());
    }
}
