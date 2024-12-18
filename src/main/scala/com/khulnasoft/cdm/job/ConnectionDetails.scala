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
package com.khulnasoft.cdm.job

case class ConnectionDetails(
                              scbPath: String,
                              host: String,
                              port: String,
                              username: String,
                              password: String,
                              sslEnabled: String,
                              trustStorePath: String,
                              trustStorePassword: String,
                              trustStoreType: String,
                              keyStorePath: String,
                              keyStorePassword: String,
                              enabledAlgorithms: String,
                              isAstra: Boolean
                            )