/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.starrocks;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;

/** StarRocks-specific connection and table-creation settings. */
public class StarRocksConfig {
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private int defaultReplicationNumber = 1;
    private int defaultBucketCount = 8;

    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    @Config("starrocks.auto-reconnect")
    @ConfigDescription("Reconnect metadata and query connections after transient failures")
    public StarRocksConfig setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects() {
        return maxReconnects;
    }

    @Config("starrocks.max-reconnects")
    @ConfigDescription("Maximum reconnection attempts made by the JDBC driver")
    public StarRocksConfig setMaxReconnects(int maxReconnects) {
        this.maxReconnects = maxReconnects;
        return this;
    }

    @Min(1)
    public int getDefaultReplicationNumber() {
        return defaultReplicationNumber;
    }

    @Config("starrocks.default-replication-number")
    @ConfigDescription("Replication number used for tables created by Trino")
    public StarRocksConfig setDefaultReplicationNumber(int defaultReplicationNumber) {
        this.defaultReplicationNumber = defaultReplicationNumber;
        return this;
    }

    @Min(1)
    public int getDefaultBucketCount() {
        return defaultBucketCount;
    }

    @Config("starrocks.default-bucket-count")
    @ConfigDescription("Hash bucket count used for tables created by Trino")
    public StarRocksConfig setDefaultBucketCount(int defaultBucketCount) {
        this.defaultBucketCount = defaultBucketCount;
        return this;
    }
}
