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
/* io.trino.plugin.starrocks.StarRocksTableProperties.java */
package io.trino.plugin.starrocks;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import com.google.inject.Inject;
import java.util.List;
import java.util.regex.Pattern;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class StarRocksTableProperties implements TablePropertiesProvider {

    // Standard property names for the WITH clause
    public static final String ENGINE = "engine";
    public static final String TABLE_TYPE = "table_type";
    public static final String TABLE_KEYS = "table_keys";
    public static final String PARTITION_BY = "partition_by";
    public static final String DISTRIBUTED_BY = "distributed_by";
    public static final String BUCKETS = "buckets";
    public static final String ROLLUP = "roll_up";
    public static final String ORDER_BY = "order_by";
    public static final String TABLE_COMMENT = "comment";
    public static final String REPLICATION_NUMBER = "replication_num";
    public static final String BUCKET_SIZE = "bucket_size";
    public static final String BLOOM_FILTER_COLUMNS = "bloom_filter_columns";

    // Patterns for extracting properties from SHOW CREATE TABLE output
    public static final Pattern ENGINE_PATTERN = Pattern.compile("ENGINE=([A-Z]+)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static final Pattern TABLE_TYPE_PATTERN = Pattern.compile(
            "(DUPLICATE|UNIQUE|AGGREGATE|PRIMARY) KEY\\(`?(.+?)`?\\)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static final Pattern PARTITION_BY_PATTERN = Pattern.compile(
            "PARTITION BY (.*?)(\\s+DISTRIBUTED BY|\\s+PROPERTIES|\\s+ORDER BY|\\s*$)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static final Pattern DISTRIBUTED_BY_PATTERN = Pattern.compile("DISTRIBUTED BY HASH\\((.*?)\\)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static final Pattern BUCKETS_PATTERN = Pattern.compile("BUCKETS (\\d+)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static final Pattern ORDER_BY_PATTERN = Pattern.compile("ORDER BY \\((.*?)\\)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static final Pattern TABLE_COMMENT_PATTERN = Pattern.compile("COMMENT '(.*?)'",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static final Pattern PROPS_BLOCK_PATTERN = Pattern.compile("PROPERTIES \\((.*?)\\);?$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static final Pattern REPLICATION_NUM_PATTERN = Pattern.compile("\"replication_num\"\\s*=\\s*\"(.*?)\"");
    public static final Pattern BUCKET_SIZE_PATTERN = Pattern.compile(
            "\"dynamic_partition\\.bucket_size\"\\s*=\\s*\"(.*?)\"");
    public static final Pattern BLOOM_FILTER_PATTERN = Pattern.compile("\"bloom_filter_columns\"\\s*=\\s*\"(.*?)\"");

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public StarRocksTableProperties() {
        this.properties = ImmutableList.of(
                stringProperty(
                        ENGINE,
                        "The StarRocks storage engine type (e.g., OLAP). Defaults to OLAP.",
                        null,
                        false
                ),
                stringProperty(
                        TABLE_TYPE,
                        "The StarRocks table model: 'DUPLICATE', 'UNQIUE', 'AGGREGATE', or 'PRIMARY'.",
                        null,
                        false
                ),
                stringProperty(
                        TABLE_KEYS,
                        "Comma-separated list of key columns based on the table model (e.g., 'k1,k2').",
                        null,
                        false
                ),
                stringProperty(
                        PARTITION_BY,
                        "The StarRocks partitioning definition (e.g., 'DATE_TRUNC(\\'day\\', dt)') OR (dt).",
                        null,
                        false
                ),
                stringProperty(
                        DISTRIBUTED_BY,
                        "Comma-separated list of column names for hash distribution.",
                        null,
                        false
                ),
                stringProperty(
                        BUCKETS,
                        "The number of buckets for data distribution (e.g., '10').",
                        null,
                        false
                ),
                stringProperty(
                        ROLLUP,
                        "JSON array definition for creating rollup tables (e.g., '[\"r1(k1)\"]').",
                        null,
                        false
                ),
                stringProperty(
                        ORDER_BY,
                        "Comma-separated list of columns for data sorting (required for Primary/Unique/Aggregate tables).",
                        null,
                        false
                ),
                stringProperty(
                        TABLE_COMMENT,
                        "Optional comment for the table. Overrides the standard DDL COMMENT if present.",
                        null,
                        false
                ),
                stringProperty(
                        REPLICATION_NUMBER,
                        "The number of replicas for each tablet (e.g., '3').",
                        null,
                        false
                ),
                stringProperty(
                        BUCKET_SIZE,
                        "The number of rows per bucket (e.g., '1000000').",
                        null,
                        false
                ),
                stringProperty(
                        BLOOM_FILTER_COLUMNS,
                        "Comma-separated list of columns on which to apply a Bloom filter.",
                        null,
                        false
                )
        );
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return properties;
    }
}
