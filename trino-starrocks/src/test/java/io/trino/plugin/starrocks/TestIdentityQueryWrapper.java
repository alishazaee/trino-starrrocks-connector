package io.trino.plugin.starrocks;

import io.trino.plugin.jdbc.PreparedQuery;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestIdentityQueryWrapper
{
    @Test
    void removesOnlyExactIdentityProjection()
    {
        PreparedQuery source = query("SELECT `a`, `b` FROM t WHERE `a` > ? ORDER BY `b` LIMIT 7");
        PreparedQuery wrapper = query("SELECT `a`, `b` FROM (" + source.query() + ") o");
        assertThat(StarRocksClient.removeIdentityWrapper(wrapper, source, "`a`, `b`"))
                .isSameAs(source);
    }

    @Test
    void preservesPruningReorderingAndOperations()
    {
        PreparedQuery source = query("SELECT `a`, `b` FROM t");
        for (String projection : List.of("`a`", "`b`, `a`", "count(*)")) {
            PreparedQuery wrapper = query("SELECT " + projection + " FROM (" + source.query() + ") o");
            assertThat(StarRocksClient.removeIdentityWrapper(wrapper, source, projection)).isSameAs(wrapper);
        }
        for (String suffix : List.of(" WHERE `a` > ?", " ORDER BY `b` LIMIT 2", " GROUP BY `a`, `b`")) {
            PreparedQuery wrapper = query("SELECT `a`, `b` FROM (" + source.query() + ") o" + suffix);
            assertThat(StarRocksClient.removeIdentityWrapper(wrapper, source, "`a`, `b`")).isSameAs(wrapper);
        }
    }

    @Test
    void preservesExpressionAndAliasBoundaries()
    {
        for (String sql : List.of("SELECT `a` AS `b` FROM t", "SELECT DISTINCT `b` FROM t", "SELECT 'FROM t' AS `b` FROM t")) {
            PreparedQuery source = query(sql);
            PreparedQuery wrapper = query("SELECT `b` FROM (" + sql + ") o");
            assertThat(StarRocksClient.removeIdentityWrapper(wrapper, source, "`b`")).isSameAs(wrapper);
        }
    }

    private static PreparedQuery query(String sql)
    {
        return new PreparedQuery(sql, List.of());
    }
}
