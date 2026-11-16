package io.trino.plugin.starrocks.aggregation;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Pushes Trino's conditional count directly to StarRocks.
 *
 * <p>The argument is translated by the connector expression rewriter before being embedded in
 * {@code count_if(...)}. In Trino 484, a computed predicate is normally separated from its
 * aggregation by a local projection, so the common pushdown case is a physical boolean column.</p>
 */
public final class ImplementCountIf
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private final JdbcTypeHandle bigintTypeHandle;

    public ImplementCountIf(JdbcTypeHandle bigintTypeHandle)
    {
        this.bigintTypeHandle = requireNonNull(bigintTypeHandle, "bigintTypeHandle is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return Pattern.typeOf(AggregateFunction.class);
    }

    @Override
    public Optional<JdbcExpression> rewrite(
            AggregateFunction aggregateFunction,
            Captures captures,
            RewriteContext<ParameterizedExpression> context)
    {
        if (!aggregateFunction.getFunctionName().equals("count_if") ||
                !aggregateFunction.getOutputType().equals(BIGINT) ||
                aggregateFunction.getArguments().size() != 1 ||
                aggregateFunction.isDistinct() ||
                !aggregateFunction.getSortItems().isEmpty() ||
                aggregateFunction.getFilter().isPresent()) {
            return Optional.empty();
        }

        return context.rewriteExpression(aggregateFunction.getArguments().getFirst())
                .map(argument -> new JdbcExpression(
                        format("count_if(%s)", argument.expression()),
                        argument.parameters(),
                        bigintTypeHandle));
    }
}
