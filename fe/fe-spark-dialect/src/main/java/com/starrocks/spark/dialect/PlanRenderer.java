// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.spark.dialect;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Explode;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.CTERelationDef;
import org.apache.spark.sql.catalyst.plans.logical.CTERelationRef;
import org.apache.spark.sql.catalyst.plans.logical.Distinct;
import org.apache.spark.sql.catalyst.plans.logical.Except;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Generate;
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit;
import org.apache.spark.sql.catalyst.plans.logical.Intersect;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LocalLimit;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.RebalancePartitions;
import org.apache.spark.sql.catalyst.plans.logical.Repartition;
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression;
import org.apache.spark.sql.catalyst.plans.logical.ResolvedHint;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.catalyst.plans.logical.View;
import org.apache.spark.sql.catalyst.plans.logical.Window;
import org.apache.spark.sql.catalyst.plans.logical.WithCTE;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.ArrayType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Renders an analyzed Spark logical plan as a StarRocks query.
 *
 * <p>Plans are folded bottom-up into SQL query blocks ({@link Block}): an operator is merged into the block
 * below it while standard SQL clause order allows (FROM, WHERE, GROUP BY, HAVING, SELECT, DISTINCT, ORDER BY,
 * LIMIT); otherwise the block below becomes a derived table. Columns are tracked by Spark expression id, never by
 * name, so Spark's name resolution (which already happened) is never redone by StarRocks. Every table alias is
 * unique within the statement, so correlated subqueries can refer to outer columns directly.
 */
final class PlanRenderer {
    /** One SQL query block. */
    static final class Block {
        String from;
        boolean singleRelation;
        // Expression id -> SQL usable in WHERE / GROUP BY / SELECT of this block.
        Map<Long, String> inputCols = new HashMap<>();
        final List<String> where = new ArrayList<>();
        List<String> groupBy;
        final List<String> having = new ArrayList<>();
        // Null means the block outputs its input columns unchanged.
        List<String> select;
        boolean hasWindow;
        boolean distinct;
        List<String> orderBy;
        Long limit;
        List<Attribute> output;
        // A rendered UNION / INTERSECT / EXCEPT; all other fields except output are unused.
        String setOperation;
        List<String> setOperationNames;

        boolean isPlain() {
            return setOperation == null && select == null && groupBy == null && !distinct && orderBy == null
                    && limit == null;
        }

        boolean isBareFrom() {
            return isPlain() && where.isEmpty();
        }
    }

    // StarRocks catalog behind spark_catalog; tables of other catalogs carry their own.
    private final String sessionCatalog;
    private final ExpressionRenderer expressions;
    private final Deque<Map<Long, String>> outerScopes = new ArrayDeque<>();
    private final Map<Long, String> cteNames = new HashMap<>();
    private final Map<Long, List<String>> cteColumns = new HashMap<>();
    // Rendered CTE definitions, in dependency order.
    private final LinkedHashMap<Long, String> cteDefinitions = new LinkedHashMap<>();
    private int aliasCounter;

    PlanRenderer(String sessionCatalog, ExpressionRenderer.Context context) {
        this.sessionCatalog = sessionCatalog;
        this.expressions = new ExpressionRenderer(this, context);
    }

    ExpressionRenderer expressions() {
        return expressions;
    }

    /** Top-level query with Spark's output column names. */
    String renderQuery(LogicalPlan plan) {
        Block block = visit(plan);
        List<String> names = new ArrayList<>();
        for (Attribute attribute : Scala.list(plan.output())) {
            names.add(attribute.name());
        }
        return withClause() + render(block, names);
    }

    /**
     * A column of a written result: either the query's output column {@code inputIndex} or a constant,
     * cast to {@code type}.
     */
    record TargetColumn(String name, int inputIndex, String constantSql, String type) {
        static TargetColumn input(String name, int inputIndex, String type) {
            return new TargetColumn(name, inputIndex, null, type);
        }

        static TargetColumn constant(String name, String constantSql, String type) {
            return new TargetColumn(name, -1, constantSql, type);
        }
    }

    /**
     * Top-level query whose columns are explicitly cast. Used for CTAS and INSERT, where the written column types
     * must be Spark's even where StarRocks would infer a different type for the same expression.
     */
    String renderWrite(LogicalPlan plan, List<TargetColumn> columns) {
        Block block = visit(plan);
        String alias = nextAlias();
        List<String> innerNames = uniqueNames(block.output);
        StringJoiner items = new StringJoiner(", ");
        for (TargetColumn column : columns) {
            String value = column.constantSql() != null ? column.constantSql()
                    : alias + "." + Sql.quote(innerNames.get(column.inputIndex()));
            items.add(Sql.cast(value, column.type()) + " AS " + Sql.quote(column.name()));
        }
        return withClause() + "SELECT " + items + " FROM (" + render(block, innerNames) + ") " + alias;
    }

    /** Renders a subquery expression's plan; its outer references resolve against {@code scope}. */
    String renderSubquery(LogicalPlan plan, Map<Long, String> scope) {
        outerScopes.push(scope);
        try {
            Block block = visit(plan);
            return render(block, uniqueNames(block.output));
        } finally {
            outerScopes.pop();
        }
    }

    String lookupOuter(long exprId) {
        for (Map<Long, String> scope : outerScopes) {
            String sql = scope.get(exprId);
            if (sql != null) {
                return sql;
            }
        }
        return null;
    }

    private String withClause() {
        if (cteDefinitions.isEmpty()) {
            return "";
        }
        return "WITH " + String.join(", ", cteDefinitions.values()) + " ";
    }

    Block visit(LogicalPlan plan) {
        if (plan instanceof SubqueryAlias alias) {
            return visit(alias.child());
        } else if (plan instanceof View view) {
            return visit(view.child());
        } else if (plan instanceof ResolvedHint hint) {
            return visit(hint.child());
        } else if (plan instanceof RepartitionByExpression || plan instanceof Repartition
                || plan instanceof RebalancePartitions) {
            // Distribution only; does not change the result.
            return visit(plan.children().head());
        } else if (plan instanceof HiveTableRelation relation) {
            CatalogTable table = relation.tableMeta();
            return visitTable(qualifiedName(sessionCatalog, table.identifier().database().get(),
                    table.identifier().table()), Scala.list(relation.output()));
        } else if (plan instanceof DataSourceV2Relation relation) {
            Identifier identifier = relation.identifier().get();
            return visitTable(qualifiedName(starRocksCatalog(relation.catalog().get()),
                    identifier.namespace()[0], identifier.name()), Scala.list(relation.output()));
        } else if (plan instanceof OneRowRelation) {
            Block block = new Block();
            block.output = new ArrayList<>();
            return block;
        } else if (plan instanceof LocalRelation local) {
            return visitLocalRelation(local);
        } else if (plan instanceof WithCTE withCte) {
            for (CTERelationDef definition : Scala.list(withCte.cteDefs())) {
                defineCte(definition);
            }
            return visit(withCte.plan());
        } else if (plan instanceof CTERelationRef ref) {
            return visitCteRef(ref);
        } else if (plan instanceof Project project) {
            return visitProject(project);
        } else if (plan instanceof Filter filter) {
            return visitFilter(filter);
        } else if (plan instanceof Aggregate aggregate) {
            return visitAggregate(aggregate);
        } else if (plan instanceof Window window) {
            return visitWindow(window);
        } else if (plan instanceof Sort sort) {
            return visitSort(sort);
        } else if (plan instanceof GlobalLimit limit) {
            LogicalPlan child = limit.child() instanceof LocalLimit local ? local.child() : limit.child();
            return visitLimit(limit.limitExpr(), child);
        } else if (plan instanceof LocalLimit limit) {
            return visitLimit(limit.limitExpr(), limit.child());
        } else if (plan instanceof Distinct distinct) {
            return visitDistinct(distinct);
        } else if (plan instanceof Join join) {
            return visitJoin(join);
        } else if (plan instanceof Generate generate) {
            return visitGenerate(generate);
        } else if (plan instanceof Union union) {
            return visitSetOperation(Scala.list(union.children()), "UNION ALL", plan);
        } else if (plan instanceof Intersect intersect) {
            if (intersect.isAll()) {
                throw new SparkDialectException("INTERSECT ALL is not supported by StarRocks");
            }
            return visitSetOperation(List.of(intersect.left(), intersect.right()), "INTERSECT", plan);
        } else if (plan instanceof Except except) {
            if (except.isAll()) {
                throw new SparkDialectException("EXCEPT ALL is not supported by StarRocks");
            }
            return visitSetOperation(List.of(except.left(), except.right()), "EXCEPT", plan);
        }
        throw new SparkDialectException("Unsupported Spark plan node: " + plan.nodeName());
    }

    /** StarRocks catalog of a table in a non-session Spark catalog. */
    static String starRocksCatalog(CatalogPlugin plugin) {
        if (plugin instanceof BridgeTableCatalog tables) {
            return tables.bridge().catalogName();
        }
        throw new SparkDialectException("Spark catalog " + plugin.name() + " is not backed by StarRocks");
    }

    static String qualifiedName(String catalog, String db, String table) {
        return Sql.quote(catalog) + "." + Sql.quote(db) + "." + Sql.quote(table);
    }

    private Block visitTable(String qualifiedName, List<Attribute> output) {
        String alias = nextAlias();
        Block block = new Block();
        block.from = qualifiedName + " " + alias;
        block.singleRelation = true;
        block.output = output;
        for (Attribute attribute : block.output) {
            block.inputCols.put(attribute.exprId().id(), alias + "." + Sql.quote(attribute.name()));
        }
        return block;
    }

    private Block visitLocalRelation(LocalRelation local) {
        List<Attribute> output = Scala.list(local.output());
        List<InternalRow> rows = Scala.list(local.data());
        if (rows.isEmpty()) {
            throw new SparkDialectException("Empty local relations are not supported");
        }
        List<String> names = uniqueNames(output);
        StringJoiner union = new StringJoiner(" UNION ALL ");
        for (InternalRow row : rows) {
            StringJoiner items = new StringJoiner(", ");
            for (int i = 0; i < output.size(); i++) {
                Literal literal = new Literal(row.get(i, output.get(i).dataType()), output.get(i).dataType());
                items.add(expressions.literal(literal) + " AS " + Sql.quote(names.get(i)));
            }
            union.add("SELECT " + items);
        }
        return setOperationBlock(union.toString(), names, output);
    }

    private void defineCte(CTERelationDef definition) {
        long id = definition.id();
        if (cteNames.containsKey(id)) {
            return;
        }
        Block block = visit(definition.child());
        List<String> names = uniqueNames(block.output);
        String name = "cte_" + id;
        cteNames.put(id, name);
        cteColumns.put(id, names);
        cteDefinitions.put(id, Sql.quote(name) + " AS (" + render(block, names) + ")");
    }

    private Block visitCteRef(CTERelationRef ref) {
        String name = cteNames.get(ref.cteId());
        if (name == null) {
            throw new SparkDialectException("CTE " + ref.cteId() + " referenced before its definition");
        }
        List<String> columns = cteColumns.get(ref.cteId());
        String alias = nextAlias();
        Block block = new Block();
        block.from = Sql.quote(name) + " " + alias;
        block.singleRelation = true;
        block.output = Scala.list(ref.output());
        for (int i = 0; i < block.output.size(); i++) {
            block.inputCols.put(block.output.get(i).exprId().id(), alias + "." + Sql.quote(columns.get(i)));
        }
        return block;
    }

    private Block visitProject(Project project) {
        Block block = visit(project.child());
        Map<Long, String> scope;
        if (block.setOperation == null && block.select == null && !block.distinct && block.limit == null) {
            // Plain FROM/WHERE block, possibly already ordered: ORDER BY refers to inputs, so it stays valid.
            scope = block.inputCols;
        } else if (block.setOperation == null && block.select != null && !block.distinct && block.limit == null) {
            // Inline the projection into the block's own select list (also over GROUP BY / window blocks).
            scope = outputExprs(block);
        } else {
            block = wrap(block);
            scope = block.inputCols;
        }
        block.select = renderNamed(Scala.list(project.projectList()), scope);
        block.output = Scala.list(project.output());
        return block;
    }

    private Block visitFilter(Filter filter) {
        Block block = visit(filter.child());
        if (block.isPlain()) {
            block.where.add(expressions.render(filter.condition(), block.inputCols));
        } else if (block.groupBy != null && !block.hasWindow && !block.distinct && block.orderBy == null
                && block.limit == null) {
            block.having.add(expressions.render(filter.condition(), outputExprs(block)));
        } else if (block.setOperation == null && block.select != null && block.groupBy == null && !block.hasWindow
                && !block.distinct && block.orderBy == null && block.limit == null) {
            block.where.add(expressions.render(filter.condition(), outputExprs(block)));
        } else {
            block = wrap(block);
            block.where.add(expressions.render(filter.condition(), block.inputCols));
        }
        return block;
    }

    private Block visitAggregate(Aggregate aggregate) {
        Block block = visit(aggregate.child());
        Map<Long, String> scope;
        if (block.isPlain()) {
            scope = block.inputCols;
        } else if (block.setOperation == null && block.select != null && block.groupBy == null && !block.hasWindow
                && !block.distinct && block.orderBy == null && block.limit == null) {
            scope = outputExprs(block);
        } else {
            block = wrap(block);
            scope = block.inputCols;
        }
        List<String> groupBy = new ArrayList<>();
        for (Expression expression : Scala.list(aggregate.groupingExpressions())) {
            groupBy.add(expressions.render(expression, scope));
        }
        block.select = renderNamed(Scala.list(aggregate.aggregateExpressions()), scope);
        block.groupBy = groupBy;
        block.output = Scala.list(aggregate.output());
        return block;
    }

    private Block visitWindow(Window window) {
        Block block = visit(window.child());
        if (block.setOperation != null || block.hasWindow || block.distinct || block.orderBy != null
                || block.limit != null) {
            block = wrap(block);
        }
        Map<Long, String> scope = block.select == null ? block.inputCols : outputExprs(block);
        List<String> select = new ArrayList<>();
        List<Attribute> childOutput = block.output;
        for (Attribute attribute : childOutput) {
            select.add(scope.get(attribute.exprId().id()) + ALIAS_MARKER + attribute.name());
        }
        select.addAll(renderNamed(Scala.list(window.windowExpressions()), scope));
        block.select = select;
        block.hasWindow = true;
        block.output = Scala.list(window.output());
        return block;
    }

    private Block visitSort(Sort sort) {
        Block block = visit(sort.child());
        if (block.setOperation != null || block.limit != null) {
            block = wrap(block);
        }
        Map<Long, String> scope = block.select == null ? block.inputCols : outputExprs(block);
        List<String> orderBy = new ArrayList<>();
        for (SortOrder order : Scala.list(sort.order())) {
            orderBy.add(expressions.render(order.child(), scope) + " " + order.direction().sql() + " "
                    + order.nullOrdering().sql());
        }
        block.orderBy = orderBy;
        return block;
    }

    private Block visitLimit(Expression limitExpr, LogicalPlan child) {
        Block block = visit(child);
        if (block.setOperation != null || block.limit != null) {
            block = wrap(block);
        }
        Object value = limitExpr.eval(null);
        block.limit = ((Number) value).longValue();
        return block;
    }

    private Block visitDistinct(Distinct distinct) {
        Block block = visit(distinct.child());
        if (block.setOperation != null || block.orderBy != null || block.limit != null) {
            block = wrap(block);
        }
        if (block.select == null) {
            block.select = passThrough(block);
        }
        block.distinct = true;
        return block;
    }

    private Block visitJoin(Join join) {
        Block left = visit(join.left());
        if (!left.isBareFrom() || left.from == null) {
            left = wrap(left);
        }
        Block right = visit(join.right());
        if (!right.isBareFrom() || !right.singleRelation) {
            right = wrap(right);
        }
        Map<Long, String> scope = new HashMap<>(left.inputCols);
        scope.putAll(right.inputCols);

        String condition = join.condition().isDefined() ? expressions.render(join.condition().get(), scope) : null;
        String joinType = join.joinType().sql();
        String clause;
        if (joinType.equals("INNER") && condition == null) {
            clause = " CROSS JOIN " + right.from;
        } else if (joinType.equals("CROSS")) {
            clause = " CROSS JOIN " + right.from + (condition == null ? "" : " ON " + condition);
        } else {
            clause = " " + joinType + " JOIN " + right.from + " ON " + (condition == null ? "1 = 1" : condition);
        }

        Block block = new Block();
        block.from = left.from + clause;
        block.inputCols = scope;
        block.output = Scala.list(join.output());
        return block;
    }

    private Block visitGenerate(Generate generate) {
        if (!(generate.generator() instanceof Explode explode)
                || !(explode.child().dataType() instanceof ArrayType)) {
            throw new SparkDialectException("Only explode(array) is supported in LATERAL VIEW / generators, got "
                    + ((Expression) generate.generator()).prettyName());
        }
        Block block = visit(generate.child());
        if (!block.isPlain() || block.from == null) {
            block = wrap(block);
        }
        String array = expressions.render(explode.child(), block.inputCols);
        Attribute element = Scala.list(generate.generatorOutput()).get(0);
        String alias = nextAlias();
        String unnest = "unnest(" + array + ") " + alias + "(" + Sql.quote(element.name()) + ")";
        block.from = block.from + (generate.outer() ? " LEFT JOIN " + unnest + " ON TRUE" : ", " + unnest);
        block.singleRelation = false;
        block.inputCols.put(element.exprId().id(), alias + "." + Sql.quote(element.name()));
        block.output = Scala.list(generate.output());
        return block;
    }

    private Block visitSetOperation(List<LogicalPlan> children, String operator, LogicalPlan plan) {
        StringJoiner sql = new StringJoiner(" " + operator + " ");
        List<String> names = null;
        for (LogicalPlan child : children) {
            Block block = visit(child);
            List<String> childNames = names == null ? uniqueNames(block.output) : names;
            if (names == null) {
                names = childNames;
            }
            sql.add("SELECT * FROM (" + render(block, childNames) + ") " + nextAlias());
        }
        return setOperationBlock(sql.toString(), names, Scala.list(plan.output()));
    }

    private Block setOperationBlock(String sql, List<String> names, List<Attribute> output) {
        Block block = new Block();
        block.setOperation = sql;
        block.setOperationNames = names;
        block.output = output;
        return block;
    }

    /** Turns a block into a derived table and returns a fresh block selecting from it. */
    private Block wrap(Block inner) {
        String alias = nextAlias();
        List<String> names = uniqueNames(inner.output);
        Block block = new Block();
        block.from = "(" + render(inner, names) + ") " + alias;
        block.singleRelation = true;
        block.output = inner.output;
        for (int i = 0; i < inner.output.size(); i++) {
            block.inputCols.put(inner.output.get(i).exprId().id(), alias + "." + Sql.quote(names.get(i)));
        }
        return block;
    }

    /** Renders a block with the given output column names. */
    String render(Block block, List<String> names) {
        if (block.setOperation != null) {
            if (names.equals(block.setOperationNames)) {
                return block.setOperation;
            }
            String alias = nextAlias();
            StringJoiner items = new StringJoiner(", ");
            for (int i = 0; i < names.size(); i++) {
                items.add(alias + "." + Sql.quote(block.setOperationNames.get(i)) + " AS " + Sql.quote(names.get(i)));
            }
            return "SELECT " + items + " FROM (" + block.setOperation + ") " + alias;
        }

        List<String> exprs = block.select != null ? block.select : passThrough(block);
        StringJoiner items = new StringJoiner(", ");
        for (int i = 0; i < exprs.size(); i++) {
            items.add(stripAlias(exprs.get(i)) + " AS " + Sql.quote(names.get(i)));
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        if (block.distinct) {
            sql.append("DISTINCT ");
        }
        sql.append(items.length() == 0 ? "1" : items.toString());
        if (block.from != null) {
            sql.append(" FROM ").append(block.from);
        }
        if (!block.where.isEmpty()) {
            sql.append(" WHERE ").append(conjunction(block.where));
        }
        if (block.groupBy != null && !block.groupBy.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", block.groupBy));
        }
        if (!block.having.isEmpty()) {
            sql.append(" HAVING ").append(conjunction(block.having));
        }
        if (block.orderBy != null) {
            sql.append(" ORDER BY ").append(String.join(", ", block.orderBy));
        }
        if (block.limit != null) {
            sql.append(" LIMIT ").append(block.limit);
        }
        return sql.toString();
    }

    // Select items are kept as expr + ALIAS_MARKER + name, so the expression can be inlined by outputExprs and
    // the name replaced at render time. The marker contains NUL, which never occurs in rendered SQL.
    private static final String ALIAS_MARKER = "\u0000AS\u0000";

    private List<String> renderNamed(List<? extends NamedExpression> items, Map<Long, String> scope) {
        List<String> rendered = new ArrayList<>();
        for (NamedExpression item : items) {
            Expression expression = item instanceof Alias alias ? alias.child() : (Expression) item;
            rendered.add(expressions.render(expression, scope) + ALIAS_MARKER + item.name());
        }
        return rendered;
    }

    private List<String> passThrough(Block block) {
        List<String> items = new ArrayList<>();
        for (Attribute attribute : block.output) {
            String sql = block.inputCols.get(attribute.exprId().id());
            if (sql == null) {
                throw new SparkDialectException("Lost track of column " + attribute.name());
            }
            items.add(sql + ALIAS_MARKER + attribute.name());
        }
        return items;
    }

    /** Expression id -> SQL of each output column, in terms of the block's inputs. */
    private Map<Long, String> outputExprs(Block block) {
        if (block.select == null) {
            return block.inputCols;
        }
        Map<Long, String> exprs = new HashMap<>();
        for (int i = 0; i < block.output.size(); i++) {
            exprs.put(block.output.get(i).exprId().id(), "(" + stripAlias(block.select.get(i)) + ")");
        }
        return exprs;
    }

    private static String stripAlias(String item) {
        return item.substring(0, item.lastIndexOf(ALIAS_MARKER));
    }

    private static String conjunction(List<String> predicates) {
        if (predicates.size() == 1) {
            return predicates.get(0);
        }
        StringJoiner joiner = new StringJoiner(" AND ");
        for (String predicate : predicates) {
            joiner.add("(" + predicate + ")");
        }
        return joiner.toString();
    }

    static List<String> uniqueNames(List<Attribute> output) {
        List<String> names = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Attribute attribute : output) {
            String name = attribute.name();
            if (!seen.add(name.toLowerCase(Locale.ROOT))) {
                name = name + "_" + attribute.exprId().id();
                seen.add(name.toLowerCase(Locale.ROOT));
            }
            names.add(name);
        }
        return names;
    }

    String nextAlias() {
        return "t" + (aliasCounter++);
    }
}
