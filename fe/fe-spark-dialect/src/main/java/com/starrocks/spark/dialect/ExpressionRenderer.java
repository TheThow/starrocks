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

import org.apache.spark.sql.catalyst.expressions.Abs;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.AddMonths;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.ArrayContains;
import org.apache.spark.sql.catalyst.expressions.ArrayDistinct;
import org.apache.spark.sql.catalyst.expressions.ArrayFilter;
import org.apache.spark.sql.catalyst.expressions.ArrayJoin;
import org.apache.spark.sql.catalyst.expressions.ArrayMax;
import org.apache.spark.sql.catalyst.expressions.ArrayMin;
import org.apache.spark.sql.catalyst.expressions.ArrayPosition;
import org.apache.spark.sql.catalyst.expressions.ArrayTransform;
import org.apache.spark.sql.catalyst.expressions.Ascii;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryArithmetic;
import org.apache.spark.sql.catalyst.expressions.BinaryComparison;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Ceil;
import org.apache.spark.sql.catalyst.expressions.CheckOverflowInTableInsert;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.Concat;
import org.apache.spark.sql.catalyst.expressions.ConcatWs;
import org.apache.spark.sql.catalyst.expressions.Contains;
import org.apache.spark.sql.catalyst.expressions.Crc32;
import org.apache.spark.sql.catalyst.expressions.CreateArray;
import org.apache.spark.sql.catalyst.expressions.CreateMap;
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.CumeDist;
import org.apache.spark.sql.catalyst.expressions.CurrentDate;
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp;
import org.apache.spark.sql.catalyst.expressions.DateAdd;
import org.apache.spark.sql.catalyst.expressions.DateAddInterval;
import org.apache.spark.sql.catalyst.expressions.DateAddYMInterval;
import org.apache.spark.sql.catalyst.expressions.DateDiff;
import org.apache.spark.sql.catalyst.expressions.DateFormatClass;
import org.apache.spark.sql.catalyst.expressions.DateSub;
import org.apache.spark.sql.catalyst.expressions.DayOfMonth;
import org.apache.spark.sql.catalyst.expressions.DayOfWeek;
import org.apache.spark.sql.catalyst.expressions.DayOfYear;
import org.apache.spark.sql.catalyst.expressions.DenseRank;
import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.ElementAt;
import org.apache.spark.sql.catalyst.expressions.EndsWith;
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Exists;
import org.apache.spark.sql.catalyst.expressions.Exp;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Floor;
import org.apache.spark.sql.catalyst.expressions.FromUnixTime;
import org.apache.spark.sql.catalyst.expressions.GetArrayItem;
import org.apache.spark.sql.catalyst.expressions.GetJsonObject;
import org.apache.spark.sql.catalyst.expressions.GetMapValue;
import org.apache.spark.sql.catalyst.expressions.GetStructField;
import org.apache.spark.sql.catalyst.expressions.GetTimestamp;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.Greatest;
import org.apache.spark.sql.catalyst.expressions.Hour;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.catalyst.expressions.InSet;
import org.apache.spark.sql.catalyst.expressions.InSubquery;
import org.apache.spark.sql.catalyst.expressions.InitCap;
import org.apache.spark.sql.catalyst.expressions.IntegralDivide;
import org.apache.spark.sql.catalyst.expressions.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.IsNull;
import org.apache.spark.sql.catalyst.expressions.KnownNotNull;
import org.apache.spark.sql.catalyst.expressions.KnownNullable;
import org.apache.spark.sql.catalyst.expressions.Lag;
import org.apache.spark.sql.catalyst.expressions.LambdaFunction;
import org.apache.spark.sql.catalyst.expressions.LastDay;
import org.apache.spark.sql.catalyst.expressions.Lead;
import org.apache.spark.sql.catalyst.expressions.Least;
import org.apache.spark.sql.catalyst.expressions.Length;
import org.apache.spark.sql.catalyst.expressions.LessThan;
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.Like;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Log;
import org.apache.spark.sql.catalyst.expressions.Log10;
import org.apache.spark.sql.catalyst.expressions.Log2;
import org.apache.spark.sql.catalyst.expressions.Logarithm;
import org.apache.spark.sql.catalyst.expressions.Lower;
import org.apache.spark.sql.catalyst.expressions.MapKeys;
import org.apache.spark.sql.catalyst.expressions.MapValues;
import org.apache.spark.sql.catalyst.expressions.Md5;
import org.apache.spark.sql.catalyst.expressions.Minute;
import org.apache.spark.sql.catalyst.expressions.Month;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.NTile;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.NamedLambdaVariable;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.expressions.Now;
import org.apache.spark.sql.catalyst.expressions.Or;
import org.apache.spark.sql.catalyst.expressions.OuterReference;
import org.apache.spark.sql.catalyst.expressions.PercentRank;
import org.apache.spark.sql.catalyst.expressions.Pmod;
import org.apache.spark.sql.catalyst.expressions.Pow;
import org.apache.spark.sql.catalyst.expressions.Quarter;
import org.apache.spark.sql.catalyst.expressions.RLike;
import org.apache.spark.sql.catalyst.expressions.Rand;
import org.apache.spark.sql.catalyst.expressions.Rank;
import org.apache.spark.sql.catalyst.expressions.RegExpExtract;
import org.apache.spark.sql.catalyst.expressions.RegExpReplace;
import org.apache.spark.sql.catalyst.expressions.Remainder;
import org.apache.spark.sql.catalyst.expressions.Reverse;
import org.apache.spark.sql.catalyst.expressions.Round;
import org.apache.spark.sql.catalyst.expressions.RowNumber;
import org.apache.spark.sql.catalyst.expressions.RuntimeReplaceable;
import org.apache.spark.sql.catalyst.expressions.RuntimeReplaceableAggregate;
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery;
import org.apache.spark.sql.catalyst.expressions.Second;
import org.apache.spark.sql.catalyst.expressions.Sha2;
import org.apache.spark.sql.catalyst.expressions.Size;
import org.apache.spark.sql.catalyst.expressions.Slice;
import org.apache.spark.sql.catalyst.expressions.SortArray;
import org.apache.spark.sql.catalyst.expressions.SpecialFrameBoundary;
import org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame;
import org.apache.spark.sql.catalyst.expressions.Sqrt;
import org.apache.spark.sql.catalyst.expressions.StartsWith;
import org.apache.spark.sql.catalyst.expressions.StringInstr;
import org.apache.spark.sql.catalyst.expressions.StringLPad;
import org.apache.spark.sql.catalyst.expressions.StringLocate;
import org.apache.spark.sql.catalyst.expressions.StringRPad;
import org.apache.spark.sql.catalyst.expressions.StringRepeat;
import org.apache.spark.sql.catalyst.expressions.StringReplace;
import org.apache.spark.sql.catalyst.expressions.StringSplit;
import org.apache.spark.sql.catalyst.expressions.StringTrim;
import org.apache.spark.sql.catalyst.expressions.StringTrimLeft;
import org.apache.spark.sql.catalyst.expressions.StringTrimRight;
import org.apache.spark.sql.catalyst.expressions.Substring;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.expressions.TimeAdd;
import org.apache.spark.sql.catalyst.expressions.TimestampAddYMInterval;
import org.apache.spark.sql.catalyst.expressions.ToUnixTimestamp;
import org.apache.spark.sql.catalyst.expressions.TruncDate;
import org.apache.spark.sql.catalyst.expressions.TruncTimestamp;
import org.apache.spark.sql.catalyst.expressions.UnaryMinus;
import org.apache.spark.sql.catalyst.expressions.UnaryPositive;
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp;
import org.apache.spark.sql.catalyst.expressions.Upper;
import org.apache.spark.sql.catalyst.expressions.WeekOfYear;
import org.apache.spark.sql.catalyst.expressions.WindowExpression;
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition;
import org.apache.spark.sql.catalyst.expressions.Year;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction;
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile;
import org.apache.spark.sql.catalyst.expressions.aggregate.Average;
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList;
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet;
import org.apache.spark.sql.catalyst.expressions.aggregate.Corr;
import org.apache.spark.sql.catalyst.expressions.aggregate.Count;
import org.apache.spark.sql.catalyst.expressions.aggregate.CovPopulation;
import org.apache.spark.sql.catalyst.expressions.aggregate.CovSample;
import org.apache.spark.sql.catalyst.expressions.aggregate.First;
import org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus;
import org.apache.spark.sql.catalyst.expressions.aggregate.Last;
import org.apache.spark.sql.catalyst.expressions.aggregate.Max;
import org.apache.spark.sql.catalyst.expressions.aggregate.MaxBy;
import org.apache.spark.sql.catalyst.expressions.aggregate.Min;
import org.apache.spark.sql.catalyst.expressions.aggregate.MinBy;
import org.apache.spark.sql.catalyst.expressions.aggregate.StddevPop;
import org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp;
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum;
import org.apache.spark.sql.catalyst.expressions.aggregate.VariancePop;
import org.apache.spark.sql.catalyst.expressions.aggregate.VarianceSamp;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.unsafe.types.CalendarInterval;
import scala.Tuple2;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Renders analyzed Catalyst expressions as StarRocks SQL.
 *
 * <p>The analyzer has already applied Spark's type coercion, so every implicit cast is an explicit {@link Cast}
 * here. What is left is keeping Spark's result types and Spark's function semantics:
 * <ul>
 *   <li>Arithmetic, numeric functions and aggregates are cast to the type Spark computed, because StarRocks
 *       widens differently (e.g. INT + INT is BIGINT in StarRocks, INT in Spark).</li>
 *   <li>Functions are mapped explicitly. A Spark function with no faithful StarRocks equivalent fails the
 *       translation instead of being passed through by name.</li>
 * </ul>
 */
final class ExpressionRenderer {
    record Context(ZoneId sessionZone) {
    }

    private static final DateTimeFormatter DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    // Spark expressions whose StarRocks function takes the same arguments with the same meaning.
    private static final Map<Class<?>, String> SAME_ARGUMENTS = new HashMap<>();
    // Aggregate functions with the same arguments and semantics.
    private static final Map<Class<?>, String> AGGREGATES = new HashMap<>();

    static {
        SAME_ARGUMENTS.put(Upper.class, "upper");
        SAME_ARGUMENTS.put(Lower.class, "lower");
        SAME_ARGUMENTS.put(StringRepeat.class, "repeat");
        SAME_ARGUMENTS.put(StringReplace.class, "replace");
        SAME_ARGUMENTS.put(StringInstr.class, "instr");
        SAME_ARGUMENTS.put(StringLocate.class, "locate");
        SAME_ARGUMENTS.put(StringLPad.class, "lpad");
        SAME_ARGUMENTS.put(StringRPad.class, "rpad");
        SAME_ARGUMENTS.put(InitCap.class, "initcap");
        SAME_ARGUMENTS.put(Ascii.class, "ascii");
        SAME_ARGUMENTS.put(Md5.class, "md5");
        SAME_ARGUMENTS.put(Sha2.class, "sha2");
        SAME_ARGUMENTS.put(Crc32.class, "crc32");
        SAME_ARGUMENTS.put(ConcatWs.class, "concat_ws");
        SAME_ARGUMENTS.put(Reverse.class, "reverse");
        SAME_ARGUMENTS.put(StartsWith.class, "starts_with");
        SAME_ARGUMENTS.put(EndsWith.class, "ends_with");
        SAME_ARGUMENTS.put(RegExpExtract.class, "regexp_extract");
        SAME_ARGUMENTS.put(GetJsonObject.class, "get_json_string");
        SAME_ARGUMENTS.put(Abs.class, "abs");
        SAME_ARGUMENTS.put(Sqrt.class, "sqrt");
        SAME_ARGUMENTS.put(Exp.class, "exp");
        SAME_ARGUMENTS.put(Log.class, "ln");
        SAME_ARGUMENTS.put(Log10.class, "log10");
        SAME_ARGUMENTS.put(Log2.class, "log2");
        SAME_ARGUMENTS.put(Logarithm.class, "log");
        SAME_ARGUMENTS.put(Pow.class, "pow");
        SAME_ARGUMENTS.put(Ceil.class, "ceil");
        SAME_ARGUMENTS.put(Floor.class, "floor");
        SAME_ARGUMENTS.put(Round.class, "round");
        SAME_ARGUMENTS.put(Rand.class, "rand");
        SAME_ARGUMENTS.put(Year.class, "year");
        SAME_ARGUMENTS.put(Month.class, "month");
        SAME_ARGUMENTS.put(DayOfMonth.class, "dayofmonth");
        SAME_ARGUMENTS.put(Hour.class, "hour");
        SAME_ARGUMENTS.put(Minute.class, "minute");
        SAME_ARGUMENTS.put(Second.class, "second");
        SAME_ARGUMENTS.put(Quarter.class, "quarter");
        // Both count Sunday as 1.
        SAME_ARGUMENTS.put(DayOfWeek.class, "dayofweek");
        SAME_ARGUMENTS.put(DayOfYear.class, "dayofyear");
        SAME_ARGUMENTS.put(WeekOfYear.class, "weekofyear");
        SAME_ARGUMENTS.put(LastDay.class, "last_day");
        SAME_ARGUMENTS.put(DateDiff.class, "datediff");
        SAME_ARGUMENTS.put(DateAdd.class, "date_add");
        SAME_ARGUMENTS.put(DateSub.class, "date_sub");
        SAME_ARGUMENTS.put(AddMonths.class, "add_months");
        SAME_ARGUMENTS.put(ArrayContains.class, "array_contains");
        SAME_ARGUMENTS.put(ArrayDistinct.class, "array_distinct");
        SAME_ARGUMENTS.put(ArrayMax.class, "array_max");
        SAME_ARGUMENTS.put(ArrayMin.class, "array_min");
        SAME_ARGUMENTS.put(ArrayPosition.class, "array_position");
        SAME_ARGUMENTS.put(ArrayJoin.class, "array_join");
        // Both are 1-based with a length argument.
        SAME_ARGUMENTS.put(Slice.class, "array_slice");
        SAME_ARGUMENTS.put(MapKeys.class, "map_keys");
        SAME_ARGUMENTS.put(MapValues.class, "map_values");

        AGGREGATES.put(Sum.class, "sum");
        AGGREGATES.put(Average.class, "avg");
        AGGREGATES.put(Min.class, "min");
        AGGREGATES.put(Max.class, "max");
        // By class, not by name: Spark's stddev()/variance() are the sample versions, StarRocks' are population.
        AGGREGATES.put(StddevSamp.class, "stddev_samp");
        AGGREGATES.put(StddevPop.class, "stddev_pop");
        AGGREGATES.put(VarianceSamp.class, "var_samp");
        AGGREGATES.put(VariancePop.class, "var_pop");
        AGGREGATES.put(Corr.class, "corr");
        AGGREGATES.put(CovSample.class, "covar_samp");
        AGGREGATES.put(CovPopulation.class, "covar_pop");
        AGGREGATES.put(MaxBy.class, "max_by");
        AGGREGATES.put(MinBy.class, "min_by");
        AGGREGATES.put(HyperLogLogPlusPlus.class, "approx_count_distinct");
    }

    private final PlanRenderer plans;
    private final Context context;
    private final Map<Long, String> lambdaVariables = new HashMap<>();

    ExpressionRenderer(PlanRenderer plans, Context context) {
        this.plans = plans;
        this.context = context;
    }

    String render(Expression e, Map<Long, String> scope) {
        if (e instanceof AttributeReference attribute) {
            String sql = scope.get(attribute.exprId().id());
            if (sql == null) {
                throw new SparkDialectException("Cannot resolve column " + attribute.sql() + " in its query block");
            }
            return sql;
        } else if (e instanceof NamedLambdaVariable variable) {
            return lambdaVariables.get(variable.exprId().id());
        } else if (e instanceof OuterReference outer) {
            String sql = plans.lookupOuter(outer.e().exprId().id());
            if (sql == null) {
                throw new SparkDialectException("Cannot resolve outer reference " + outer.sql());
            }
            return sql;
        } else if (e instanceof Alias alias) {
            return render(alias.child(), scope);
        } else if (e instanceof KnownNotNull || e instanceof KnownNullable) {
            return render(e.children().head(), scope);
        } else if (e instanceof CheckOverflowInTableInsert check) {
            // ANSI store assignment: Spark fails the INSERT on overflow, StarRocks' CAST yields NULL.
            return render(check.child(), scope);
        } else if (e instanceof Literal literal) {
            return literal(literal);
        } else if (e instanceof Cast cast) {
            // ANSI casts fail on bad input in Spark; StarRocks returns NULL instead.
            return Sql.cast(render(cast.child(), scope), StarRocksTypes.of(cast.dataType()));
        } else if (e instanceof BinaryArithmetic arithmetic) {
            return arithmetic(arithmetic, scope);
        } else if (e instanceof UnaryMinus minus) {
            return castResult("(- " + render(minus.child(), scope) + ")", e.dataType());
        } else if (e instanceof UnaryPositive positive) {
            return render(positive.child(), scope);
        } else if (e instanceof BinaryComparison comparison) {
            return "(" + render(comparison.left(), scope) + " " + comparisonOperator(comparison) + " "
                    + render(comparison.right(), scope) + ")";
        } else if (e instanceof And and) {
            return "(" + render(and.left(), scope) + " AND " + render(and.right(), scope) + ")";
        } else if (e instanceof Or or) {
            return "(" + render(or.left(), scope) + " OR " + render(or.right(), scope) + ")";
        } else if (e instanceof Not not) {
            return "(NOT " + render(not.child(), scope) + ")";
        } else if (e instanceof IsNull isNull) {
            return "(" + render(isNull.child(), scope) + " IS NULL)";
        } else if (e instanceof IsNotNull isNotNull) {
            return "(" + render(isNotNull.child(), scope) + " IS NOT NULL)";
        } else if (e instanceof In in) {
            return "(" + render(in.value(), scope) + " IN (" + join(Scala.list(in.list()), scope) + "))";
        } else if (e instanceof InSet inSet) {
            StringJoiner values = new StringJoiner(", ");
            for (Object value : Scala.list(inSet.hset().toSeq())) {
                values.add(literal(new Literal(value, inSet.child().dataType())));
            }
            return "(" + render(inSet.child(), scope) + " IN (" + values + "))";
        } else if (e instanceof InSubquery inSubquery) {
            List<Expression> values = Scala.list(inSubquery.values());
            if (values.size() != 1) {
                throw new SparkDialectException("Multi-column IN subqueries are not supported");
            }
            return "(" + render(values.get(0), scope) + " IN ("
                    + plans.renderSubquery(inSubquery.query().plan(), scope) + "))";
        } else if (e instanceof Exists exists) {
            return "(EXISTS (" + plans.renderSubquery(exists.plan(), scope) + "))";
        } else if (e instanceof ScalarSubquery subquery) {
            return "(" + plans.renderSubquery(subquery.plan(), scope) + ")";
        } else if (e instanceof CaseWhen caseWhen) {
            StringBuilder sql = new StringBuilder("CASE");
            for (Tuple2<Expression, Expression> branch : Scala.list(caseWhen.branches())) {
                sql.append(" WHEN ").append(render(branch._1(), scope))
                        .append(" THEN ").append(render(branch._2(), scope));
            }
            if (caseWhen.elseValue().isDefined()) {
                sql.append(" ELSE ").append(render(caseWhen.elseValue().get(), scope));
            }
            return sql.append(" END").toString();
        } else if (e instanceof If ifExpr) {
            return "CASE WHEN " + render(ifExpr.predicate(), scope) + " THEN " + render(ifExpr.trueValue(), scope)
                    + " ELSE " + render(ifExpr.falseValue(), scope) + " END";
        } else if (e instanceof Coalesce coalesce) {
            return "coalesce(" + join(Scala.list(coalesce.children()), scope) + ")";
        } else if (e instanceof Greatest || e instanceof Least) {
            return nullSkippingExtreme(e, scope);
        } else if (e instanceof Like like) {
            if (like.escapeChar() != '\\') {
                throw new SparkDialectException("LIKE ... ESCAPE is only supported with the default escape '\\'");
            }
            return "(" + render(like.left(), scope) + " LIKE " + render(like.right(), scope) + ")";
        } else if (e instanceof RLike rlike) {
            return "regexp(" + render(rlike.left(), scope) + ", " + render(rlike.right(), scope) + ")";
        } else if (e instanceof Contains contains) {
            return "(instr(" + render(contains.left(), scope) + ", " + render(contains.right(), scope) + ") > 0)";
        } else if (e instanceof AggregateExpression aggregate) {
            return aggregate(aggregate, scope, null);
        } else if (e instanceof WindowExpression window) {
            return window(window, scope);
        } else if (e instanceof GetStructField field) {
            return render(field.child(), scope) + "." + Sql.quote(field.extractFieldName());
        } else if (e instanceof GetArrayItem item) {
            // Spark is 0-based, StarRocks 1-based. Both return NULL out of bounds outside ANSI mode.
            return render(item.child(), scope) + "[" + plusOne(item.ordinal(), scope) + "]";
        } else if (e instanceof GetMapValue mapValue) {
            return render(mapValue.child(), scope) + "[" + render(mapValue.key(), scope) + "]";
        } else if (e instanceof ElementAt elementAt) {
            return elementAt(elementAt, scope);
        } else if (e instanceof Size size) {
            return size(size, scope);
        } else if (e instanceof CreateArray array) {
            return "[" + join(Scala.list(array.children()), scope) + "]";
        } else if (e instanceof CreateNamedStruct struct) {
            return "named_struct(" + join(Scala.list(struct.children()), scope) + ")";
        } else if (e instanceof CreateMap map) {
            List<Expression> children = Scala.list(map.children());
            List<Expression> keys = new ArrayList<>();
            List<Expression> values = new ArrayList<>();
            for (int i = 0; i < children.size(); i += 2) {
                keys.add(children.get(i));
                values.add(children.get(i + 1));
            }
            return "map_from_arrays([" + join(keys, scope) + "], [" + join(values, scope) + "])";
        } else if (e instanceof Concat concat) {
            String function = concat.dataType() instanceof ArrayType ? "array_concat" : "concat";
            return function + "(" + join(Scala.list(concat.children()), scope) + ")";
        } else if (e instanceof Length length) {
            // Spark counts characters; StarRocks length() counts bytes.
            String function = length.child().dataType() instanceof BinaryType ? "length" : "char_length";
            return castResult(function + "(" + render(length.child(), scope) + ")", e.dataType());
        } else if (e instanceof Substring substring) {
            return substring(substring, scope);
        } else if (e instanceof StringTrim || e instanceof StringTrimLeft || e instanceof StringTrimRight) {
            return trim(e, scope);
        } else if (e instanceof StringSplit split) {
            return split(split, scope);
        } else if (e instanceof RegExpReplace replace) {
            return regexpReplace(replace, scope);
        } else if (e instanceof SortArray sort) {
            return sortArray(sort, scope);
        } else if (e instanceof ArrayTransform transform) {
            return "array_map(" + lambda(transform.function(), scope) + ", " + render(transform.argument(), scope)
                    + ")";
        } else if (e instanceof ArrayFilter filter) {
            return "array_filter(" + lambda(filter.function(), scope) + ", " + render(filter.argument(), scope)
                    + ")";
        } else if (e instanceof DateFormatClass format) {
            // Spark datetime patterns are java.time patterns; jodatime_format accepts the same common letters.
            return "jodatime_format(" + render(format.left(), scope) + ", " + render(format.right(), scope) + ")";
        } else if (e instanceof GetTimestamp getTimestamp) {
            return Sql.cast("str_to_jodatime(" + render(getTimestamp.left(), scope) + ", "
                    + render(getTimestamp.right(), scope) + ")", StarRocksTypes.of(getTimestamp.dataType()));
        } else if (e instanceof UnixTimestamp || e instanceof ToUnixTimestamp) {
            return unixTimestamp(e, scope);
        } else if (e instanceof FromUnixTime fromUnixTime) {
            return "jodatime_format(CAST(from_unixtime(" + render(fromUnixTime.sec(), scope) + ") AS DATETIME), "
                    + render(fromUnixTime.format(), scope) + ")";
        } else if (e instanceof CurrentDate) {
            return "current_date()";
        } else if (e instanceof CurrentTimestamp || e instanceof Now) {
            return "now()";
        } else if (e instanceof TruncTimestamp trunc) {
            return Sql.cast("date_trunc(lower(" + render(trunc.format(), scope) + "), "
                    + render(trunc.timestamp(), scope) + ")", "DATETIME");
        } else if (e instanceof TruncDate trunc) {
            return Sql.cast("date_trunc(lower(" + render(trunc.format(), scope) + "), " + render(trunc.date(), scope)
                    + ")", "DATE");
        } else if (e instanceof TimeAdd timeAdd) {
            return castResult("microseconds_add(" + render(timeAdd.start(), scope) + ", "
                    + intervalMicros(timeAdd.interval()) + ")", e.dataType());
        } else if (e instanceof DateAddInterval dateAdd) {
            return Sql.cast("date_add(" + render(dateAdd.start(), scope) + ", " + intervalDays(dateAdd.interval())
                    + ")", "DATE");
        } else if (e instanceof DateAddYMInterval || e instanceof TimestampAddYMInterval) {
            Expression start = e.children().apply(0);
            Expression interval = e.children().apply(1);
            return castResult("add_months(" + render(start, scope) + ", " + render(interval, scope) + ")",
                    e.dataType());
        } else if (e instanceof StaticInvoke invoke) {
            return staticInvoke(invoke, scope);
        } else if (SAME_ARGUMENTS.containsKey(e.getClass())) {
            return castResult(SAME_ARGUMENTS.get(e.getClass()) + "(" + join(Scala.list(e.children()), scope) + ")",
                    e.dataType());
        } else if (e instanceof RuntimeReplaceable replaceable) {
            // nvl, ifnull, nullif, left, to_date, ilike, ... are defined by Spark in terms of other expressions.
            return render(replaceable.replacement(), scope);
        }
        throw new SparkDialectException("Unsupported Spark expression: " + e.prettyName() + " (" + e.sql() + ")");
    }

    String literal(Literal literal) {
        Object value = literal.value();
        DataType type = literal.dataType();
        if (value == null) {
            return type instanceof NullType ? "NULL" : Sql.cast("NULL", StarRocksTypes.of(type));
        } else if (type instanceof BooleanType) {
            return (Boolean) value ? "TRUE" : "FALSE";
        } else if (type instanceof IntegerType) {
            return value.toString();
        } else if (type instanceof FloatType || type instanceof DoubleType) {
            double d = ((Number) value).doubleValue();
            if (!Double.isFinite(d)) {
                throw new SparkDialectException("NaN and infinite literals are not supported");
            }
            return Sql.cast(Double.toString(d), StarRocksTypes.of(type));
        } else if (type instanceof DecimalType) {
            return Sql.cast(((Decimal) value).toJavaBigDecimal().toPlainString(), StarRocksTypes.of(type));
        } else if (type instanceof NumericType) {
            return Sql.cast(value.toString(), StarRocksTypes.of(type));
        } else if (type instanceof StringType || type instanceof VarcharType || type instanceof CharType) {
            return Sql.string(value.toString());
        } else if (type instanceof DateType) {
            return Sql.cast(Sql.string(LocalDate.ofEpochDay((Integer) value).toString()), "DATE");
        } else if (type instanceof TimestampType) {
            LocalDateTime local = LocalDateTime.ofInstant(microsToInstant((Long) value), context.sessionZone());
            return Sql.cast(Sql.string(DATETIME.format(local)), "DATETIME");
        } else if (type instanceof TimestampNTZType) {
            LocalDateTime local = LocalDateTime.ofInstant(microsToInstant((Long) value), ZoneOffset.UTC);
            return Sql.cast(Sql.string(DATETIME.format(local)), "DATETIME");
        }
        throw new SparkDialectException("Unsupported literal of type " + type.simpleString());
    }

    private String arithmetic(BinaryArithmetic arithmetic, Map<Long, String> scope) {
        String left = render(arithmetic.left(), scope);
        String right = render(arithmetic.right(), scope);
        String sql;
        if (arithmetic instanceof Add) {
            sql = "(" + left + " + " + right + ")";
        } else if (arithmetic instanceof Subtract) {
            sql = "(" + left + " - " + right + ")";
        } else if (arithmetic instanceof Multiply) {
            sql = "(" + left + " * " + right + ")";
        } else if (arithmetic instanceof Divide) {
            // Spark has already cast integral operands to DOUBLE or DECIMAL; x / 0 is NULL in both engines.
            sql = "(" + left + " / " + right + ")";
        } else if (arithmetic instanceof IntegralDivide) {
            sql = "(" + left + " DIV " + right + ")";
        } else if (arithmetic instanceof Remainder) {
            sql = "(" + left + " % " + right + ")";
        } else if (arithmetic instanceof Pmod) {
            sql = "pmod(" + left + ", " + right + ")";
        } else {
            throw new SparkDialectException("Unsupported arithmetic: " + arithmetic.prettyName());
        }
        if (!(arithmetic.dataType() instanceof NumericType)) {
            throw new SparkDialectException("Interval arithmetic is not supported: " + arithmetic.sql());
        }
        return castResult(sql, arithmetic.dataType());
    }

    private static String comparisonOperator(BinaryComparison comparison) {
        if (comparison instanceof EqualTo) {
            return "=";
        } else if (comparison instanceof EqualNullSafe) {
            return "<=>";
        } else if (comparison instanceof LessThan) {
            return "<";
        } else if (comparison instanceof LessThanOrEqual) {
            return "<=";
        } else if (comparison instanceof GreaterThan) {
            return ">";
        } else if (comparison instanceof GreaterThanOrEqual) {
            return ">=";
        }
        throw new SparkDialectException("Unsupported comparison: " + comparison.prettyName());
    }

    /**
     * Renders an aggregate call. In a window, First/Last become first_value/last_value; elsewhere they have no
     * ordering in Spark either, so any_value is equivalent.
     */
    private String aggregate(AggregateExpression aggregate, Map<Long, String> scope, String over) {
        AggregateFunction function = aggregate.aggregateFunction();
        if (function instanceof RuntimeReplaceableAggregate replaceable) {
            function = (AggregateFunction) replaceable.replacement();
        }
        boolean distinct = aggregate.isDistinct();
        Expression filter = Scala.orNull(aggregate.filter());
        if (over != null && (distinct || filter != null)) {
            throw new SparkDialectException("DISTINCT and FILTER are not supported in window aggregates");
        }
        List<Expression> children = Scala.list(function.children());

        String call;
        if (function instanceof Count) {
            boolean star = children.size() == 1 && children.get(0) instanceof Literal literal
                    && literal.value() != null;
            if (star && !distinct) {
                call = filter == null ? "count(*)" : "count(CASE WHEN " + render(filter, scope) + " THEN 1 END)";
            } else {
                call = "count(" + (distinct ? "DISTINCT " : "") + arguments(children, filter, scope) + ")";
            }
        } else if (function instanceof CollectList || function instanceof CollectSet) {
            // Spark drops NULLs; StarRocks array_agg keeps them.
            String aggregated = "array_agg(" + (function instanceof CollectSet || distinct ? "DISTINCT " : "")
                    + arguments(children.subList(0, 1), filter, scope) + ")";
            if (over != null) {
                throw new SparkDialectException("collect_list/collect_set over a window is not supported");
            }
            return "array_filter((x) -> x IS NOT NULL, " + aggregated + ")";
        } else if (function instanceof First || function instanceof Last) {
            boolean ignoreNulls = function instanceof First first ? first.ignoreNulls() : ((Last) function).ignoreNulls();
            if (over == null) {
                call = "any_value(" + arguments(children.subList(0, 1), filter, scope) + ")";
            } else if (ignoreNulls) {
                throw new SparkDialectException("first/last with IGNORE NULLS over a window is not supported");
            } else {
                call = (function instanceof First ? "first_value(" : "last_value(")
                        + render(children.get(0), scope) + ")";
            }
        } else if (function instanceof ApproximatePercentile percentile) {
            if (!(percentile.percentageExpression().dataType() instanceof NumericType)) {
                throw new SparkDialectException("percentile_approx with an array of percentages is not supported");
            }
            call = "percentile_approx(" + arguments(List.of(percentile.child()), filter, scope) + ", "
                    + render(percentile.percentageExpression(), scope) + ")";
        } else if (AGGREGATES.containsKey(function.getClass())) {
            List<Expression> args = function instanceof HyperLogLogPlusPlus ? children.subList(0, 1) : children;
            call = AGGREGATES.get(function.getClass()) + "(" + (distinct ? "DISTINCT " : "")
                    + arguments(args, filter, scope) + ")";
        } else {
            throw new SparkDialectException("Unsupported aggregate function: " + function.prettyName());
        }
        if (over != null) {
            call = call + " " + over;
        }
        return castResult(call, aggregate.dataType());
    }

    /** Aggregate arguments; a FILTER clause moves into the first argument, which StarRocks then ignores. */
    private String arguments(List<Expression> args, Expression filter, Map<Long, String> scope) {
        List<String> rendered = new ArrayList<>();
        for (Expression arg : args) {
            rendered.add(render(arg, scope));
        }
        if (filter != null) {
            rendered.set(0, "CASE WHEN " + render(filter, scope) + " THEN " + rendered.get(0) + " END");
        }
        return String.join(", ", rendered);
    }

    private String window(WindowExpression window, Map<Long, String> scope) {
        WindowSpecDefinition spec = window.windowSpec();
        StringBuilder over = new StringBuilder("OVER (");
        List<Expression> partitions = Scala.list(spec.partitionSpec());
        if (!partitions.isEmpty()) {
            over.append("PARTITION BY ").append(join(partitions, scope));
        }
        List<org.apache.spark.sql.catalyst.expressions.SortOrder> orders = Scala.list(spec.orderSpec());
        if (!orders.isEmpty()) {
            StringJoiner orderBy = new StringJoiner(", ");
            for (org.apache.spark.sql.catalyst.expressions.SortOrder order : orders) {
                orderBy.add(render(order.child(), scope) + " " + order.direction().sql() + " "
                        + order.nullOrdering().sql());
            }
            over.append(partitions.isEmpty() ? "" : " ").append("ORDER BY ").append(orderBy);
        }

        Expression function = window.windowFunction();
        // StarRocks only accepts a frame together with ORDER BY; without ORDER BY both engines use the whole
        // partition. Ranking and offset functions take no frame at all.
        if (function instanceof AggregateExpression && !orders.isEmpty()) {
            over.append(" ").append(frame(spec));
        }
        over.append(")");

        if (function instanceof AggregateExpression aggregate) {
            return aggregate(aggregate, scope, over.toString());
        }
        String call;
        if (function instanceof RowNumber) {
            call = "row_number()";
        } else if (function instanceof Rank) {
            call = "rank()";
        } else if (function instanceof DenseRank) {
            call = "dense_rank()";
        } else if (function instanceof PercentRank) {
            call = "percent_rank()";
        } else if (function instanceof CumeDist) {
            call = "cume_dist()";
        } else if (function instanceof NTile ntile) {
            call = "ntile(" + render(ntile.buckets(), scope) + ")";
        } else if (function instanceof Lead lead) {
            // children are (input, offset, default); "default" is a Java keyword, so no accessor.
            call = offsetFunction("lead", lead.input(), lead.offset(), lead.children().apply(2), lead.ignoreNulls(),
                    scope);
        } else if (function instanceof Lag lag) {
            call = offsetFunction("lag", lag.input(), lag.inputOffset(), lag.children().apply(2), lag.ignoreNulls(),
                    scope);
        } else {
            throw new SparkDialectException("Unsupported window function: " + function.prettyName());
        }
        return castResult(call + " " + over, window.dataType());
    }

    private String frame(WindowSpecDefinition spec) {
        if (!(spec.frameSpecification() instanceof SpecifiedWindowFrame frame)) {
            throw new SparkDialectException("Unresolved window frame: " + spec.sql());
        }
        boolean range = frame.frameType().sql().equals("RANGE");
        if (range && !(frame.lower() instanceof SpecialFrameBoundary && frame.upper() instanceof SpecialFrameBoundary)) {
            throw new SparkDialectException("RANGE frames with offsets are not supported by StarRocks");
        }
        return frame.sql();
    }

    private String offsetFunction(String name, Expression input, Expression offset, Expression defaultValue,
                                  boolean ignoreNulls, Map<Long, String> scope) {
        if (ignoreNulls) {
            throw new SparkDialectException(name + " with IGNORE NULLS is not supported");
        }
        String sql = name + "(" + render(input, scope) + ", " + render(offset, scope);
        if (!(defaultValue instanceof Literal literal && literal.value() == null)) {
            sql += ", " + render(defaultValue, scope);
        }
        return sql + ")";
    }

    private String nullSkippingExtreme(Expression e, Map<Long, String> scope) {
        // Spark's greatest/least skip NULLs; StarRocks returns NULL if any argument is NULL. Replacing each
        // argument by coalesce(arg, all args) keeps the non-NULL set and only yields NULL when all are NULL.
        List<Expression> args = Scala.list(e.children());
        String all = join(args, scope);
        StringJoiner coalesced = new StringJoiner(", ");
        for (Expression arg : args) {
            coalesced.add("coalesce(" + render(arg, scope) + ", " + all + ")");
        }
        return castResult((e instanceof Greatest ? "greatest(" : "least(") + coalesced + ")", e.dataType());
    }

    private String elementAt(ElementAt elementAt, Map<Long, String> scope) {
        String collection = render(elementAt.left(), scope);
        if (elementAt.left().dataType() instanceof MapType) {
            return collection + "[" + render(elementAt.right(), scope) + "]";
        }
        // Spark element_at is 1-based and counts negative indexes from the end.
        if (elementAt.right() instanceof Literal literal && literal.value() != null
                && ((Number) literal.value()).intValue() > 0) {
            return collection + "[" + literal.value() + "]";
        }
        throw new SparkDialectException("element_at on arrays is only supported with a positive constant index");
    }

    private String size(Size size, Map<Long, String> scope) {
        String child = render(size.child(), scope);
        String function = size.child().dataType() instanceof MapType ? "map_size" : "array_length";
        String sql = function + "(" + child + ")";
        if (size.legacySizeOfNull()) {
            // spark.sql.legacy.sizeOfNull: size(NULL) is -1.
            sql = "CASE WHEN " + child + " IS NULL THEN -1 ELSE " + sql + " END";
        }
        return castResult(sql, size.dataType());
    }

    private String substring(Substring substring, Map<Long, String> scope) {
        // Spark treats position 0 like 1; StarRocks returns an empty string for 0.
        Expression pos = substring.pos();
        String position;
        if (pos instanceof Literal literal && literal.value() != null) {
            position = ((Number) literal.value()).intValue() == 0 ? "1" : literal.value().toString();
        } else {
            String rendered = render(pos, scope);
            position = "CASE WHEN " + rendered + " = 0 THEN 1 ELSE " + rendered + " END";
        }
        return "substring(" + render(substring.str(), scope) + ", " + position + ", " + render(substring.len(), scope)
                + ")";
    }

    private String trim(Expression e, Map<Long, String> scope) {
        List<Expression> children = Scala.list(e.children());
        if (children.size() != 1) {
            throw new SparkDialectException("trim with explicit trim characters is not supported");
        }
        String function = e instanceof StringTrimLeft ? "ltrim" : e instanceof StringTrimRight ? "rtrim" : "trim";
        return function + "(" + render(children.get(0), scope) + ")";
    }

    private String split(StringSplit split, Map<Long, String> scope) {
        if (!(split.limit() instanceof Literal literal) || ((Number) literal.value()).intValue() > 0) {
            throw new SparkDialectException("split with a positive limit is not supported");
        }
        // Spark's delimiter is a Java regex.
        return "regexp_split(" + render(split.str(), scope) + ", " + render(split.regex(), scope) + ")";
    }

    private String regexpReplace(RegExpReplace replace, Map<Long, String> scope) {
        if (!(replace.pos() instanceof Literal pos) || ((Number) pos.value()).intValue() != 1) {
            throw new SparkDialectException("regexp_replace with a position is not supported");
        }
        String replacement;
        if (replace.rep() instanceof Literal literal && literal.value() != null) {
            // Java uses $1 for groups, StarRocks \1.
            replacement = Sql.string(literal.value().toString().replaceAll("\\$(\\d)", "\\\\$1"));
        } else {
            replacement = render(replace.rep(), scope);
        }
        return "regexp_replace(" + render(replace.subject(), scope) + ", " + render(replace.regexp(), scope) + ", "
                + replacement + ")";
    }

    private String sortArray(SortArray sort, Map<Long, String> scope) {
        if (!(sort.ascendingOrder() instanceof Literal literal) || literal.value() == null) {
            throw new SparkDialectException("sort_array needs a constant sort order");
        }
        String sorted = "array_sort(" + render(sort.base(), scope) + ")";
        return (Boolean) literal.value() ? sorted : "reverse(" + sorted + ")";
    }

    private String lambda(Expression function, Map<Long, String> scope) {
        LambdaFunction lambda = (LambdaFunction) function;
        List<NamedExpression> arguments = Scala.list(lambda.arguments());
        if (arguments.size() != 1) {
            throw new SparkDialectException("Lambdas with an index argument are not supported");
        }
        NamedLambdaVariable variable = (NamedLambdaVariable) arguments.get(0);
        String name = "x_" + variable.exprId().id();
        lambdaVariables.put(variable.exprId().id(), name);
        return "(" + name + ") -> " + render(lambda.function(), scope);
    }

    private String unixTimestamp(Expression e, Map<Long, String> scope) {
        Expression time = e.children().apply(0);
        Expression format = e.children().apply(1);
        String value = time.dataType() instanceof StringType
                ? "str_to_jodatime(" + render(time, scope) + ", " + render(format, scope) + ")"
                : render(time, scope);
        return castResult("unix_timestamp(" + value + ")", e.dataType());
    }

    private String staticInvoke(StaticInvoke invoke, Map<Long, String> scope) {
        List<Expression> args = Scala.list(invoke.arguments());
        switch (invoke.functionName()) {
            case "readSidePadding":
            case "charTypeWriteSideCheck": {
                // CHAR(n) semantics: values are padded with spaces to n characters.
                String value = render(args.get(0), scope);
                String length = render(args.get(1), scope);
                return "CASE WHEN char_length(" + value + ") < " + length + " THEN rpad(" + value + ", " + length
                        + ", ' ') ELSE " + value + " END";
            }
            case "varcharTypeWriteSideCheck":
                return render(args.get(0), scope);
            default:
                throw new SparkDialectException("Unsupported Spark expression: " + invoke.sql());
        }
    }

    private String plusOne(Expression ordinal, Map<Long, String> scope) {
        if (ordinal instanceof Literal literal && literal.value() != null) {
            return Long.toString(((Number) literal.value()).longValue() + 1);
        }
        return "(" + render(ordinal, scope) + " + 1)";
    }

    private static String intervalMicros(Expression interval) {
        if (interval instanceof Literal literal && literal.value() != null) {
            if (literal.dataType() instanceof DayTimeIntervalType) {
                return literal.value().toString();
            }
            if (literal.dataType() instanceof CalendarIntervalType) {
                CalendarInterval value = (CalendarInterval) literal.value();
                if (value.months == 0) {
                    return Long.toString(value.days * 86_400_000_000L + value.microseconds);
                }
            }
        }
        throw new SparkDialectException("Only constant day-time intervals are supported: " + interval.sql());
    }

    private static String intervalDays(Expression interval) {
        if (interval instanceof Literal literal && literal.value() instanceof CalendarInterval value
                && value.months == 0 && value.microseconds == 0) {
            return Integer.toString(value.days);
        }
        throw new SparkDialectException("Only constant whole-day intervals can be added to dates: " + interval.sql());
    }

    /** Casts numeric and datetime results to Spark's type; StarRocks often infers a wider or different one. */
    private static String castResult(String sql, DataType type) {
        if (type instanceof NumericType || type instanceof DateType || type instanceof TimestampType) {
            return Sql.cast(sql, StarRocksTypes.of(type));
        }
        return sql;
    }

    private String join(List<Expression> expressions, Map<Long, String> scope) {
        StringJoiner joiner = new StringJoiner(", ");
        for (Expression expression : expressions) {
            joiner.add(render(expression, scope));
        }
        return joiner.toString();
    }

    private static Instant microsToInstant(long micros) {
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L);
    }
}
