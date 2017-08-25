package edu.indiana.d2i.polystore;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;

public class Linq4jExample {

	public static void main(String[] args) {
		ParameterExpression x = Expressions.parameter(long.class, "x");
		ParameterExpression y = Expressions.parameter(long.class, "y");
		
		Method methodGetMillis = Linq4j.getMethod("java.lang.System", "currentTimeMillis");
		Method methodGetNanos = Linq4j.getMethod("java.lang.System", "nanoTime");
		
		ConstantExpression zero = Expressions.constant(0L);
		
		BlockStatement statement = Expressions.block(
				Expressions.declare(0, x, zero),
				Expressions.declare(Modifier.FINAL, y, Expressions.call(methodGetMillis)),
				Expressions.ifThen(Expressions.greaterThan(Expressions.call(methodGetNanos), zero), Expressions.statement(Expressions.assign(x, y))),
				Expressions.statement(Expressions.call(Expressions.field(null, System.class, "out"), "println", x))
				);
		
		System.out.println(statement.toString());

		
		/*{
			  final java.util.List<T> predicates = java.util.Arrays.asList(new String[] {});
			  return ((org.apache.calcite.adapter.htrc.cassandra.HtrcCassandraTranslatableTable.HtrcCassandraQueryable) org.apache.calcite.schema.Schemas.queryable(root, root.getRootSchema().getSubSchema("htrcCassandra"), java.lang.Object[].class, "testtable")).xquery(java.util.Arrays.asList(new org.apache.calcite.util.Pair[] {
			      new org.apache.calcite.util.Pair(
			        "id",
			        java.lang.String.class),
			      new org.apache.calcite.util.Pair(
			        "country",
			        java.lang.String.class)}), java.util.Arrays.asList(new org.apache.calcite.util.Pair[] {
			      new org.apache.calcite.util.Pair(
			        "id",
			        "id"),
			      new org.apache.calcite.util.Pair(
			        "country",
			        "country")}), predicates, predicates, 0, -1);
			}*/
	}

}
