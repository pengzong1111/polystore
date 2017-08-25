package edu.indiana.d2i.polystore;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Util;


public class HtrcSqlParser {

	public static void main(String[] args) throws SqlParseException, ValidationException, RelConversionException {
		String sql = "select c.\"id\", d.\"id\", c.\"country\", s.\"price\", d.\"right\" from \"testtable\" c , \"htrcTestCollection\" s, \"redis\" d WHERE (s.\"name\"=\'item_15\' OR s.\"freq\" <= 5) AND c.\"country\" = \'china\' AND c.\"id\" = d.\"id\" AND c.\"id\" = s.\"id\"";
		// String sql = "select c.id, d.id, c.country, s.price, d.right from
		// testtable c , htrcTestCollection s, redis d WHERE s.name=\'item_15\'
		// AND c.id = s.id AND c.country = \'china\' AND c.id = d.id";

		HtrcSqlParser htrcSqlParser = new HtrcSqlParser();
		/*SqlParser parser = htrcSqlParser.getSqlParser(sql);
		SqlNode sqlNode = parser.parseStmt();
		System.out.println(sqlNode);
		System.out.println(sqlNode.getClass().getCanonicalName());

		SqlSelect select = (SqlSelect) sqlNode;
		System.out.println(select.getSelectList());
		System.out.println(select.getFrom());
		System.out.println(select.getWhere().getKind());

		Map<String, Set<String>> tableToSelectedFields = new HashMap<String, Set<String>>();
		for (SqlNode node : select.getSelectList()) {
			String identifier = node.toString();
			String[] tabAndField = identifier.split("\\.");
			System.out.println(tabAndField[0] + ": " + tabAndField[1]);
			if (tableToSelectedFields.get(tabAndField[0]) == null) {
				Set<String> fieldSet = new HashSet<String>();
				fieldSet.add(tabAndField[1]);
				tableToSelectedFields.put(tabAndField[0], fieldSet);
			} else {
				tableToSelectedFields.get(tabAndField[0]).add(tabAndField[1]);
			}
		}
		System.out.println(tableToSelectedFields);

		SqlBasicCall where = (SqlBasicCall) select.getWhere();
		System.out.println(where.getOperandList());
		System.out.println(where.getKind());
		// put conditions for particular table into its didicated conditon set
		// and also extract potential joins
		parseWhereClause(where);*/
		
		/////
		sql = "select * from \"emps\" where \"name\" like '%e%'";
		htrcSqlParser.plan(sql);

	}

	public void plan(String sql) throws SqlParseException, ValidationException, RelConversionException {
		Planner planner = getPlanner(null);
		SqlNode parse = planner.parse(sql);
		System.out.println(parse.toString());

	    SqlNode validate = planner.validate(parse);
	    RelNode rel = planner.rel(validate).project();
	    System.out.println(toString(rel));
	}

	private Planner getPlanner(List<RelTraitDef> traitDefs, Program... programs) {
		return getPlanner(traitDefs, SqlParser.Config.DEFAULT, programs);
	}

	private Planner getPlanner(List<RelTraitDef> traitDefs, SqlParser.Config parserConfig, Program... programs) {
		final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
		final FrameworkConfig config = Frameworks.newConfigBuilder().parserConfig(parserConfig)
				.defaultSchema(rootSchema.add("hr", new ReflectiveSchema(new HrSchema()))).traitDefs(traitDefs)
				.programs(programs).build();
		return Frameworks.getPlanner(config);
	}

	private static void parseWhereClause(SqlBasicCall predicate) {
		List<SqlNode> operandList = predicate.getOperandList();

		for (SqlNode node : operandList) {
			if (SqlKind.COMPARISON.contains(node.getKind())) {
				System.out.println(node + " : COMPARISON" + " : " + node.getKind());
			} else if (node.getKind() == SqlKind.AND || node.getKind() == SqlKind.OR) {
				System.out.println(node + " : " + node.getKind());
				parseWhereClause((SqlBasicCall) node);
			}
		}

	}

	protected SqlParser getSqlParser(String sql) {
		return SqlParser.create(sql,
				SqlParser.configBuilder().setParserFactory(SqlParserImpl.FACTORY).setQuoting(Quoting.DOUBLE_QUOTE)
						.setUnquotedCasing(Casing.TO_UPPER).setQuotedCasing(Casing.UNCHANGED)
						.setConformance(SqlConformanceEnum.DEFAULT).build());
	}

	
	
	
	
	
	private String toString(RelNode rel) {
	    return Util.toLinux(
	        RelOptUtil.dumpPlan("", rel, SqlExplainFormat.TEXT,
	            SqlExplainLevel.DIGEST_ATTRIBUTES));
	  }
	
	
	
	//////////////////////////////
	
	

	  public static class HrSchema {
	    @Override public String toString() {
	      return "HrSchema";
	    }

	    public final Employee[] emps = {
	      new Employee(100, 10, "Bill", 10000, 1000),
	      new Employee(200, 20, "Eric", 8000, 500),
	      new Employee(150, 10, "Sebastian", 7000, null),
	      new Employee(110, 10, "Theodore", 11500, 250),
	    };
	    public final Department[] depts = {
	      new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
	          new Location(-122, 38)),
	      new Department(30, "Marketing", Collections.<Employee>emptyList(),
	          new Location(0, 52)),
	      new Department(40, "HR", Collections.singletonList(emps[1]), null),
	    };
	    public final Dependent[] dependents = {
	      new Dependent(10, "Michael"),
	      new Dependent(10, "Jane"),
	    };
	    public final Dependent[] locations = {
	      new Dependent(10, "San Francisco"),
	      new Dependent(20, "San Diego"),
	    };

	    public QueryableTable foo(int count) {
	      return Smalls.generateStrings(count);
	    }

	    public TranslatableTable view(String s) {
	      return Smalls.view(s);
	    }
	  }

	  public static class Employee {
	    public final int empid;
	    public final int deptno;
	    public final String name;
	    public final float salary;
	    public final Integer commission;

	    public Employee(int empid, int deptno, String name, float salary,
	        Integer commission) {
	      this.empid = empid;
	      this.deptno = deptno;
	      this.name = name;
	      this.salary = salary;
	      this.commission = commission;
	    }

	    @Override public String toString() {
	      return "Employee [empid: " + empid + ", deptno: " + deptno
	          + ", name: " + name + "]";
	    }

	    @Override public boolean equals(Object obj) {
	      return obj == this
	          || obj instanceof Employee
	          && empid == ((Employee) obj).empid;
	    }
	  }

	  public static class Department {
	    public final int deptno;
	    public final String name;

	    @org.apache.calcite.adapter.java.Array(component = Employee.class)
	    public final List<Employee> employees;
	    public final Location location;

	    public Department(int deptno, String name, List<Employee> employees,
	        Location location) {
	      this.deptno = deptno;
	      this.name = name;
	      this.employees = employees;
	      this.location = location;
	    }

	    @Override public String toString() {
	      return "Department [deptno: " + deptno + ", name: " + name
	          + ", employees: " + employees + ", location: " + location + "]";
	    }

	    @Override public boolean equals(Object obj) {
	      return obj == this
	          || obj instanceof Department
	          && deptno == ((Department) obj).deptno;
	    }
	  }

	  public static class Location {
	    public final int x;
	    public final int y;

	    public Location(int x, int y) {
	      this.x = x;
	      this.y = y;
	    }

	    @Override public String toString() {
	      return "Location [x: " + x + ", y: " + y + "]";
	    }

	    @Override public boolean equals(Object obj) {
	      return obj == this
	          || obj instanceof Location
	          && x == ((Location) obj).x
	          && y == ((Location) obj).y;
	    }
	  }

	  public static class Dependent {
	    public final int empid;
	    public final String name;

	    public Dependent(int empid, String name) {
	      this.empid = empid;
	      this.name = name;
	    }

	    @Override public String toString() {
	      return "Dependent [empid: " + empid + ", name: " + name + "]";
	    }

	    @Override public boolean equals(Object obj) {
	      return obj == this
	          || obj instanceof Dependent
	          && empid == ((Dependent) obj).empid
	          && Objects.equals(name, ((Dependent) obj).name);
	    }
	  }

	  public static class FoodmartSchema {
	    public final SalesFact[] sales_fact_1997 = {
	      new SalesFact(100, 10),
	      new SalesFact(150, 20),
	    };
	  }

	  public static class LingualSchema {
	    public final LingualEmp[] EMPS = {
	      new LingualEmp(1, 10),
	      new LingualEmp(2, 30)
	    };
	  }

	  public static class LingualEmp {
	    public final int EMPNO;
	    public final int DEPTNO;

	    public LingualEmp(int EMPNO, int DEPTNO) {
	      this.EMPNO = EMPNO;
	      this.DEPTNO = DEPTNO;
	    }

	    @Override public boolean equals(Object obj) {
	      return obj == this
	          || obj instanceof LingualEmp
	          && EMPNO == ((LingualEmp) obj).EMPNO;
	    }
	  }

	  public static class FoodmartJdbcSchema extends JdbcSchema {
	    public FoodmartJdbcSchema(DataSource dataSource, SqlDialect dialect,
	        JdbcConvention convention, String catalog, String schema) {
	      super(dataSource, dialect, convention, catalog, schema);
	    }

	    public final Table customer = getTable("customer");
	  }

	  public static class Customer {
	    public final int customer_id;

	    public Customer(int customer_id) {
	      this.customer_id = customer_id;
	    }

	    @Override public boolean equals(Object obj) {
	      return obj == this
	          || obj instanceof Customer
	          && customer_id == ((Customer) obj).customer_id;
	    }
	  }

	  public static class SalesFact {
	    public final int cust_id;
	    public final int prod_id;

	    public SalesFact(int cust_id, int prod_id) {
	      this.cust_id = cust_id;
	      this.prod_id = prod_id;
	    }

	    @Override public boolean equals(Object obj) {
	      return obj == this
	          || obj instanceof SalesFact
	          && cust_id == ((SalesFact) obj).cust_id
	          && prod_id == ((SalesFact) obj).prod_id;
	    }
	  }

}
