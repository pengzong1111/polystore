package edu.indiana.d2i.polystore;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Optional;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;

public class SqlExecutor {

	public static void main(String[] args) {

		SqlParser sqlParser = new SqlParser();
		//String queryStr = "select a,b,c from xyz A where x=y group by x having count(x)>10 order by y  limit 10";
		
		String queryStr = "select A.a, B.b, B.c from xyz A, xxx B where A.x = B.y";
		 Statement wrappedStatement = sqlParser.createStatement(queryStr);
		 System.out.println(wrappedStatement.getClass());
  //       statement = unwrapExecuteStatement(wrappedStatement, sqlParser, session);
		 List<Expression> parameters = wrappedStatement instanceof Execute ? ((Execute) wrappedStatement).getParameters() : emptyList();
         System.out.println(parameters.isEmpty());
         
	//	Query query = (Query)parser.createStatement(sql);
        Query query = (Query)wrappedStatement;
		QuerySpecification body = (QuerySpecification) query.getQueryBody();
		Select select = body.getSelect();
		System.out.println("select:" + select.getSelectItems());
		
		Optional<Relation> from = body.getFrom();
		System.out.println("from:" + from.get());
		
		Optional<Expression> where = body.getWhere();
		System.out.println("where:" + where.get());
		
		
		/*Optional<GroupBy> groupBy = body.getGroupBy();
		System.out.println("groupBy:" + groupBy.get());
		
		Optional<Expression> having = body.getHaving();
		System.out.println("having:" + having.get() );
		
		List<SortItem> orderBy = body.getOrderBy();
		System.out.println("orderBy:" + orderBy);
		
		Optional<String> limit = body.getLimit();
		System.out.println("limit:" + limit.get());*/
	

	}

}
