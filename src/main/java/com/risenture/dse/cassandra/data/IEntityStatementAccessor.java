package com.risenture.dse.cassandra.data;

import com.datastax.driver.core.Statement;
import com.risenture.dse.cassandra.core.options.StatementOptions;

public interface IEntityStatementAccessor<T> extends IEntityCrudAccessor<T> {

  public Statement saveQuery(T entity);
  
  public Statement saveQuery(StatementOptions options, T entity);

  public Statement getQuery(Object... objects);
  
  public Statement getQuery(StatementOptions options, Object... objects);

  public Statement deleteQuery(T entity) ;
  
  public Statement deleteQuery(StatementOptions options,T entity);

  public Statement deleteQuery(Object...objects );
  
  public Statement deleteQuery(StatementOptions options, Object...objects );
}
