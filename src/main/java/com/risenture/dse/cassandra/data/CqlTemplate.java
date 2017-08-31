package com.risenture.dse.cassandra.data;

import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

import com.risenture.dse.cassandra.core.CassandraContext;
import com.risenture.dse.cassandra.core.options.StatementOptions;

import java.util.Map;


public class CqlTemplate {

  private final Session session;

  public CqlTemplate(CassandraContext context) {
    this.session = context.getSession();
  }

  public Session getSession() {
    return this.session;
  }

  public ResultSet execute(String cql) {
    return doExecute(new SimpleStatement(cql));
  }

  public ResultSet execute(String cql, StatementOptions options) {
    return doExecute(new SimpleStatement(cql), options);
  }

  public ResultSet execute(String cql, StatementOptionsBinder statementOptionsBinder) {
    return doExecute(new SimpleStatement(cql), statementOptionsBinder);
  }

  public ResultSet execute(String cql, Map<String, Object> values) {
    return doExecute(new SimpleStatement(cql, values));
  }

  public ResultSet execute(String cql, Map<String, Object> values, StatementOptions options) {
    return doExecute(new SimpleStatement(cql, values), options);
  }

  public ResultSet execute(String cql, Map<String, Object> values,
      StatementOptionsBinder statementOptionsBinder) {
    return doExecute(new SimpleStatement(cql, values), statementOptionsBinder);
  }

  public ResultSet execute(Statement statement) {
    return doExecute(statement);
  }

  public ResultSet execute(Statement statement, StatementOptions options) {
    return doExecute(statement, options);
  }

  public ResultSetFuture executeAsync(String cql) {
    return doExecuteAsync(new SimpleStatement(cql));
  }

  public ResultSetFuture executeAsync(String cql, StatementOptions options) {
    return doExecuteAsync(new SimpleStatement(cql), options);
  }

  public ResultSetFuture executeAsync(String cql, Map<String, Object> values) {
    return doExecuteAsync(new SimpleStatement(cql, values));
  }

  public ResultSetFuture executeAsync(String cql, Map<String, Object> values,
      StatementOptions options) {
    return doExecuteAsync(new SimpleStatement(cql, values), options);
  }

  public ResultSetFuture executeAsync(Statement statement) {
    return doExecuteAsync(statement);
  }

  public ResultSetFuture executeAsync(Statement statement, StatementOptions options) {
    return doExecuteAsync(statement, options);
  }

  public PreparedStatement prepare(String query) {
    return doPrepare(new SimpleStatement(query));
  }

  public PreparedStatement prepare(String query, StatementOptions options) {
    return doPrepare(new SimpleStatement(query));
  }

  public PreparedStatement prepare(RegularStatement statement) {
    return doPrepare(statement);
  }

  public PreparedStatement prepare(RegularStatement statement, StatementOptions options) {
    return doPrepare(statement);
  }

  public ListenableFuture<PreparedStatement> prepareAsync(String query) {
    return doPrepareAsync(new SimpleStatement(query));
  }

  public ListenableFuture<PreparedStatement> prepareAsync(String query, StatementOptions options) {
    return doPrepareAsync(new SimpleStatement(query));
  }

  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    return doPrepareAsync(statement);
  }

  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement,
      StatementOptions options) {
    return doPrepareAsync(statement);
  }

  public ResultSet executeBatch(Statement... statements) {
    return executeBatch(null, statements);
  }

  public ResultSet executeBatch(Type type, Statement... statements) {
    type = type == null ? Type.UNLOGGED : type;

    BatchStatement batchStatement = new BatchStatement(type);
    for (Statement statement : statements) {
      batchStatement.add(statement);
    }
    return doExecuteBatch(batchStatement);
  }

  public ResultSet executeBatch(BatchStatement batchStatement) {
    return doExecuteBatch(batchStatement);
  }

  private ResultSet doExecute(Statement statement) {
    return getSession().execute(statement);
  }

  private ResultSet doExecute(Statement statement, StatementOptions options) {
    return doExecute(statement, new DefaultStatementOptionsBinder(options));
  }

  private ResultSet doExecute(Statement statement, StatementOptionsBinder stOptionsBinder) {
    if (stOptionsBinder != null) {
      stOptionsBinder.bindOptions(statement);
    }
    return getSession().execute(statement);
  }

  private ResultSetFuture doExecuteAsync(Statement statement) {
    return getSession().executeAsync(statement);
  }

  private ResultSetFuture doExecuteAsync(Statement statement, StatementOptions options) {
    return doExecuteAsync(statement, new DefaultStatementOptionsBinder(options));
  }

  private ResultSetFuture doExecuteAsync(Statement statement,
      StatementOptionsBinder statementOptionsBinder) {
    if (statementOptionsBinder != null) {
      statementOptionsBinder.bindOptions(statement);
    }
    return getSession().executeAsync(statement);
  }


  private PreparedStatement doPrepare(RegularStatement statement) {
    return getSession().prepare(statement);
  }

  private ListenableFuture<PreparedStatement> doPrepareAsync(RegularStatement statement) {
    return getSession().prepareAsync(statement);
  }

  private ResultSet doExecuteBatch(BatchStatement batchStatement) {
    return getSession().execute(batchStatement);
  }

  private static class DefaultStatementOptionsBinder implements StatementOptionsBinder {

    private final StatementOptions options;

    public DefaultStatementOptionsBinder(StatementOptions options) {
      this.options = options;
    }

    @Override
    public void bindOptions(Statement statement) {
      if (options == null) {
        return;
      }

      if (options.getConsistency() != null) {
        statement.setConsistencyLevel(options.getConsistency());
      }
      if (options.getRetryPolicy() != null) {
        statement.setRetryPolicy(options.getRetryPolicy());
      }
      if (options.getTimestamp() > 0) {
        statement.setDefaultTimestamp(options.getTimestamp());
      }
      if (options.getSerialConsistency() != null) {
        statement.setSerialConsistencyLevel(options.getSerialConsistency());
      }
      if (options.getReadTimeoutMillis() > 0) {
        statement.setReadTimeoutMillis(options.getReadTimeoutMillis());
      }
      // TODO:: add write options...
      // TDOO:: add pagination options...
    }
  }
}
