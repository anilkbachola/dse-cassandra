package com.risenture.dse.cassandra.data;

import com.datastax.driver.core.Statement;

public interface StatementOptionsBinder {

  public void bindOptions(Statement statement);
}
