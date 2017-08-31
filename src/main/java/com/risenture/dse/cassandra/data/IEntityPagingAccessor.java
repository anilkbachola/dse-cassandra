package com.risenture.dse.cassandra.data;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.QueryOptions;
import com.risenture.dse.cassandra.core.options.ReadOptions;
import com.risenture.dse.cassandra.core.options.StatementOptions;


/**
 * @author anil bachola.
 *
 * @param <T> type
 */
public interface IEntityPagingAccessor<T> {

  /**
   * Get all the rows in a single partition.
   * Limits the number of rows returned based on the default 
   * fetch size set using {@link QueryOptions}
   * @param partitionKey parition key
   * @return response
   */
  public Iterable<T> findAll(Object...partitionKey);


  /**
   * Get all the rows in a single partition.
   * Limits the number of rows returned based on the default fetch size 
   * or {@link ReadOptions} fetchSize
   * Gets the next set of results, if an instance of {@link PagingState} 
   * is passed along with {@link ReadOptions} pagingState.
   * @param options statement options
   * @param partitionKey partitionKey
   * 
   * @return response
   */
  public Iterable<T> findAll(StatementOptions options, Object...partitionKey);


  public Iterable<T> first(int size, Object...partitionKeyColumnValues);

  public Iterable<T> next(int size, Object...pkColumnValues);

  public Iterable<T> prev(int size, Object...pkColumnValues);
}
