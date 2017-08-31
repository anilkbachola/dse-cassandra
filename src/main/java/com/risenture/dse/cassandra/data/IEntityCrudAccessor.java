package com.risenture.dse.cassandra.data;

import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;

import com.risenture.dse.cassandra.core.options.StatementOptions;
import com.risenture.dse.cassandra.core.options.WriteOptions;

/**
 *  Cassandra CRUD repository.
 * @author anil bachola
 *
 * @param <T> Entity
 */
public interface IEntityCrudAccessor<T> extends IEntityAccessor<T> {

  /**
   * Counts total number of records inside a partition of a table.
   * Values to all partition key columns must be provided and additionally 
   * values for clustering columns can be passed in order
   * @param pkColumnValues Values for the primary key columns
   * @return count of rows
   */
  public long count(Object... pkColumnValues);


  /**
   * Checks whether a row or partition exists.
   * Values to all partition key columns must be provided and additionally values
   * for clustering columns can be passed in order
   * @param pkColumnValues Values for the primary key columns
   * @return boolean
   */
  public boolean exists(Object... pkColumnValues);

  /**
   * Saves the entity.
   * @param entity the cassandra entity to be saved
   */
  public void save(T entity);


  /**
   * Saves an entity and pass additional statement options required on the insert query.
   * @param options An instance of {@link StatementOptions} or {@link WriteOptions}
   * @param entity The entity to be saved
   */
  public void save(StatementOptions options, T entity);

  /**
   * Replace is a function to cater for situations, where there is a need to update 
   * the primary key column values.
   * By default, the values for the primary key columns can not be updated. 
   * If there is certainly a need, for example, update value for a clustering column, 
   * this function can be used.
   * Do not use extensively, as each replace will delete the old partition and creates 
   * a new partition, this will eventually create a lot of tombstones
   * @param newEntity Entity with new values populated
   * @param oldEntity Entity with old values
   */
  public void replace(T newEntity, T oldEntity);

  /**
   * This method is similar to {@link #replace(Object, Object)} with following differences.
   * Reference to old entity can be queried by using primary key columns
   * @param newEntity Entity with new values populated
   * @param pkColumnValues Values for the primary key columns
   */
  public void replace(T newEntity, Object...pkColumnValues);

  /**
   * This method is similar to {@link #replace(Object, Object)} with following differences.
   * Reference to old entity can be queried by using primary key columns
   * @param columns An array of columns that need to be updated
   * @param values An array of new values for the columns
   * @param pkColumnValues Values for the primary key columns
   */
  public void replace(String[] columns, Object[] values, Object...pkColumnValues);

  /**
   * Save an entity if its not already exists.
   * This is a slow operation as it queries first to check the record exists 
   * and then inserts if not exists.
   * @param entity Entity to be saved
   * @return boolean
   */
  public boolean saveIfNotExists(T entity);

  /**
   * This method is same as {@link #saveIfNotExists(Object)}, except:
   * Additional {@link StatementOptions} can be passed to add options to the insert query generated.
   * @param options An instance of An instance of {@link StatementOptions} or {@link WriteOptions}
   * @param entity Entity to be saved
   * @return boolean
   */
  public boolean saveIfNotExists(StatementOptions options, T entity);


  /**
   * Saves multiple entities. Uses the Async APIs to save each entity separately
   * This API will not guarantee the save of all entities. 
   * Retries the save and returns error for failed entities
   * @param entities List of entities to be saved
   */
  public void ingest(Iterable<T> entities);

  /**
   * This method is same as {@link #ingest(Iterable)}, except
   * Additional {@link StatementOptions} can be passed to add options to the insert query generated.
   * @param options An instance of {@link StatementOptions} or {@link WriteOptions}
   * @param entities list of entities
   * @return 
   */
  public List<ListenableFuture<Void>> ingest(StatementOptions options, Iterable<T> entities);


  /**
   * Finds a single record in a partition if the values for all primary key columns provided.
   * Otherwise, 
   * gets the first record in a partition, if values for only partition key columns provided.
   * @param pkColumnValues Values for primary key columns
   * @return entity
   */
  public T findOne(Object... pkColumnValues);

  /**
   * This method is same as {@link #findOne(Object...)} except,
   * Additional {@link StatementOptions} can be passed to add options to the insert query generated.
   * @param options An instance of {@link StatementOptions} or {@link ReadOptions}
   * @param pkColumnValues Values for primary key columns
   * @return entity
   */
  public T findOne(StatementOptions options, Object... pkColumnValues);


  public Iterable<T> findIn(Object... pkColumnValues);
  
  public Iterable<T> findIn(StatementOptions options, Object... pkColumnValues);

  public void delete(Object...objects);
  
  public void delete(StatementOptions options, Object...objects);

  public void delete(T entity);
  
  public void delete(StatementOptions options, T entity);

  public void delete(Iterable<T> entities);
  
  public void delete(StatementOptions options, Iterable<T> entities);

  public void truncate();

  public void prepend(String propertyName, Object item, Object...primaryKeys);
  
  public void append(String propertyName, Object item, Object...primaryKeys);
  
  public void replaceAt(String propertyName, Object item, int idx, Object...primaryKeys);

  public void updateValue(String propertyName, Object value, Object...primaryKeys) ;
  
  public void updateValues(String[] propertyNames, Object[] values, Object...primaryKeys);

  public ListenableFuture<Void> saveAsync(T entity);
  
  public ListenableFuture<Void> saveAsync(StatementOptions options, T entity);

  public ListenableFuture<T> selectOneAsync(Object...objects );
  
  public ListenableFuture<T> selectOneAsync(StatementOptions options, Object...objects );

  public ListenableFuture<Void> deleteAsync(Object...objects );
  
  public ListenableFuture<Void> deleteAsync(StatementOptions options, Object...objects);

  public ListenableFuture<Void> deleteAsync(T entity);
  
  public ListenableFuture<Void> deleteAsync(StatementOptions options, T entity);

}
