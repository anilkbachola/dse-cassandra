package com.risenture.dse.cassandra.data;

import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Mapper.Option;
import com.datastax.driver.mapping.Result;
import com.risenture.dse.cassandra.core.CassandraContext;
import com.risenture.dse.cassandra.core.options.ReadOptions;
import com.risenture.dse.cassandra.core.options.StatementOptions;
import com.risenture.dse.cassandra.core.options.WriteOptions;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public class SimpleEntityAccessor<T> implements IEntityPagingAccessor<T>, 
    IEntityStatementAccessor<T>, IEntityCrudAccessor<T> {

  private Class<T> entityClass;
  private final Mapper<T> mapper;
  private final Session session;
  private final String tableName;
  private final String keyspaceName;

  /**
   * Creates a repository.
   * @param context an instance of Cassandra context
   */
  public SimpleEntityAccessor(CassandraContext context) {
    
    this.mapper = context.getMappingManager().mapper(getEntityClass());
    this.session = context.getSession();
    this.tableName = mapper.getTableMetadata().getName();
    this.keyspaceName = mapper.getTableMetadata().getKeyspace() == null ? null 
        : mapper.getTableMetadata().getKeyspace().getName();

    if (tableName == null || keyspaceName == null) {
      throw new IllegalStateException("table name and/or keyspace name missing on entity");
    }
  }
  
  /**
   * Creates a repository.
   * @param context an instance of Cassandra context
   */
  public SimpleEntityAccessor(CassandraContext context, Class<T> entityClass) {
    this.mapper = context.getMappingManager().mapper(entityClass);
    this.session = context.getSession();
    this.tableName = mapper.getTableMetadata().getName();
    this.keyspaceName = mapper.getTableMetadata().getKeyspace() == null ? null 
        : mapper.getTableMetadata().getKeyspace().getName();

    if (tableName == null || keyspaceName == null) {
      throw new IllegalStateException("table name and/or keyspace name missing on entity");
    }
  }

  @SuppressWarnings("unchecked")
  private Class<T> getEntityClass() {
    if (entityClass == null) {
      Type superclass = getClass().getGenericSuperclass();
      Type type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
      String className = type.toString().split(" ")[1];
      try {
        entityClass = (Class<T>) Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("entity class could not be found");
      }
    }
    return entityClass;
  }

  public Session getSession() {
    return this.session;
  }
  
  @Override
  public long count(Object... pkColumnValues) {
    List<ColumnMetadata> pkColumns = mapper.getTableMetadata().getPrimaryKey();

    long partitionKeyColumnsSize = mapper.getTableMetadata().getPartitionKey().size();
    if (pkColumnValues.length < partitionKeyColumnsSize) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PARTITION KEY columns provided, %d expected but got %d", 
              partitionKeyColumnsSize, pkColumnValues.length));
    }
    
    Select select = QueryBuilder.select().countAll().from(keyspaceName, tableName);
    int count = 1;
    for (ColumnMetadata pkColumn : pkColumns) {
      if (count <= pkColumnValues.length) {
        select.where(QueryBuilder.eq(pkColumn.getName(), QueryBuilder.bindMarker()));
      }
      count++;
    }
    PreparedStatement ps = session.prepare(select);
    Statement statement = ps.bind(pkColumnValues);

    ResultSet rs = getSession().execute(statement);
    return rs.one().getLong(0);
  }

  @Override
  public boolean exists(Object... pkColumnValues) {
    return count(pkColumnValues) > 0;
  }

  @Override
  public void save(T entity) {
    mapper.save(entity);
  }

  @Override
  public void save(StatementOptions options, T entity) {
    mapper.save(entity, mapperOptions(options));
  }

  @Override
  public boolean saveIfNotExists(T entity) {
    // TODO :: pass the values and column names to the query
    Statement insert = QueryBuilder.insertInto(keyspaceName, tableName).ifNotExists();
    ResultSet rs = mapper.getManager().getSession().execute(insert);
    return rs.wasApplied();
  }

  @Override
  public boolean saveIfNotExists(StatementOptions options, T entity) {
    return false;
  }

  @Override
  public void ingest(Iterable<T> entities) {
    ingest(new StatementOptions(), entities);
  }

  @Override
  public List<ListenableFuture<Void>> ingest(StatementOptions options, Iterable<T> entities) {
   /* BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
    for(T entity: entities){
      Statement s = mapper.saveQuery(entity, mapperOptions(options));
      bs.add(s);
    }
    mapper.getManager().getSession().execute(bs);*/
    List<ListenableFuture<Void>> futures = new ArrayList<>();
    for (T entity: entities) {
      futures.add(saveAsync(entity));
    }
    return futures;
  }

  //TODO:: evaluate whether this version of replace method really need to be supported
  @Override
  public void replace(T newEntity, T oldEntity) {

    BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
    bs.add(mapper.deleteQuery(oldEntity));
    bs.add(mapper.saveQuery(newEntity));
    mapper.getManager().getSession().execute(bs);
  }

  @Override
  public void replace(T newEntity, Object... pkColumnValues) {
    List<ColumnMetadata> pkColumns = mapper.getTableMetadata().getPrimaryKey();
    
    if (pkColumnValues.length % pkColumns.size() != 0) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PRIMARY KEY columns provided,expected of %d, but got %d",
              pkColumns.size(), pkColumnValues.length));
    }
    
    BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
    bs.add(mapper.deleteQuery(pkColumnValues));
    bs.add(mapper.saveQuery(newEntity));
    mapper.getManager().getSession().execute(bs);
  }

  @Override
  public void replace(String[] properties,Object[] values, Object...pkColumnValues) {
    List<ColumnMetadata> pkColumns = mapper.getTableMetadata().getPrimaryKey();
    if (pkColumnValues.length % pkColumns.size() != 0) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d",
              pkColumns.size(), pkColumnValues.length));
    }
    BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
    bs.add(mapper.deleteQuery(pkColumnValues));

    Insert insert = QueryBuilder.insertInto(keyspaceName, tableName);
  }

  @Override
  public T findOne(Object... pkColumnValues) {
    return mapper.get(pkColumnValues);
  }

  @Override
  public T findOne(StatementOptions options, Object... pkColumnValues) {
    return mapper.get(mapperOptionsAndValues(options, pkColumnValues));
  }

  @Override
  public Iterable<T> findIn(Object... pkColumnValues) {
    return findIn(new StatementOptions(), pkColumnValues);
  }

  @Override
  public Iterable<T> findIn(StatementOptions options,
      Object... pkColumnValues) {
    List<ColumnMetadata> pkColumns = mapper.getTableMetadata().getPrimaryKey();
    
    if (pkColumnValues.length % pkColumns.size() != 0) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PRIMARY KEY columns provided, "
              + "expected multiples of %d, but got %d", pkColumns.size(), pkColumnValues.length));
    }
    List<T> results = new ArrayList<>();
    List<ListenableFuture<T>> futures = new ArrayList<>();
    int valuesLength = pkColumnValues.length;
    int partionKeyColumns = pkColumns.size();

    for (int i = 0 ; i < valuesLength; i = i + partionKeyColumns) {
      ListenableFuture<T> future = 
          selectOneAsync(options,Arrays.copyOfRange(pkColumnValues, i, i + partionKeyColumns));
      futures.add(future);
    }
    for (ListenableFuture<T> future : futures) {
      try {
        results.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();//TODO: handle error to retry the fetch or return error
      }
    }
    return results;
  }

  @Override
  public Iterable<T> findAll(Object... partitionKeyColumnValues) {
    return findAll(null, partitionKeyColumnValues);
  }

  @Override
  public Iterable<T> findAll(StatementOptions options, Object... partitionKeyColumnValues) {

    List<ColumnMetadata> partitionKeyColumns = mapper.getTableMetadata().getPartitionKey();
    if (partitionKeyColumnValues.length != partitionKeyColumns.size()) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PARTITION KEY columns provided, %d expected "
              + "but got %d",partitionKeyColumns.size(), partitionKeyColumnValues.length));
    }
    
    Select select = QueryBuilder.select().from(keyspaceName, tableName);
    for (ColumnMetadata pkColumn : partitionKeyColumns) {
      select.where(QueryBuilder.eq(pkColumn.getName(), QueryBuilder.bindMarker()));
    }
    PreparedStatement ps = session.prepare(select);
    Statement statement = ps.bind(partitionKeyColumnValues);

    //TODO: handle statement options binding separately, cleanup
    if (options instanceof ReadOptions) {
      ReadOptions readOptions = (ReadOptions)options;
      if (readOptions.getFetchSize() > 0) {
        statement.setFetchSize(readOptions.getFetchSize());
      }
      if (readOptions.getPagingState() != null) {
        statement.setPagingState(readOptions.getPagingState());
      }
    }
    ResultSet rs = mapper.getManager().getSession().execute(statement);
    return mapper.map(rs);
  }

  @Override
  public void delete(Object... objects) {
    mapper.delete(objects);
  }

  @Override
  public void delete(StatementOptions options, Object... objects) {
    mapper.delete(mapperOptionsAndValues(options, objects));
  }

  @Override
  public void delete(T entity) {
    mapper.delete(entity);
  }

  @Override
  public void delete(StatementOptions options, T entity) {
    mapper.delete(entity, mapperOptions(options));
  }

  @Override
  public void delete(Iterable<T> entities) {
    // TODO Auto-generated method stub
  }

  @Override
  public void delete(StatementOptions options, Iterable<T> entities) {
    // TODO Auto-generated method stub

  }

  @Override
  public void truncate() {
    Statement statement = QueryBuilder.truncate(keyspaceName, tableName);
    getSession().execute(statement);
  }

  @Override
  public Statement saveQuery(T entity) {
    return mapper.saveQuery(entity);
  }

  @Override
  public Statement saveQuery(StatementOptions options, T entity) {
    return mapper.saveQuery(entity, mapperOptions(options));
  }

  @Override
  public Statement getQuery(Object... objects) {
    return mapper.getQuery(objects);
  }

  @Override
  public Statement getQuery(StatementOptions options, Object... objects) {
    return mapper.getQuery(mapperOptionsAndValues(options, objects));
  }

  @Override
  public Statement deleteQuery(T entity) {
    return mapper.deleteQuery(entity);
  }

  @Override
  public Statement deleteQuery(StatementOptions options, T entity) {
    return mapper.deleteQuery(entity, mapperOptions(options));
  }

  @Override
  public Statement deleteQuery(Object... objects) {
    return mapper.deleteQuery(objects);
  }

  @Override
  public Statement deleteQuery(StatementOptions options, Object... objects) {
    return mapper.deleteQuery(mapperOptionsAndValues(options, objects));
  }

  @Override
  public ListenableFuture<Void> saveAsync(T entity) {
    return mapper.saveAsync(entity);
  }

  @Override
  public ListenableFuture<Void> saveAsync(StatementOptions options, T entity) {
    return mapper.saveAsync(entity, mapperOptions(options));
  }

  @Override
  public ListenableFuture<T> selectOneAsync(Object... objects) {
    return mapper.getAsync(objects);
  }

  @Override
  public ListenableFuture<T> selectOneAsync(StatementOptions options,
      Object... objects) {
    return mapper.getAsync(mapperOptionsAndValues(options, objects));
  }

  @Override
  public ListenableFuture<Void> deleteAsync(Object... objects) {
    return mapper.deleteAsync(objects);
  }

  @Override
  public ListenableFuture<Void> deleteAsync(StatementOptions options,
      Object... objects) {
    return mapper.deleteAsync(mapperOptionsAndValues(options, objects));
  }

  @Override
  public ListenableFuture<Void> deleteAsync(T entity) {
    return mapper.deleteAsync(entity);
  }

  @Override
  public ListenableFuture<Void> deleteAsync(StatementOptions options, T entity) {
    return mapper.deleteAsync(entity, statementOptionsToMapperOptions(options));
  }

  private Option[] mapperOptions(StatementOptions statementOptions) {
    Option[] options = null;
    List<Option> optionList = statementOptionsToMapperOptions(statementOptions);
    if (optionList.size() > 0) {
      options = new Option[optionList.size()];
      optionList.toArray(options);
    }
    return options;
  }

  private Object[] mapperOptionsAndValues(StatementOptions statementOptions, Object...objects ) {
    List<Option> optionList = statementOptionsToMapperOptions(statementOptions);

    Object[] finalObjects = new Object[optionList.size() + objects.length];
    int optionSize = 0;
    for (Option option: optionList) {
      finalObjects[optionSize] = option;
      optionSize++;
    }
    for (Object object: objects) {
      finalObjects[optionSize] = object;
      optionSize++;
    }
    return finalObjects;
  }

  private List<Option> statementOptionsToMapperOptions(StatementOptions statementOptions) {
    if (statementOptions == null) {
      return Collections.emptyList();
    }
    
    List<Option> optionList = new ArrayList<Option>();

    if (statementOptions.getConsistency() != null) {
      optionList.add(Option.consistencyLevel(statementOptions.getConsistency()));
    }
    if (statementOptions.getTimestamp() > 0) {
      optionList.add(Option.timestamp(statementOptions.getTimestamp()));
    }

    if (statementOptions.isTracing()) {
      optionList.add(Option.tracing(statementOptions.isTracing()));
    }

    if (statementOptions instanceof WriteOptions) {
      if (((WriteOptions) statementOptions).getTtl() > 0) {
        optionList.add(Option.ttl(((WriteOptions) statementOptions).getTtl()));
      }

      if (((WriteOptions) statementOptions).isSaveNullFields()) {
        optionList.add(Option.saveNullFields(((WriteOptions) statementOptions).isSaveNullFields()));
      }
    }
    return optionList;
  }

  @Override
  public void prepend(String columnName, Object item, Object... pkColumnValues) {

    List<ColumnMetadata> pkColumns = mapper.getTableMetadata().getPrimaryKey();
    if (pkColumnValues.length != pkColumns.size()) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d",
              pkColumns.size(), pkColumnValues.length));
    }
    ColumnMetadata columnMeta = mapper.getTableMetadata().getColumn(columnName);
    Update update = QueryBuilder.update(keyspaceName, tableName);

    if (columnMeta.getType().getName() == Name.LIST) {
      if (item instanceof List<?>) {
        List<?> list = (List<?>) item;
        if (list.size() != 0) {
          update.with(QueryBuilder.prependAll(columnMeta.getName(), list));
        }
      } else {
        update.with(QueryBuilder.prepend(columnMeta.getName(), item));
      }
    }

    for (ColumnMetadata pkColumn : pkColumns) {
      update.where(QueryBuilder.eq(pkColumn.getName(), QueryBuilder.bindMarker()));
    }

    PreparedStatement ps = session.prepare(update);
    Statement statement = ps.bind(pkColumnValues);
    getSession().execute(statement);
  }

  @Override
  public void append(String columnName, Object item, Object... pkColumnValues) {
    List<ColumnMetadata> pkColumns = mapper.getTableMetadata().getPrimaryKey();
    if (pkColumnValues.length != pkColumns.size()) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d",
              pkColumns.size(), pkColumnValues.length));
    }
    ColumnMetadata columnMeta = mapper.getTableMetadata().getColumn(columnName);
    Update update = QueryBuilder.update(keyspaceName, tableName);

    if (columnMeta.getType().getName() == Name.LIST) {
      if (item instanceof List<?>) {
        List<?> list = (List<?>) item;
        if (list.size() != 0) {
          update.with(QueryBuilder.appendAll(columnMeta.getName(), list));
        }
      } else {
        update.with(QueryBuilder.append(columnMeta.getName(), item));
      }
    } else if (columnMeta.getType().getName() == Name.SET) {
      if (item instanceof Set<?>) {
        Set<?> set = (Set<?>) item;
        if (set.size() != 0) {
          update.with(QueryBuilder.addAll(columnMeta.getName(), set));
        }
      } else {
        update.with(QueryBuilder.add(columnMeta.getName(), item));
      }
    } else if (columnMeta.getType().getName() == Name.MAP) {
      Map<?, ?> map = (Map<?, ?>) item;
      if (map.size() != 0) {
        update.with(QueryBuilder.putAll(columnMeta.getName(), map));
      }
    }

    for (ColumnMetadata pkColumn : pkColumns) {
      update.where(QueryBuilder.eq(pkColumn.getName(), QueryBuilder.bindMarker()));
    }

    PreparedStatement ps = session.prepare(update);
    Statement statement = ps.bind(pkColumnValues);
    getSession().execute(statement);

  }

  @Override
  public void replaceAt(String propertyName, Object item, int idx,
      Object... primaryKeys) {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateValue(String propertyName, Object value,
      Object... primaryKeys) {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateValues(String[] propertyNames, Object[] values,
      Object... primaryKeys) {
    // TODO Auto-generated method stub

  }

  @Override
  public Result<T> first(int size, Object... partitionKeyColumnValues) {

    List<ColumnMetadata> partitionKeyColumns = mapper.getTableMetadata().getPartitionKey();
    if (partitionKeyColumnValues.length != partitionKeyColumns.size()) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PARTITION KEY columns provided, %d expected but got %d",
              partitionKeyColumns.size(), partitionKeyColumnValues.length));
    }
    
    Select select = QueryBuilder.select().from(keyspaceName, tableName);
    for (ColumnMetadata pkColumn : partitionKeyColumns) {
      select.where(QueryBuilder.eq(pkColumn.getName(), QueryBuilder.bindMarker()));
    }
    select.limit(size);
    PreparedStatement ps = session.prepare(select);
    Statement statement = ps.bind(partitionKeyColumnValues);
    //statement.setFetchSize(size);
    ResultSet rs = mapper.getManager().getSession().execute(statement);
    return mapper.map(rs);
  }

  @Override
  public Result<T> next(int size, Object... pkColumnValues) {
    List<ColumnMetadata> pkColumns = mapper.getTableMetadata().getPrimaryKey();
    if (pkColumnValues.length != pkColumns.size()) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d",
              pkColumns.size(), pkColumnValues.length));
    }
    List<ColumnMetadata> partitionKeyColumns = mapper.getTableMetadata().getPartitionKey();

    Select select = QueryBuilder.select().from(keyspaceName, tableName);
    //equality for partition key columns
    for (ColumnMetadata pkColumn : partitionKeyColumns) {
      select.where(QueryBuilder.eq(pkColumn.getName(), QueryBuilder.bindMarker()));
    }
    List<String> clusteringColumns = new ArrayList<>();
    for (ColumnMetadata clustColumn : mapper.getTableMetadata().getClusteringColumns()) {
      clusteringColumns.add(clustColumn.getName());
    }
    ClusteringOrder order = mapper.getTableMetadata().getClusteringOrder().get(0);
    //append where clause for clustering columns
    if (order.equals(ClusteringOrder.ASC)) {
      select.where(QueryBuilder.gt(clusteringColumns, Arrays.asList(
          Arrays.copyOfRange(pkColumnValues, partitionKeyColumns.size(), pkColumnValues.length)))
      );
    } else {
      select.where(QueryBuilder.lt(clusteringColumns, Arrays.asList(
          Arrays.copyOfRange(pkColumnValues, partitionKeyColumns.size(), pkColumnValues.length)))
      );
    }

    select.limit(size);

    PreparedStatement ps = session.prepare(select);
    Statement statement = ps.bind(
        Arrays.copyOfRange(pkColumnValues, 0, partitionKeyColumns.size())
    );
    //statement.setFetchSize(size);

    ResultSet rs = mapper.getManager().getSession().execute(statement);
    return mapper.map(rs);
  }

  @Override
  public Result<T> prev(int size, Object... pkColumnValues) {
    List<ColumnMetadata> pkColumns = mapper.getTableMetadata().getPrimaryKey();
    if (pkColumnValues.length != pkColumns.size()) {
      throw new IllegalArgumentException(
          String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d",
              pkColumns.size(), pkColumnValues.length));
    }
    
    List<ColumnMetadata> partitionKeyColumns = mapper.getTableMetadata().getPartitionKey();

    Select select = QueryBuilder.select().from(keyspaceName, tableName);
    //equality for partition key columns
    for (ColumnMetadata pkColumn : partitionKeyColumns) {
      select.where(QueryBuilder.eq(pkColumn.getName(), QueryBuilder.bindMarker()));
    }
    List<String> clusteringColumns = new ArrayList<>();
    for (ColumnMetadata clustColumn : mapper.getTableMetadata().getClusteringColumns()) {
      clusteringColumns.add(clustColumn.getName());
    }
    ClusteringOrder order = mapper.getTableMetadata().getClusteringOrder().get(0);
    //append where clause for clustering columns
    if (order.equals(ClusteringOrder.ASC)) {
      select.where(QueryBuilder.lt(clusteringColumns, Arrays.asList(
          Arrays.copyOfRange(pkColumnValues, partitionKeyColumns.size(), pkColumnValues.length))
          ));
    } else {
      select.where(QueryBuilder.gt(clusteringColumns, Arrays.asList(
          Arrays.copyOfRange(pkColumnValues, partitionKeyColumns.size(), pkColumnValues.length))
          ));
    }

    select.limit(size);

    PreparedStatement ps = session.prepare(select);
    Statement statement = ps.bind(
        Arrays.copyOfRange(pkColumnValues, 0, partitionKeyColumns.size()));
    //statement.setFetchSize(size);

    ResultSet rs = mapper.getManager().getSession().execute(statement);
    return mapper.map(rs);
  }
}