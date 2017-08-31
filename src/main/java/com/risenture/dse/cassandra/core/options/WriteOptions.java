package com.risenture.dse.cassandra.core.options;

public class WriteOptions extends StatementOptions {

  private int ttl;
  private boolean saveNullFields = true;

  public int getTtl() {
    return ttl;
  }
  
  public void setTtl(int ttl) {
    this.ttl = ttl;
  }
  
  public WriteOptions withTtl(int ttl) {
    this.ttl = ttl;
    return this;
  }

  public boolean isSaveNullFields() {
    return saveNullFields;
  }
  
  public void setSaveNullFields(boolean saveNullFields) {
    this.saveNullFields = saveNullFields;
  }
  
  public WriteOptions withSaveNullFields(boolean saveNullFields) {
    this.saveNullFields = saveNullFields;
    return this;
  }

}
