package com.risenture.dse.core;

import com.risenture.dse.Connection.ConnectionType;
import com.risenture.dse.Connection;
import com.risenture.dse.Edge;
import com.risenture.dse.IdProperty;
import com.risenture.dse.NotMapped;
import com.risenture.dse.Property;
import com.risenture.dse.Vertex;
import com.risenture.dse.graph.EdgeMapper;
import com.risenture.dse.graph.NodeMapper;
import com.risenture.dse.graph.PropertyMapper;
import com.risenture.dse.graph.VertexMapper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class GraphAnnotationParser {

  public static <T> NodeMapper<T> parseGraphNodes(Class<T> klass) {
    Vertex vertex = klass.getAnnotation(Vertex.class);
    Edge edge = klass.getAnnotation(Edge.class);
    if (vertex != null) {
      return parseVertexNodes(klass);
    }
    if (edge != null) {
      return parseEdgeNodes(klass);
    }

    return null;
  }

  private static <T> VertexMapper<T> parseVertexNodes(Class<T> klass){
    Vertex vertex = klass.getAnnotation(Vertex.class);
    String label = vertex.label();

    VertexMapper<T> vertexMapper = new VertexMapper<T>(klass, label);

    List<Field> propertyFields = new ArrayList<Field>();
    List<Field> idPropertyFields = new ArrayList<Field>();
    //List<Field> indexedPropFields = new ArrayList<Field>();

    for (Field field : klass.getDeclaredFields()) {
      if(field.getAnnotation(NotMapped.class) != null)
        continue;
      if(field.getAnnotation(Property.class) != null){
        propertyFields.add(field);
      } 
      if(field.getAnnotation(IdProperty.class) != null){
        idPropertyFields.add(field);
      }
      /*if(field.getAnnotation(Indexed.class) != null){
				indexedPropFields.add(field);
			}*/
    }

    vertexMapper.addProperties(
        createPropertyMappers(idPropertyFields, klass),
        createPropertyMappers(propertyFields, klass)
        );
    return vertexMapper;
  }

  @SuppressWarnings("unchecked")
  public static <T, S, E> EdgeMapper<T,S,E> parseEdgeNodes(Class<T> klass){
    Edge edge = klass.getAnnotation(Edge.class);
    String label = edge.label();

    VertexMapper<S> inVMapper = null;
    VertexMapper<E> outVMapper = null;
    PropertyMapper<T> inVProperty = null;
    PropertyMapper<T> outVProperty = null;

    List<Field> propertyFields = new ArrayList<Field>();

    for (Field field : klass.getDeclaredFields()) {
      if(field.getAnnotation(NotMapped.class) != null)
        continue;
      if(field.getAnnotation(Property.class) != null){
        propertyFields.add(field);
      }
      if(field.getAnnotation(Connection.class) != null){
        if(field.getAnnotation(Connection.class).type() == ConnectionType.TO){
          outVMapper = (VertexMapper<E>) parseVertexNodes(field.getType());
          outVProperty = createPropertyMapper(field, klass);
        }else{
          inVMapper = (VertexMapper<S>) parseVertexNodes(field.getType());
          inVProperty = createPropertyMapper(field, klass);
        }
      }
    }
    EdgeMapper<T,S,E> edgeMapper = new EdgeMapper<T,S,E>(klass, label, inVMapper, outVMapper, inVProperty, outVProperty);
    edgeMapper.addProperties(
        createPropertyMappers(propertyFields, klass));

    return edgeMapper;
  }

  private static <S> PropertyMapper<S> createPropertyMapper(Field field,
      Class<S> klass) {
    return new PropertyMapper<S>(klass, field, 0);
  }

  private static <T> List<PropertyMapper<T>> createPropertyMappers(List<Field> fields, Class<T> klass){
    List<PropertyMapper<T>> propertyMappers = new ArrayList<PropertyMapper<T>>(fields.size());

    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      PropertyMapper<T> propertyMapper = new PropertyMapper<T>(klass, field, i);
      propertyMappers.add(propertyMapper);
    }
    return propertyMappers;
  }
}
