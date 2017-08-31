package com.risenture.dse.graph;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.risenture.dse.Property;

public class PropertyMapper<T> {
	
	final Class<T> klass;
	final String propertyName;
	final String fieldName;
	final int position;
	final Class<?> fieldType;
	private final Method readMethod;
	private final Method writeMethod;

	public PropertyMapper(Class<T> klass, Field field, int pos) {
		super();
		this.klass = klass;
		this.fieldName = field.getName();
		this.propertyName = field.getAnnotation(Property.class)==null ? field.getName() : field.getAnnotation(Property.class).name();
		this.position = pos;
		this.fieldType = field.getType();
		//access read and write accessor methods. Ensures accessor methods are available.
		try {
			PropertyDescriptor pd = new PropertyDescriptor(fieldName, field.getDeclaringClass());
			this.readMethod = pd.getReadMethod();
			this.writeMethod = pd.getWriteMethod();
		} catch (IntrospectionException e) {
			throw new IllegalArgumentException("could not find getter and setter for field: '" + fieldName + "'");
		}
	}

	public Object getValue(T node){
		try {
			return readMethod.invoke(node);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Could not get field '" + fieldName + "'");
		} catch (Exception e) {
			throw new IllegalStateException("Unable to access getter for '" + fieldName + "' in " + node.getClass().getName(), e);
		}
	}

	public void setValue(T node, Object value){
		try {
			writeMethod.invoke(node, value);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not set field '" + fieldName + "' to value '" + value + "'");
		} catch (Exception e) {
			throw new IllegalStateException("Unable to access setter for '" + fieldName + "' in " + node.getClass().getName(), e);
		}
	}

	public String getPropertyName() {
		return propertyName;
	}

	public String getFieldName() {
		return fieldName;
	}

	public int getPosition() {
		return position;
	}

}
