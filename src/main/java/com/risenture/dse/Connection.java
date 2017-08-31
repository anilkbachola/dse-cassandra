package com.risenture.dse;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Connection {

	enum ConnectionType{
		FROM,
		TO
	}
	ConnectionType type() default ConnectionType.FROM;
}
