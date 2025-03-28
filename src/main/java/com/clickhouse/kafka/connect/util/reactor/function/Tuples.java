/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.kafka.connect.util.reactor.function;

import java.util.Collection;
import java.util.function.Function;

/**
 * A {@literal Tuples} is an immutable {@link Collection} of objects, each of which can be of an arbitrary type.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings({"rawtypes"})
public abstract class Tuples implements Function {

	/**
	 * Create a {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple2} with the given array if it is small
	 * enough to fit inside a {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple2} to {@link Tuple3}.
	 *
	 * @param list the content of the Tuple (size 1 to 8)
	 * @return The new {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple2}.
	 * @throws IllegalArgumentException if the array is not of length 1-8
	 */
	public static com.clickhouse.kafka.connect.util.reactor.function.Tuple2 fromArray(Object[] list) {
		//noinspection ConstantConditions
		if (list == null || list.length < 2) {
			throw new IllegalArgumentException("null or too small array, need between 2 and 8 values");
		}

		switch (list.length){
			case 2:
				return of(list[0], list[1]);
			case 3:
				return of(list[0], list[1], list[2]);
		}
		throw new IllegalArgumentException("too many arguments ("+ list.length + "), need between 2 and 8 values");
	}

	/**
	 * Create a {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple2} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not null.
	 * @param t2   The second value in the tuple. Not null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @return The new {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple2}.
	 */
	public static <T1, T2> com.clickhouse.kafka.connect.util.reactor.function.Tuple2<T1, T2> of(T1 t1, T2 t2) {
		return new com.clickhouse.kafka.connect.util.reactor.function.Tuple2<>(t1, t2);
	}

	/**
	 * Create a {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple3} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not null.
	 * @param t2   The second value in the tuple. Not null.
	 * @param t3   The third value in the tuple. Not null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @return The new {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple3}.
	 */
	public static <T1, T2, T3> com.clickhouse.kafka.connect.util.reactor.function.Tuple3<T1, T2, T3> of(T1 t1, T2 t2, T3 t3) {
		return new com.clickhouse.kafka.connect.util.reactor.function.Tuple3<>(t1, t2, t3);
	}
	
	/**
	 * A converting function from Object array to {@link Tuples}
	 *
	 * @return The unchecked conversion function to {@link Tuples}.
	 */
	@SuppressWarnings("unchecked")
	public static Function<Object[], com.clickhouse.kafka.connect.util.reactor.function.Tuple2> fnAny() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link Tuples} to R.
	 *
	 * @param <R> The type of the return value.
	 * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <R> Function<Object[], R> fnAny(final Function<com.clickhouse.kafka.connect.util.reactor.function.Tuple2, R> delegate) {
		return objects -> delegate.apply(Tuples.fnAny().apply(objects));
	}

	/**
	 * A converting function from Object array to {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple2}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 *
	 * @return The unchecked conversion function to {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple2}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Function<Object[], com.clickhouse.kafka.connect.util.reactor.function.Tuple2<T1, T2>> fn2() {
		return empty;
	}


	/**
	 * A converting function from Object array to {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple3}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 *
	 * @return The unchecked conversion function to {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple3}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Function<Object[], com.clickhouse.kafka.connect.util.reactor.function.Tuple3<T1, T2, T3>> fn3() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link com.clickhouse.kafka.connect.util.reactor.function.Tuple3} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <R> The type of the return value.
     * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, R> Function<Object[], R> fn3(final Function<Tuple3<T1, T2, T3>, R> delegate) {
		return objects -> delegate.apply(Tuples.<T1, T2, T3>fn3().apply(objects));
	}

	@Override
	public Tuple2 apply(Object o) {
		return fromArray((Object[])o);
	}

	/**
	 * Prepare a string representation of the values suitable for a Tuple of any
	 * size by accepting an array of elements. This builds a {@link StringBuilder}
	 * containing the String representation of each object, comma separated. It manages
	 * nulls as well by putting an empty string and the comma.
	 *
	 * @param values the values of the tuple to represent
	 * @return a {@link StringBuilder} initialized with the string representation of the
	 * values in the Tuple.
	 */
	static StringBuilder tupleStringRepresentation(Object... values) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < values.length; i++) {
			Object t = values[i];
			if (i != 0) {
				sb.append(',');
			}
			if (t != null) {
				sb.append(t);
			}
		}
		return sb;
	}


	static final Tuples   empty            = new Tuples(){};

	Tuples(){}
}
