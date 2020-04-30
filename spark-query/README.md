# Query DSL

Wrap a dataframe into a Query with an alias

```scala
val a = Query(df1, "a")
val b = Query(df2, "b")
```

Automatic join with column names

```scala
a + b
InnerJoin(a, b)

a % b
LeftOuterJoin(a, b)

a %> b
RightOuterJoin(a, b)

a %% b
FullOuterJoin(a, b)

a ^ b
LeftSemiJoin(a, b)

a - b
LeftAntiJoin(a, b)

a * b
CrossJoin(a, b)

a & b
UnionQuery(a, b)
```

Join on all common columns

```scala
a + ~b
InnerJoin(a, ~b)
InnerJoin(a, b, CommonColumnsJoiner)
```

Specify columns to join on

```scala
a + b.on("col1", "col2", "col3")
InnerJoin(a, b, "col1", "col2", "col3")
```

Specify different columns left & right

```scala
a + b.on("left_col1" -> "right_col1", ...)
```
Specify spark.sql.Column join column

```scala
a + b.on(a("left_col1") === b("right_col1"))
InnerJoin(a, b, a("left_col1") === b("right_col1"))
```

Filter

```scala
a | a("ok") === 1
a + b | a("ok") === 1
FilterQuery(a, a("ok") === 1)
FilterQuery(a, expr("ok == 1"))
```

Compose

```scala
(a + b - c) * d | (a("ok") === 1) | (b("ok") === 1)
```