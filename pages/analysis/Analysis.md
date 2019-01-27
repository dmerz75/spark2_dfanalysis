[(home)](https://dmerz75.github.io/spark2_dfanalysis)

# Analysis:

- [Basic groupBy, orderBy, agg, desribe, and filter commands.](./Analysis_basic_group_orderBy_aggregate_describe.md)
```python
# price based columns

price_cols = ['msrp','discount','tax','upgrades']   

price_sum_expr = [sum(x) for x in price_cols]


df.groupBy("division")\
.agg(*price_sum_expr)\
.orderBy("sum(msrp)",ascending=False)\
.limit(5).toPandas()
```

- [Multiple Category groupBy and describe by group](./Categorize.md)
```python
df\
.groupBy("price_range")\
.agg(count("price_range"),min("net_worth"),max("net_worth"),mean("net_worth"),sum("net_worth"))\
.limit(5)\
.toPandas()
```
