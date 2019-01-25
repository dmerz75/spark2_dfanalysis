[(home)](https://dmerz75.github.io/spark2_dfanalysis)

# Analysis:

- [Basic groupBy, orderBy, agg, desribe, and filter commands.](./Analysis_basic_group_orderBy_aggregate_describe.html)

```python
# price based columns

price_cols = ['msrp','discount','tax','upgrades']   

price_sum_expr = [sum(x) for x in price_cols]


df.groupBy("division")\
.agg(*price_sum_expr)\
.orderBy("sum(msrp)",ascending=False)\
.limit(5).toPandas()
```
