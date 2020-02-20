# Data Store

Data Store is library that implenets a time series database with automatic data rollup.

Once the data is inserted it is aggregated by hour, day, month and year on-the-fly

Requests can be made on the raw data or on the aggregated data

Data can be dumped so extracted and pushblish to other service periodically

It is easy to build a REST service around the library making ot accessible by other processes

Sample program
```python
if __name__ == "__main__":
    db = Datastore("./telemetry.db3", create=True)
    sensor_name = "test"
    t = Timeseries(name=sensor_name)
    now = datetime(2020, 1, 1)
    for i in range(96 * 10):            # 10 days
        t.add_data_point(now, 1)        # new data point
        now += timedelta(minutes=15)    # next timestamp
    db.data_create(t)                   # persist time series
    t2 = db.data_read_interval(sensor_name, "2020-01-01", "2021-01-01")
    t3 = db.data_read_rollup(sensor_name, "2020-01-01", "2021-01-01", "day", "sum")
    db.close()
```
