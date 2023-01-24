# vaa backfiller

reads a bulk csv dump of VAAs and upsert it into mongodb

## compile 

```bash
go build
```

## run 

```bash
./backfiller -file test.csv
```

## config

The mongodb uri is set via env using the variable `MONGODB_URI`
If is not set it will use the default `mongodb://localhost:27017/`



