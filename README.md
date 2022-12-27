# FlightRadarAnalysis

## Instructions:

use make to create a dev or prod virtual env:

```bash
make prod
```

```bash
make dev
```

then source it respectively with:

```bash
source .venv/bin/activate
```

```bash
source .devenv/bin/activate
```

build the pyspark job dependecies as a zip file package with

```bash
make build
```

then use the following command to submit it to the spark cluster:
 
```bash
make submit
```

