### Update dependencies
go mod download

### Clear cache
go clean -cache -modcache -i -r

### Clean Table(s)
pg-backup --profile=development --config=sample.json --clean-tables

### Export Data
pg-backup --profile=local --config=sample.json --export=raw

pg-backup --profile=local --config=sample.json --export=csv

pg-backup --profile=local --config=sample.json --export=json --obscure

pg-backup --profile=custom:production --config=sample.json --export=json --obscure

### Import Data
pg-backup --profile=development --config=sample.json --bulkFile=organizations.raw --import

pg-backup --profile=development --config=sample.json --bulkFile=organizations.json --import

pg-backup --profile=development --config=sample.json --bulkFile=organizations-csv.zip --import

pg-backup --profile=development --config=sample.json --bulkFile=organizations.raw --import --clean-tables


### Obscure pattern
- Obscure Primary Document service masks (obscures) the content of the primary document by replacing the original content with an unintelligible version of the original content.

[**PATTERN**]{**MIN**,**MAX**}  or [**PATTERN**]{**LENGTH**}

**MIN** - The preceding item is matched at least n times (If value set to zero it may result in nil/null)

**MAX** - The preceding item is matched no more than n times.

**LENGTH** - The preceding item is matched exactly n times.

**PATTERN**:

> A-Z - All upper case

>a-z - All lower case

>A-z - Capitalize

>0-9 - Numbers

>[any characters]

#### Example:
_**[A-z]**{3,50} **[A-Z]**{0,50}_
```
    Hello WORLD
    Hello
```


 _**[a-z]**{5,20}**[-]**{0,1}**[A-Z]**{0,20}_
```
    hello-WORLD
    hello
```


_**[+]**{0,1}**[0-9]**{1,3}_
```
    (nil)
    +1
    +231
```


_**[A]**{0,1}**[0-9]**{9}_
```
    (nil)
    A123456789
```

