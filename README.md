# sas-convert
Simple SAS7BDAT to CSV conversion tool based on the [Parso library](http://lifescience.opensource.epam.com/parso.html)
and [opencsv](http://opencsv.sourceforge.net).

### Download
The latest version can be downloaded here:
[sas-convert-0.3.zip](https://github.com/thehyve/sas-convert/releases/download/0.3/sas-convert-0.3.zip).

```bash
# Download sas-convert
curl -L https://github.com/thehyve/sas-convert/releases/download/0.3/sas-convert-0.3.zip -o sas-convert-0.3.zip
unzip sas-convert-0.3.zip
```

### Usage
Usage:
```bash
./sas-convert/sas-convert <file.sas7bdat> <file.csv>
```

### Build and run from source
```bash
mvn package
./sas-convert <file.sas7bdat> <file.csv>
```
