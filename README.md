
### Setup
clone the repo
```sh
git clone git@github.com:lsissoko/sf-sanity-check.git
cd sf-sanity-check
```
create a `data/config.json` file with your Salesforce credentials:
```
{
  "salesforce": {
    "version": "",
    "clientId": "",
    "clientSecret": "",
    "clientPassword": "",
    "clientUserName": "",
    "salesforceUrl": ""
  }
}
```

### Run
Optional args

- `-l <limit>`  sets an upper limit on the number of implementations to fetch from Salesforce (default 200)
- `-o <offset>` skips a given number of implementations fetch from Salesforce (default 0)
- `--load`      loads saved data instead of querying Salesforce

```sh
python script.py [-l <limit>] [-o <offset>] [--load]
```
