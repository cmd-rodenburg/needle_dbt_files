# data-transformation
Everything to do with dbt and our Data Warehouse

Current dbt version: [">=1.0.0", "<1.2.0"]

# First time setup

> ⚠️ Run this before opening your IDE

## Windows 11
(Powershell)
1. Open the command prompt and navigate to your development folder where you wish to save this repository
 `git clone https://solytic@dev.azure.com/solytic/Gemma-Solytic/_git/data-transformation`
1. Add the following paths to the system variables "Path" \
    `C:\Python\Python38` \
    `C:\Python`

2. Create a new virtual environment using Python 3.8 \
`python -m venv .venv`


2. Activate the virtual environment \
  `.\.venv\Scripts\activate`

3. Install the required packages \
   - PIP \
  `pip install --upgrade pip`

   - DBT (Core and snowflake driver) \
  `pip install --upgrade dbt-core==1.2.0` \
  `pip install --upgrade dbt-snowflake==1.2.0`



4. Create a `profiles.yml` is in your dbt folder \
  linux: `~/.dbt/profiles.yml`  \
  Windows: `C:\users\username\.dbt\profiles.yml`
   1.  Create the folder using command prompt
         1.  navigate to your home folder (`~/` or `C:\users\username`) \
         `mkdir .dbt`
   2. Create `profiles.yml` in the .dbt folder
      1. Ensure the file is not `profiles.yml.txt`
   3. see below for a sample `profiles.yml` file content
    - Multiple profiles can be configured in the profiles.yml
    - To switch profile: `dbt run -t <profile name>` or `dbt run --target <profile name>`
- run your first dbt commands
  - `dbt deps` - install any dependent packages
  - `dbt seed` - upload any seed data, if there is any
  - `dbt snapshot` - snapshot any slowly changing data, if there is any
  - `dbt run` - execute dbt
  - `dbt test` - execute dbt tests
  - `dbt docs generate` - generate dbt lineage graph
  - `dbt docs serve` - show dbt lineage graph

### Sample profiles.yml file

```yaml
solytic_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: hv90342.north-europe.azure
      role: BI_DEVELOPER

      # User/password auth
      user: xxxx
      password: xxxx
      authenticator: username_password_mfa

      database: BI_SOLYTIC_DEV
      warehouse: BI_DEVELOPING
      schema: <> #Pick a schema name, preferably your first name
      threads: 4
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
