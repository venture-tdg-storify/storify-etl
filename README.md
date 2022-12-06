# Arteli ETL 

### Prerequisites 

- Follow [these instructions](https://docs.getdbt.com/docs/get-started/installation) to install `dbt Core` on your development environment. 
- Follow [these instructions](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup) to install the `dbt-snowflake` adapter on your development environment. 
- Run `dbt deps` to install dependencies defined in `packages.yml`
- Run `dbt --version` to check the installed version of `dbt Core` and `dbt Snowflake` adapter.

### Configure dbt Profile

- For local development, dbt Core projects will reference the `~/.dbt/profiles.yml` file for connectivity between dbt Core and Snowflake
- To configure your dbt Core profile for the Storify ETL project, do the following:
  - Open a Terminal window
  - Browse to your project folder; e.g.: `~/_arteli/storify-etl`
  - Run `dbt init` and follow the prompts to connect to Snowflake:
    - dbt Project Name: `arteli-etl`
    - Account: `QNDVSNG-CI89152`
    - Warehouse: `compute_wh`
    - Database: `ARTELI_ANALYTICS_DEV`
    - Schema: DEFINE_YOUR_TEST_SCHEMA
    - User: YOUR_USER 
    - Password: YOUR_USER
    - Role: ARTELI_ANALYTICS_DEV_ROLE

### Running dbt Transformations

- To run a full refresh, run `dbt run --full-refresh`

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
- Check out [dbt-utils](https://github.com/dbt-labs/dbt-utils) documentation
