# `soql-redshift-adapter`

This repository [implements](store-redshift/src/main/scala/com/socrata/service/RedshiftSecondary.scala) a [secondary](https://github.com/socrata-platform/data-coordinator/blob/405523e986467c14d5079522f25e8ff18de1d811/coordinatorlib/src/main/scala/com/socrata/datacoordinator/secondary/Secondary.scala#L16) known as `soql-redshift-adaptor`.

`soql-redshift-adaptor` receives [dataset updates](https://github.com/socrata-platform/data-coordinator/blob/405523e986467c14d5079522f25e8ff18de1d811/coordinatorlib/src/main/scala/com/socrata/datacoordinator/secondary/Secondary.scala#L40C3-L40C3) (i.e., [version changes](https://github.com/socrata-platform/data-coordinator/blob/405523e986467c14d5079522f25e8ff18de1d811/coordinatorlib/src/main/scala/com/socrata/datacoordinator/secondary/Secondary.scala#L7)) from `truth` via the `secondary` interface and writes them to [Redshift](https://aws.amazon.com/redshift/) tables.

In addition, `query-coordinator` sends [`soql`](https://github.com/socrata-platform/soql-reference) queries to `soql-redshift-server`, which transforms that `soql` to `SQL`, and executes those queries against Redshift.

In somewhat imprecise  terms, this project integrates AWS's Redshift offering into our data pipelines and query ecosystem. This integration allows customer datasets to exist in Redshift, be kept up to date with `truth`, and be queried via `soql`.

## Context
`truth` stores a canonical representation of our datasets. In turn, secondaries store representations of datasets, originating from `truth`.

[`query-coordinator`](https://github.com/socrata-platform/query-coordinator) serves the read path for `truth`; Such reads always occur through the proxy of secondaries.

_Data Story:_
1. [`core`](https://github.com/socrata/core) receives a `soql` query from a customer. This `soql` query is a string.

2. `core` transmits the query to `soda-fountain`.

3. `soda-fountain` transmits the query to `query-coordinator`.

4. `query-coordinator` transforms the query string into a `soql` [analysis](https://github.com/socrata/soql-reference/blob/main/soql-analyzer/src/main/scala/com/socrata/soql/analyzer2/SoQLAnalysis.scala#L11) _and then_ finds the secondary where the relevant dataset is located.

5. `query-coordinator` sends the `soql` analysis to the chosen secondary.

6. the secondary transforms `soql` to `SQL`, then executes those queries against Redshift.

7. The resulting data percolates back up this call chain to `core`, communicating results of the query to the customer.

[`data-coordinator`](https://github.com/socrata-platform/data-coordinator) fulfils the write requirements for `truth`.

By far, [`soql-postgres-adapter`](https://github.com/socrata-platform/soql-postgres-adapter) is the most trafficked secondary. However, this Redshift implementation is useful for very large datasets. Postgres performance fails to meet business requirements for extremely large datasets (e.g., tables with around ten million rows).

## Use
This section provides an entry point to using this project.

### Running the project
This project cannot run fully locally since Redshift is hosted by AWS.

Running either the `server` or the `adapter` subprojects requires VPN access in order to communicate with a (AWS) hosted Redshift cluster.

#### Dependencies

In order to run this project requires:
- Java (version 17)
- Maven
- Scala (2.12)

#### Installation

Install as such:

```bash
mvn install
```
_NOTE_: New installations of Maven require running the [`rotate-my-artifactory`](https://github.com/socrata/junk-drawer/blob/main/artifactory/rotate-my-artifactory) script, as described in developer onboarding documentation.

Be sure to check that `last-pass` stored credentials are included in your local `~/.m2/settings.xml` document. These credentials are necessary to pull company-private artifacts needed for installation.

### Testing

Tests may be run with project and test-module level specificity:

```bash
mvn install  # Necessary
mvn test -DskipTests=false \
         -Dtest="com.socrata.store.sqlizer.FunctionSqlizerTest" \
         -pl store-redshift
```
