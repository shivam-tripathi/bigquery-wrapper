
import { BigQuery as BQ, BigQueryOptions, Dataset, InsertRowsResponse, Job, Query, QueryOptions, QueryRowsResponse, Table } from '@google-cloud/bigquery';
import { CreateOptions, Metadata } from '@google-cloud/common/build/src/service-object';
import EventEmitter from 'events';
import { CredentialBody } from 'google-auth-library';

/**
 * @class BigQuery
 */
class BigQuery {
  emitter: EventEmitter;
  name: string;
  config: BigQueryOptions;
  client: BQ;
  datasets: Record<string, Dataset>;
  tables: Record<string, Table>;

  /**
   * @param {string} name - unique name to this service
   * @param {EventEmitter} emitter
   * @param {Object} config - configuration object of service
   */
  constructor(name: string, emitter: EventEmitter, config: BigQueryOptions) {
    this.emitter = emitter;
    this.name = name;
    this.config = config;
    this.client = null;
    this.datasets = {};
    this.tables = {};
  }

  log(message: string, data?: Record<string, unknown>) {
    this.emitter.emit('log', {
      service: this.name,
      message,
      data,
    });
  }

  success(message: string, data?: Record<string, unknown>) {
    this.emitter.emit('success', {
      service: this.name, message, data,
    });
  }

  error(err: Error, data?: Record<string, unknown>) {
    this.emitter.emit('error', {
      service: this.name,
      data,
      err,
    });
  }


  /**
   * Initialize the config for connecting to google apis
   */
  init(): Promise<BigQuery> {
    this.log('Using config', {
      projectId: this.config.projectId,
      email: this.config.credentials ? (this.config.credentials as CredentialBody).client_email : 'n/a',
      method: this.config.credentials ? 'PrivateKey' : 'KeyFile',
    });
    const client = new BQ(this.config);
    return client.getDatasets().then(() => {
      this.client = client;
      this.success(`Successfully connected on project ${this.config.projectId}`);
      return this;
    });
  }


  /**
   * Create a dataset, must be done before anything else, if not already exists.
   *
   * @param {string} datasetName - name of the dataset to use
   *
   * @return {Object} the dataset
   */
  createDataSet(datasetName: string): Promise<Dataset> {
    this.log(`Creating dataset ${datasetName}`);
    const dataset = this.client.dataset(datasetName);
    return dataset.exists().then(result => {
      return result[0];
    }).then(exists => {
      if (exists === false) {
        return dataset.create().then(() => {
          this.success(`Dataset "${datasetName}" created`);
          return true;
        });
      }
      this.success(`Dataset "${datasetName}" already exists`);
      return true;
    }).then(() => {
      this.datasets[datasetName] = dataset;
      return dataset;
    });
  }


  /**
   * Get a dataset, throw error if not present
   *
   * @param {string} datasetName - the name of dataset
   *
   * @return {Object} the dataset object
   * @throws {ReferenceError} dataset not found
   */
  getDataset(datasetName: string): Dataset {
    const dataset = this.datasets[datasetName];
    if (!dataset) {
      throw new ReferenceError(`Dataset "${datasetName}" not found`);
    }
    return dataset;
  }


  /**
   * Create a table, must be done after creating dataset
   *
   * * @param {string} datasetName - dataset name
   * @param {string} tableName - name of the table to use
   * @param  {Object} schema - a comma-separated list of name:type pairs.
   *   Valid types are "string", "integer", "float", "boolean", and "timestamp".
   *
   * @return {boolean}
   */
  createTable(datasetName: string, tableName: string, schema: Record<string, unknown> = null): Promise<Table> {
    this.log(`Creating table ${tableName} in ${datasetName}`);
    // check if table exists
    const dataset = this.getDataset(datasetName);
    const table = dataset.table(tableName);
    return table.exists().then(result => {
      return result[0];
    }).then(exists => {
      if (exists === false && !schema) {
        throw new ReferenceError(`"${datasetName}.${tableName}" does not exists and no schema was given`);
      }
      if (exists === false && schema) {
        const options: CreateOptions = { schema };
        return table.create(options).then(() => {
          this.success(`Table "${datasetName}.${tableName}" created`);
          return true;
        });
      }
      this.success(`Table "${datasetName}.${tableName}" already exists`);
      return true;
    }).then(() => {
      this.tables[`${datasetName}.${tableName}`] = table;
      return table;
    });
  }


  /**
   * Get a table
   *
   * @param {string} datasetName - dataset name
   * @param {string} tableName - table name
   *
   * @return {Object} the table object
   * @throws {RefrenceError} table not found
   */
  getTable(datasetName: string, tableName: string): Table {
    const table = this.tables[`${datasetName}.${tableName}`];
    if (!table) {
      throw new ReferenceError(`Table "<${tableName}>" not found in "<${datasetName}>"`);
    }
    return table;
  }


  /**
   * Streaming Insert
   *
   * @param {Object} rows - rows to insert into the table
   * @param {string} datasetName - dataset name
   * @param {string} tableName - table name
   *
   * @return {boolean}
   */
  insert(rows: Record<string, unknown>, datasetName: string, tableName: string): Promise<InsertRowsResponse> {
    // check if tableName valid
    const table = this.getTable(datasetName, tableName);
    // Insert into table
    return table.insert(rows).catch(ex => {
      if (ex.name === 'PartialFailureError') {
        throw new Error(JSON.stringify(ex, null, 2));
      }
      throw ex;
    });
  }


  /**
   * Run Query scoped to the dataset
   *
   * @param {string} rawQuery - direct query to run
   *                               Warning: check for SQL injection when accepting input.
   * @param {boolean} [useLegacySql=false]
   * @param {number} [maximumBillingTier=3]
   *
   * @return {Array} rows
   */
  async query<T>(rawQuery: string, useLegacySql = false, maximumBillingTier = 3): Promise<T[]> {
    const options: Query = {
      query: rawQuery,
      useLegacySql,
      maximumBillingTier,
    };
    let [job] = await this.client.createQueryJob(options);
    const results = await job.getQueryResults()
      .catch((e) => {
        this.error(e); // eslint-disable-line
        throw new Error(e.message);
      });
    const [rows, _, response] = results;
    if (Array.isArray(response.errors)) {
      throw new Error(response.errors.map(err => err.message).join(";"));
    }
    if (!response.jobComplete) {
      throw new Error('Job not complete: Might need to implement pagination');
    }
    this.log(`Query Successful: ${rawQuery}`);
    return rows;
  }


  /**
   * Create a query stream
   *
   * @param {string} query - string query
   * @param {boolean} [useLegacySql=false]
   * @param {number} [maximumBillingTier=3]
   *
   * @return {ReadStream}
   */
  queryStream(query, useLegacySql = false, maximumBillingTier = 3) {
    const options: Query = {
      query,
      useLegacySql,
      maximumBillingTier,
    };

    return this.client.createQueryStream(options);
  }


  /**
  * Check if update/delete is available on table
  *
  * @param {string} tableName - table to check
  * @param {string} datasetName - optional
  *
  * @return {boolean}
  */
  updateAvailable(tableName: string, datasetName: string) {
    const table = this.getTable(datasetName, tableName);
    // check if table object has streamingBuffer section
    return table.getMetadata().then(result => {
      const metadata: Metadata = result[0];
      if (metadata.streamingBuffer == null) {
        return true;
      }
      return false;
    });
  }
}

export = BigQuery;
