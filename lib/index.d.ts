/// <reference types="node" />
import { BigQuery as BQ, BigQueryOptions, Dataset, InsertRowsResponse, Table } from '@google-cloud/bigquery';
import EventEmitter from 'events';
/**
 * @class BigQuery
 */
declare class BigQuery {
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
    constructor(name: string, emitter: EventEmitter, config: BigQueryOptions);
    log(message: string, data?: Record<string, unknown>): void;
    success(message: string, data?: Record<string, unknown>): void;
    error(err: Error, data?: Record<string, unknown>): void;
    /**
     * Initialize the config for connecting to google apis
     */
    init(): Promise<BigQuery>;
    /**
     * Create a dataset, must be done before anything else, if not already exists.
     *
     * @param {string} datasetName - name of the dataset to use
     *
     * @return {Object} the dataset
     */
    createDataSet(datasetName: string): Promise<Dataset>;
    /**
     * Get a dataset, throw error if not present
     *
     * @param {string} datasetName - the name of dataset
     *
     * @return {Object} the dataset object
     * @throws {ReferenceError} dataset not found
     */
    getDataset(datasetName: string): Dataset;
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
    createTable(datasetName: string, tableName: string, schema?: Record<string, unknown>): Promise<Table>;
    /**
     * Get a table
     *
     * @param {string} datasetName - dataset name
     * @param {string} tableName - table name
     *
     * @return {Object} the table object
     * @throws {RefrenceError} table not found
     */
    getTable(datasetName: string, tableName: string): Table;
    /**
     * Streaming Insert
     *
     * @param {Object} rows - rows to insert into the table
     * @param {string} datasetName - dataset name
     * @param {string} tableName - table name
     *
     * @return {boolean}
     */
    insert(rows: Record<string, unknown>, datasetName: string, tableName: string): Promise<InsertRowsResponse>;
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
    query<T>(rawQuery: string, useLegacySql?: boolean, maximumBillingTier?: number): Promise<T[]>;
    /**
     * Create a query stream
     *
     * @param {string} query - string query
     * @param {boolean} [useLegacySql=false]
     * @param {number} [maximumBillingTier=3]
     *
     * @return {ReadStream}
     */
    queryStream(query: any, useLegacySql?: boolean, maximumBillingTier?: number): import("@google-cloud/paginator").ResourceStream<any>;
    /**
    * Check if update/delete is available on table
    *
    * @param {string} tableName - table to check
    * @param {string} datasetName - optional
    *
    * @return {boolean}
    */
    updateAvailable(tableName: string, datasetName: string): Promise<boolean>;
}
export = BigQuery;
