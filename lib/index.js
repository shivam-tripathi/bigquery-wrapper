"use strict";
const bigquery_1 = require("@google-cloud/bigquery");
/**
 * @class BigQuery
 */
class BigQuery {
    emitter;
    name;
    config;
    client;
    datasets;
    tables;
    /**
     * @param {string} name - unique name to this service
     * @param {EventEmitter} emitter
     * @param {Object} config - configuration object of service
     */
    constructor(name, emitter, config) {
        this.emitter = emitter;
        this.name = name;
        this.config = config;
        this.client = null;
        this.datasets = {};
        this.tables = {};
    }
    log(message, data) {
        this.emitter.emit('log', {
            service: this.name,
            message,
            data,
        });
    }
    success(message, data) {
        this.emitter.emit('success', {
            service: this.name, message, data,
        });
    }
    error(err, data) {
        this.emitter.emit('error', {
            service: this.name,
            data,
            err,
        });
    }
    /**
     * Initialize the config for connecting to google apis
     */
    init() {
        this.log('Using config', {
            projectId: this.config.projectId,
            email: this.config.credentials ? this.config.credentials.client_email : 'n/a',
            method: this.config.credentials ? 'PrivateKey' : 'KeyFile',
        });
        const client = new bigquery_1.BigQuery(this.config);
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
    createDataSet(datasetName) {
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
    getDataset(datasetName) {
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
    createTable(datasetName, tableName, schema = null) {
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
                const options = { schema };
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
    getTable(datasetName, tableName) {
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
    insert(rows, datasetName, tableName) {
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
    async query(rawQuery, useLegacySql = false, maximumBillingTier = 3) {
        const options = {
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
        const options = {
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
    updateAvailable(tableName, datasetName) {
        const table = this.getTable(datasetName, tableName);
        // check if table object has streamingBuffer section
        return table.getMetadata().then(result => {
            const metadata = result[0];
            if (metadata.streamingBuffer == null) {
                return true;
            }
            return false;
        });
    }
}
module.exports = BigQuery;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IjtBQUNBLHFEQUEwSjtBQUsxSjs7R0FFRztBQUNILE1BQU0sUUFBUTtJQUNaLE9BQU8sQ0FBZTtJQUN0QixJQUFJLENBQVM7SUFDYixNQUFNLENBQWtCO0lBQ3hCLE1BQU0sQ0FBSztJQUNYLFFBQVEsQ0FBMEI7SUFDbEMsTUFBTSxDQUF3QjtJQUU5Qjs7OztPQUlHO0lBQ0gsWUFBWSxJQUFZLEVBQUUsT0FBcUIsRUFBRSxNQUF1QjtRQUN0RSxJQUFJLENBQUMsT0FBTyxHQUFHLE9BQU8sQ0FBQztRQUN2QixJQUFJLENBQUMsSUFBSSxHQUFHLElBQUksQ0FBQztRQUNqQixJQUFJLENBQUMsTUFBTSxHQUFHLE1BQU0sQ0FBQztRQUNyQixJQUFJLENBQUMsTUFBTSxHQUFHLElBQUksQ0FBQztRQUNuQixJQUFJLENBQUMsUUFBUSxHQUFHLEVBQUUsQ0FBQztRQUNuQixJQUFJLENBQUMsTUFBTSxHQUFHLEVBQUUsQ0FBQztJQUNuQixDQUFDO0lBRUQsR0FBRyxDQUFDLE9BQWUsRUFBRSxJQUE4QjtRQUNqRCxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxLQUFLLEVBQUU7WUFDdkIsT0FBTyxFQUFFLElBQUksQ0FBQyxJQUFJO1lBQ2xCLE9BQU87WUFDUCxJQUFJO1NBQ0wsQ0FBQyxDQUFDO0lBQ0wsQ0FBQztJQUVELE9BQU8sQ0FBQyxPQUFlLEVBQUUsSUFBOEI7UUFDckQsSUFBSSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsU0FBUyxFQUFFO1lBQzNCLE9BQU8sRUFBRSxJQUFJLENBQUMsSUFBSSxFQUFFLE9BQU8sRUFBRSxJQUFJO1NBQ2xDLENBQUMsQ0FBQztJQUNMLENBQUM7SUFFRCxLQUFLLENBQUMsR0FBVSxFQUFFLElBQThCO1FBQzlDLElBQUksQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLE9BQU8sRUFBRTtZ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