'use strict';

const _ = require('lodash');
const AbstractConnectionManager = require('../abstract/connection-manager');
const SequelizeErrors = require('../../errors');
const { logger } = require('../../utils/logger');
const debug = logger.debugContext('connection:clickhouse');
const Datatypes = require('../../data-types').clickhouse;
const momentTz = require('moment-timezone');
const parseMap = new Map();
const { promisify } = require('util');

class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    super(dialect, sequelize);
    
    this.sequelize = sequelize;
    this.sequelize.config.database = this.sequelize.config.database || 'default';
    this.sequelize.config.username = this.sequelize.config.username || 'default';
    this.sequelize.config.password = this.sequelize.config.password || '';
    this.sequelize.config.port = this.sequelize.config.port || 8123;
    this.sequelize.config.host = this.sequelize.config.host || 'localhost';
    this.sequelize.config.timezone = this.sequelize.config.timezone || momentTz.tz.guess();

    try {
      if (sequelize.config.dialectModulePath) {
        this.lib = require(sequelize.config.dialectModulePath).ClickHouse;
      } else {
        this.lib = require('clickhouse');
      }
    } catch (error) {
      if (error.code === 'MODULE_NOT_FOUND') {
        throw new Error('Please install clickhouse package manually');
      }

      throw error;
    }

    this.refreshTypeParser(Datatypes);
  }

  _refreshTypeParser(dataType) {
    for (const type of dataType.types.clickhouse) {
      parseMap.set(type, dataType.parse);
    }
  }

  _clearTypeParser() {
    parseMap.clear();
  }

  static _typecast(field, next) {
    if (parseMap.has(field.type)) {
      return parseMap.get(field.type)(field, this.sequelize.options, next);
    }

    return next();
  }

  async connect(config) {
    const connectionConfig = {
      url: config.host || config.url,
      port: config.port || this.sequelize.config.port,
      user: config.username || this.sequelize.config.username,
      flags: '-FOUND_ROWS',
      password: config.password || this.sequelize.config.password,
      database: config.database || this.sequelize.config.database,
      timezone: config.timezone || this.sequelize.config.timezone,
      typeCast: ConnectionManager._typecast.bind(this),
      bigNumberStrings: false,
      supportBigNumbers: true,
      debug: config.debug || this.sequelize.config.debug,
      reqParams: config.reqParams || this.sequelize.config.reqParams || {}
    };

    if (config.dialectOptions) {
      Object.entries(config.dialectOptions).forEach(([key, value]) => {
        connectionConfig[key] = value;
      });
    }

    const connection = await new Promise(resolve => {
      const cconnection = new this.lib.ClickHouse(connectionConfig);
      resolve(cconnection);
    }).catch(err => {
      switch (err.code) {
        case 'ECONNREFUSED':
          throw new SequelizeErrors.ConnectionRefusedError(err);
        case 'ER_ACCESS_DENIED_ERROR':
          throw new SequelizeErrors.AccessDeniedError(err);
        case 'ENOTFOUND':
          throw new SequelizeErrors.HostNotFoundError(err);
        case 'EHOSTUNREACH':
          throw new SequelizeErrors.HostNotReachableError(err);
        case 'EINVAL':
          throw new SequelizeErrors.InvalidConnectionError(err);
        default: 
          throw new SequelizeErrors.ConnectionError(err);
      }
    });

    debug('connection acquired');

    return connection;
  }

  async disconnect(_connection) {
    return null;
  }
}

_.extend(ConnectionManager.prototype, AbstractConnectionManager.prototype);

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
