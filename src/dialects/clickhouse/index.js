'use strict';

const _ = require('lodash');
const AbstractDialect = require('../abstract');
const ConnectionManager = require('./connection-manager');
const Query = require('./query');
const ClickHouseQueryGenerator = require('./query-generator');
const DataTypes = require('../../data-types').clickhouse;
const { ClickHouseQueryInterface } = require('./query-interface');

class ClickHouseDialect extends AbstractDialect {
  constructor(sequelize) {
    super();
    this.sequelize = sequelize;
    this.connectionManager = new ConnectionManager(this, sequelize);
    this.queryGenerator = new ClickHouseQueryGenerator({
      _dialect: this,
      sequelize
    });
    
    this.queryInterface = new ClickHouseQueryInterface(sequelize, this.queryGenerator);
  }
}

ClickHouseDialect.prototype.supports = _.merge(_.cloneDeep(AbstractDialect.prototype.supports), {
  'VALUES ()': true,
  'LIMIT ON UPDATE': false,
  'IGNORE': ' IGNORE',
  lock: true,
  forShare: 'LOCK IN SHARE MODE',
  index: {
    collate: false,
    length: true,
    parser: true,
    type: true,
    using: 1
  },
  constraints: {
    dropConstraint: false,
    check: false
  },
  ignoreDuplicates: ' IGNORE',
  updateOnDuplicate: false,
  indexViaAlter: true,
  NUMERIC: true,
  GEOMETRY: true,
  JSON: true,
  REGEXP: true
});

ClickHouseDialect.prototype.defaultVersion = '5.6.0';
ClickHouseDialect.prototype.Query = Query;
ClickHouseDialect.prototype.DataTypes = DataTypes;
ClickHouseDialect.prototype.name = 'clickhouse';
ClickHouseDialect.prototype.TICK_CHAR = '`';
ClickHouseDialect.prototype.TICK_CHAR_LEFT = ClickHouseDialect.prototype.TICK_CHAR;
ClickHouseDialect.prototype.TICK_CHAR_RIGHT = ClickHouseDialect.prototype.TICK_CHAR;

module.exports = ClickHouseDialect;