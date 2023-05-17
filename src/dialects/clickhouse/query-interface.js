'use strict';

/**
 Returns an object that treats MySQL's inabilities to do certain queries.

 @class QueryInterface
 @static
 @private
 */

const _ = require('lodash');
const { QueryInterface } = require('../abstract/query-interface');
const UnknownConstraintError = require('../../errors').UnknownConstraintError;

class ClickHouseQueryInterface extends QueryInterface {
  async removeColumn(tableName, columnName, options) {
    options = options || {};

    // const results = await this.sequelize.query(
    //   this.queryGenerator.getForeignKeyQuery(tableName.tableName ? tableName : {
    //     tableName,
    //     schema: this.sequelize.config.database
    //   }, columnName),
    //   _.assign({ raw: true }, options)
    // );

    // if (results.length && results[0].constraint_name !== 'PRIMARY') {
    //   await Promise.all(results.map(result => this.sequelize.query(
    //     this.queryGenerator.dropForeignKeyQuery(tableName, result.constraint_name),
    //     { raw: true, ...options }
    //   )));
    // }

    return this.sequelize.query(
      this.queryGenerator.removeColumnQuery(tableName, columnName),
      _.assign({ raw: true }, options)
    );
  }

  async removeConstraint(tableName, constraintName, options) {
    const sql = this.queryGenerator.showConstraintsQuery(tableName.tableName ? tableName : {
      tableName,
      schema: this.sequelize.config.database
    }, constraintName);

    const constraints = await this.sequelize.query(
      sql, 
      { ...options, type: this.sequelize.QueryTypes.SHOWCONSTRAINTS }
    );
    const [constraint] = constraints;
    let query;
    
    if (constraint && constraint.constraintType) {
      if (constraint.constraintType === 'FOREIGN KEY') {
        query = this.queryGenerator.dropForeignKeyQuery(tableName, constraintName);
      } else {
        query = this.queryGenerator.removeIndexQuery(constraint.tableName, constraint.constraintName);
      }
    } else {
      throw new UnknownConstraintError(`Constraint ${constraintName} on table ${tableName} does not exist`);
    }

    return this.sequelize.query(query, options);
  }
}

exports.ClickHouseQueryInterface = ClickHouseQueryInterface;