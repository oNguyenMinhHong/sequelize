'use strict';

const _ = require('lodash');
const Utils = require('../../utils');
const AbstractQueryGenerator = require('../abstract/query-generator');
const util = require('util');
const Model = require('../../model');
const Op = require('../../operators');
const DataTypes = require('../../data-types');

class ClickHouseQueryGenerator extends AbstractQueryGenerator {
  static OperatorMap = { 
    ...AbstractQueryGenerator.OperatorMap, 
    [Op.regexp]: 'match',
    [Op.notRegexp]: 'NOT REGEXP'
  };

  createDatabaseQuery(databaseName, options) {
    options = options || {};

    let query = `CREATE DATABASE IF NOT EXISTS ${this.quoteIdentifier(databaseName)}`;
    if (options.cluster) {
      query += `ON CLUSTER ${this.quoteIdentifier(options.cluster)}`;
    }

    if (options.engine) {
      query += `ENGINE = ${options.engine}`;
    }

    if (options.comment) {
      query += `COMMENT = ${this.escape(options.comment)}`;
    }

    return `${query};`;
  }

  tableExistsQuery(tableName) {
    const table = tableName.tableName || tableName;
    const schema = tableName.schema || 'public';

    return `SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ${this.escape(schema)} AND table_name = ${this.escape(table)}`;
  }

  dropDatabaseQuery(databaseName) {
    return `DROP DATABASE IF EXISTS ${this.quoteIdentifier(databaseName)} SYNC;`;
  }

  createSchema() {
    return 'SHOW TABLES';
  }

  showSchemasQuery() {
    return 'SHOW TABLES';
  }

  versionQuery() {
    return 'SELECT version() as `version`';
  }

  createTableQuery(tableName, attributes, options) {
    options = _.extend({
      engine: 'MergeTree()',
      charset: null,
      rowFormat: null,
      indexGranularity: 0,
      orderBy: [],
      partitionBy: [],
      primaryKey: []
    }, options || {});

    let query = 'CREATE TABLE IF NOT EXISTS <%= table %> (<%= attributes%>) ENGINE=<%= engine %>';

    if (options.partitionBy) {
      if (options.partitionBy.length === 0) {
        query += ' PARTITION BY tuple()';
      } else {
        query += ` PARTITION BY (${options.partitionBy.join(',')})`;
      }
    }

    if (options.orderBy) {
      if (options.orderBy.length === 0) {
        query += ' ORDER BY tuple()';
      } else {
        query += ` ORDER BY (${options.orderBy.join(',')})`;
      }
    }

    if (options.primaryKey.length > 0 && !_.isEqual(options.orderBy, options.primaryKey)) {
      query += ` PRIMARY KEY (${options.primaryKey.join(',')})`;
    }

    const primaryKeys = [];
    const foreignKeys = {};
    const attrStr = [];

    for (const attr in attributes) {
      if (Object.prototype.hasOwnProperty.call(attributes, attr)) {
        const dataType = attributes[attr];

        attrStr.push(`${this.quoteIdentifier(attr)} ${dataType}`);
      }
    }

    const values = {
      table: this.quoteTable(tableName),
      attributes: attrStr.join(', '),
      comment: options.comment && _.isString(options.comment) ? ` COMMENT ${this.escape(options.comment)}` : '',
      engine: options.engine,
      charset: options.charset ? ` DEFAULT CHARSET=${options.charset}` : '',
      collation: options.collate ? ` COLLATE ${options.collate}` : '',
      rowFormat: options.rowFormat ? ` ROW_FORMAT=${options.rowFormat}` : '',
      initialAutoIncrement: options.initialAutoIncrement ? ` AUTO_INCREMENT=${options.initialAutoIncrement}` : ''
    };
    const pkString = primaryKeys.map(pk => this.quoteIdentifier(pk)).join(', ');

    if (options.uniqueKeys) {
      _.each(options.uniqueKeys, (columns, indexName) => {
        if (columns.customIndex) {
          if (!_.isString(indexName)) {
            indexName = `uniq_${tableName}_${columns.fields.join('_')}`;
          }
          values.attributes += `, UNIQUE ${this.quoteIdentifier(indexName)} (${columns.fields.map(field => this.quoteIdentifier(field)).join(', ')})`;
        }
      });
    }

    if (pkString.length > 0) {
      values.attributes += `, PRIMARY KEY (${pkString})`;
    }

    return `${_.template(query, this._templateSettings)(values).trim()};`;
  }

  showTablesQuery() {
    return 'SHOW TABLES;';
  }

  addColumnQuery(table, key, dataType) {
    const definition = this.attributeToSQL(dataType, {
      context: 'addColumn',
      tableName: table,
      foreignKey: key
    });

    return `ALTER TABLE ${this.quoteTable(table)} ADD COLUMN ${this.quoteIdentifier(key)} ${definition};`;
  }

  removeColumnQuery(tableName, attributeName) {
    return `ALTER TABLE ${this.quoteTable(tableName)} DROP COLUMN ${this.quoteIdentifier(attributeName)};`;
  }

  changeColumnQuery(tableName, attributes) {
    const attrString = [];
    const constraintString = [];

    for (const attributeName in attributes) {
      let definition = attributes[attributeName];
      if (definition.match(/REFERENCES/)) {
        const fkName = this.quoteIdentifier(`${tableName}_${attributeName}_foreign_idx`);
        const attrName = this.quoteIdentifier(attributeName);
        definition = definition.replace(/.+?(?=REFERENCES)/, '');
        constraintString.push(`${fkName} FOREIGN KEY (${attrName}) ${definition}`);
      } else {
        attrString.push(`\`${attributeName}\` \`${attributeName}\` ${definition}`);
      }
    }

    let finalQuery = '';
    if (attrString.length) {
      finalQuery += `CHANGE ${attrString.join(', ')}`;
      finalQuery += constraintString.length ? ' ' : '';
    }
    if (constraintString.length) {
      finalQuery += `ADD CONSTRAINT ${constraintString.join(', ')}`;
    }

    return `ALTER TABLE ${this.quoteTable(tableName)} ${finalQuery};`;
  }

  renameColumnQuery(tableName, attrBefore, attributes) {
    const attrString = [];

    for (const attrName in attributes) {
      const definition = attributes[attrName];
      attrString.push(`\`${attrBefore}\` \`${attrName}\` ${definition}`);
    }

    return `ALTER TABLE ${this.quoteTable(tableName)} CHANGE ${attrString.join(', ')};`;
  }

  handleSequelizeMethod(smth, tableName, factory, options, prepend) {
    if (smth instanceof Utils.Json) {

      if (smth.conditions) {
        const conditions = _.map(this.parseConditionObject(smth.conditions), condition =>
          `${this.quoteIdentifier(_.first(condition.path))}->>'$.${_.tail(condition.path).join('.')}' = '${condition.value}'`
        );

        return conditions.join(' and ');
      } if (smth.path) {
        let str;


        if (this._checkValidJsonStatement(smth.path)) {
          str = smth.path;
        } else {

          let path = smth.path;
          let startWithDot = true;


          path = path.replace(/\.(\d+)\./g, '[$1].');

          path = path.replace(/\.(\d+)$/, '[$1]');

          path = path.split('.');

          let columnName = path.shift();
          const match = columnName.match(/\[\d+\]$/);

          if (match !== null) {
            path.unshift(columnName.substr(match.index));
            columnName = columnName.substr(0, match.index);
            startWithDot = false;
          }

          str = `${this.quoteIdentifier(columnName)}->>'$${startWithDot ? '.' : ''}${path.join('.')}'`;
        }

        if (smth.value) {
          str += util.format(' = %s', this.escape(smth.value));
        }

        return str;
      }
    } else if (smth instanceof Utils.Cast) {
      if (/timestamp/i.test(smth.type)) {
        smth.type = 'datetime';
      } else if (smth.json && /boolean/i.test(smth.type)) {

        smth.type = 'char';
      } else if (/double precision/i.test(smth.type) || /boolean/i.test(smth.type) || /integer/i.test(smth.type)) {
        smth.type = 'decimal';
      } else if (/text/i.test(smth.type)) {
        smth.type = 'char';
      }
    }

    return super.handleSequelizeMethod(smth, tableName, factory, options, prepend);
  }

  _toJSONValue(value) {

    if (typeof value === 'boolean') {
      return value.toString();
    }

    if (value === null) {
      return 'null';
    }
    return value;
  }

  addIndexQuery(tableName, attributes, options, rawTablename) {
    options = options || {};

    if (!Array.isArray(attributes)) {
      options = attributes;
      attributes = undefined;
    } else {
      options.fields = attributes;
    }

    options.prefix = options.prefix || rawTablename || tableName;
    if (options.prefix && typeof options.prefix === 'string') {
      options.prefix = options.prefix.replace(/\./g, '_');
      options.prefix = options.prefix.replace(/("|')/g, '');
    }

    const fieldsSql = options.fields.map(field => {
      if (field instanceof Utils.SequelizeMethod) {
        return this.handleSequelizeMethod(field);
      }
      if (typeof field === 'string') {
        field = {
          name: field
        };
      }
      let result = '';

      if (field.attribute) {
        field.name = field.attribute;
      }

      if (!field.name) {
        throw new Error(`The following index field has no name: ${util.inspect(field)}`);
      }

      result += this.quoteIdentifier(field.name);

      if (this._dialect.supports.index.collate && field.collate) {
        result += ` COLLATE ${this.quoteIdentifier(field.collate)}`;
      }

      if (this._dialect.supports.index.operator) {
        const operator = field.operator || options.operator;
        if (operator) {
          result += ` ${operator}`;
        }
      }

      if (this._dialect.supports.index.length && field.length) {
        result += `(${field.length})`;
      }

      if (field.order) {
        result += ` ${field.order}`;
      }

      return result;
    });

    if (!options.name) {
      // Mostly for cases where addIndex is called directly by the user without an options object (for example in migrations)
      // All calls that go through sequelize should already have a name
      options = Utils.nameIndex(options, options.prefix);
    }

    options = Model._conformIndex(options);

    if (!this._dialect.supports.index.type) {
      delete options.type;
    }

    if (options.where) {
      options.where = this.whereQuery(options.where);
    }

    if (typeof tableName === 'string') {
      tableName = this.quoteIdentifiers(tableName);
    } else {
      tableName = this.quoteTable(tableName);
    }

    const concurrently = this._dialect.supports.index.concurrently && options.concurrently ? 'CONCURRENTLY' : undefined;
    let ind;
    if (this._dialect.supports.indexViaAlter) {
      ind = [
        'ALTER TABLE',
        tableName,
        concurrently,
        'ADD'
      ];
    } else {
      ind = ['CREATE'];
    }

    ind = ind.concat(
      'INDEX',
      !this._dialect.supports.indexViaAlter ? concurrently : undefined,
      this.quoteIdentifiers(options.name),
      this._dialect.supports.index.using === 1 && options.using ? `USING ${options.using}` : '',
      !this._dialect.supports.indexViaAlter ? `ON ${tableName}` : undefined,
      this._dialect.supports.index.using === 2 && options.using ? `USING ${options.using}` : '',
      `(${fieldsSql.join(', ')})`,
      this._dialect.supports.index.parser && options.parser ? `WITH PARSER ${options.parser}` : undefined,
      this._dialect.supports.index.where && options.where ? options.where : undefined,
      options.type ? `TYPE ${options.type}` : undefined 
    );

    return _.compact(ind).join(' ');
  }

  upsertQuery(tableName, insertValues, updateValues, where, model, options) {
    options.onDuplicate = 'UPDATE ';

    options.onDuplicate += Object.keys(updateValues).map(key => {
      key = this.quoteIdentifier(key);
      return `${key}=VALUES(${key})`;
    }).join(', ');

    return this.insertQuery(tableName, insertValues, model.rawAttributes, options);
  }

  updateQuery(tableName, attrValueHash, where, options, attributes) {
    options = options || {};
    _.defaults(options, this.options);

    attrValueHash = Utils.removeNullValuesFromHash(attrValueHash, options.omitNull, options);

    const values = [];
    const bind = [];
    const modelAttributeMap = {};
    let outputFragment = '';
    let suffix = '';

    if (_.get(this, ['sequelize', 'options', 'dialectOptions', 'prependSearchPath']) || options.searchPath) {
      options.bindParam = false;
    }

    const bindParam = options.bindParam === undefined ? this.bindParam(bind) : options.bindParam;

    if (this._dialect.supports.returnValues && options.returning) {
      const returnValues = this.generateReturnValues(attributes, options);

      suffix += returnValues.returningFragment;
      outputFragment = returnValues.outputFragment || '';

      if (!this._dialect.supports.returnValues.output && options.returning) {
        options.mapToModel = true;
      }
    }

    if (attributes) {
      _.each(attributes, (attribute, key) => {
        modelAttributeMap[key] = attribute;
        if (attribute.field) {
          modelAttributeMap[attribute.field] = attribute;
        }
      });
    }

    for (const key in attrValueHash) {
      if (modelAttributeMap && modelAttributeMap[key] &&
        modelAttributeMap[key].autoIncrement === true &&
        !this._dialect.supports.autoIncrement.update) {
        // not allowed to update identity column
        continue;
      }

      const value = attrValueHash[key];

      if (value instanceof Utils.SequelizeMethod || options.bindParam === false) {
        values.push(`${this.quoteIdentifier(key)}=${this.escape(value, modelAttributeMap && modelAttributeMap[key] || undefined, { context: 'UPDATE' })}`);
      } else {
        values.push(`${this.quoteIdentifier(key)}=${this.format(value, modelAttributeMap && modelAttributeMap[key] || undefined, { context: 'UPDATE' }, bindParam)}`);
      }
    }

    const whereOptions = { ...options, bindParam };

    if (values.length === 0) {
      return '';
    }

    const query = `ALTER TABLE ${this.quoteTable(tableName)} UPDATE ${values.join(',')}${outputFragment} ${this.whereQuery(where, whereOptions)}${suffix}`.trim();
  
    const result = { query };
    if (options.bindParam !== false) {
      result.bind = bind;
    }
    return result;
  }

  deleteQuery(tableName, where, options, model) {
    options = options || {};

    const table = this.quoteTable(tableName);
    if (options.truncate === true) {

      return `TRUNCATE ${table}`;
    }

    where = this.getWhereConditions(where, null, model, options);
    let limit = '';

    if (_.isUndefined(options.limit)) {
      options.limit = 1;
    }

    if (options.limit) {
      limit = ` LIMIT ${this.escape(options.limit)}`;
    }

    let query = `DELETE FROM ${table}`;
    if (where) query += ` WHERE ${where}`;
    query += limit;

    return query;
  }

  showIndexesQuery(tableName, options) {
    return `SHOW INDEX FROM ${this.quoteTable(tableName)}${(options || {}).database ? ` FROM \`${options.database}\`` : ''}`;
  }

  showConstraintsQuery(table, constraintName) {
    const tableName = table.tableName || table;
    const schemaName = table.schema;

    let sql = [
      'SELECT CONSTRAINT_CATALOG AS constraintCatalog,',
      'CONSTRAINT_NAME AS constraintName,',
      'CONSTRAINT_SCHEMA AS constraintSchema,',
      'CONSTRAINT_TYPE AS constraintType,',
      'TABLE_NAME AS tableName,',
      'TABLE_SCHEMA AS tableSchema',
      'from INFORMATION_SCHEMA.TABLE_CONSTRAINTS',
      `WHERE table_name='${tableName}'`
    ].join(' ');

    if (constraintName) {
      sql += ` AND constraint_name = '${constraintName}'`;
    }

    if (schemaName) {
      sql += ` AND TABLE_SCHEMA = '${schemaName}'`;
    }

    return `${sql};`;
  }

  removeIndexQuery(tableName, indexNameOrAttributes) {
    let indexName = indexNameOrAttributes;

    if (typeof indexName !== 'string') {
      indexName = Utils.underscore(`${tableName}_${indexNameOrAttributes.join('_')}`);
    }

    return `ALTER TABLE ${this.quoteTable(tableName)} DROP INDEX ${this.quoteIdentifier(indexName)}`;
  }

  attributeToSQL(attribute, options) {
    if (!_.isPlainObject(attribute)) {
      attribute = {
        type: attribute
      };
    }

    let attributeString = attribute.type.toString({ escape: this.escape.bind(this) });
    let template = attributeString;

    if (attribute.allowNull === false) {
      template = attributeString;
    } else if (attribute.type instanceof DataTypes.ARRAY) {
      attributeString = attribute.type.type.toString({ escape: this.escape.bind(this) });
      template = `Array(Nullable(${attributeString}))`;
    } else {
      template = `Nullable(${template})`;
    }

    return template;
  }

  attributesToSQL(attributes, options) {
    const result = {};

    for (const key in attributes) {
      const attribute = attributes[key];
      result[attribute.field || key] = this.attributeToSQL(attribute, options);
    }

    return result;
  }

  quoteIdentifier(identifier) {
    if (identifier === '*') return identifier;
    return Utils.addTicks(Utils.removeTicks(identifier, '`'), '`');
  }

  /**
   * Check whether the statmement is json function or simple path
   *
   * @param   {string}  stmt  The statement to validate
   * @returns {boolean}       true if the given statement is json function
   * @throws  {Error}         throw if the statement looks like json function but has invalid token
   * @private
   */
  _checkValidJsonStatement(stmt) {
    if (!_.isString(stmt)) {
      return false;
    }

    const jsonFunctionRegex = /^\s*((?:[a-z]+_){0,2}jsonb?(?:_[a-z]+){0,2})\([^)]*\)/i;
    const jsonOperatorRegex = /^\s*(->>?|@>|<@|\?[|&]?|\|{2}|#-)/i;
    const tokenCaptureRegex = /^\s*((?:([`"'])(?:(?!\2).|\2{2})*\2)|[\w\d\s]+|[().,;+-])/i;

    let currentIndex = 0;
    let openingBrackets = 0;
    let closingBrackets = 0;
    let hasJsonFunction = false;
    let hasInvalidToken = false;

    while (currentIndex < stmt.length) {
      const string = stmt.substr(currentIndex);
      const functionMatches = jsonFunctionRegex.exec(string);
      if (functionMatches) {
        currentIndex += functionMatches[0].indexOf('(');
        hasJsonFunction = true;
        continue;
      }

      const operatorMatches = jsonOperatorRegex.exec(string);
      if (operatorMatches) {
        currentIndex += operatorMatches[0].length;
        hasJsonFunction = true;
        continue;
      }

      const tokenMatches = tokenCaptureRegex.exec(string);
      if (tokenMatches) {
        const capturedToken = tokenMatches[1];
        if (capturedToken === '(') {
          openingBrackets++;
        } else if (capturedToken === ')') {
          closingBrackets++;
        } else if (capturedToken === ';') {
          hasInvalidToken = true;
          break;
        }
        currentIndex += tokenMatches[0].length;
        continue;
      }

      break;
    }


    hasInvalidToken |= openingBrackets !== closingBrackets;
    if (hasJsonFunction && hasInvalidToken) {
      throw new Error(`Invalid json statement: ${stmt}`);
    }


    return hasJsonFunction;
  }

  /**
   * Generates an SQL query that extract JSON property of given path.
   *
   * @param   {string}               column  The JSON column
   * @param   {string | Array<string>} [path]  The path to extract (optional)
   * @returns {string}                       The generated sql query
   * @private
   */
  jsonPathExtractionQuery(column, path) {
    /**
     * Sub paths need to be quoted as ECMAScript identifiers
     *
     * https://bugs.mysql.com/bug.php?id=81896
     */
    const paths = _.toPath(path).map(subPath => Utils.addTicks(subPath, '"'));
    const pathStr = `${['$'].concat(paths).join('.')}`;
    const quotedColumn = this.isIdentifierQuoted(column) ? column : this.quoteIdentifier(column);
    return `(${quotedColumn}->>'${pathStr}')`;
  }

  /**
   *  Generates fields for getForeignKeysQuery
   *
   * @returns {string} fields
   * @private
   */
  _getForeignKeysQueryFields() {
    return [
      'CONSTRAINT_NAME as constraint_name',
      'CONSTRAINT_NAME as constraintName',
      'CONSTRAINT_SCHEMA as constraintSchema',
      'CONSTRAINT_SCHEMA as constraintCatalog',
      'TABLE_NAME as tableName',
      'TABLE_SCHEMA as tableSchema',
      'TABLE_SCHEMA as tableCatalog',
      'COLUMN_NAME as columnName',
      'REFERENCED_TABLE_SCHEMA as referencedTableSchema',
      'REFERENCED_TABLE_SCHEMA as referencedTableCatalog',
      'REFERENCED_TABLE_NAME as referencedTableName',
      'REFERENCED_COLUMN_NAME as referencedColumnName'
    ].join(',');
  }

  /**
   * Generates an SQL query that returns all foreign keys of a table.
   *
   * @param  {string} tableName  The name of the table.
   * @param  {string} schemaName The name of the schema.
   * @returns {string}            The generated sql query.
   * @private
   */
  getForeignKeysQuery(tableName, schemaName) {
    return `SELECT ${this._getForeignKeysQueryFields()} FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_NAME = '${tableName /* jshint ignore: line */
    }' AND CONSTRAINT_NAME!='PRIMARY' AND CONSTRAINT_SCHEMA='${schemaName}' AND REFERENCED_TABLE_NAME IS NOT NULL;`; /* jshint ignore: line */
  }

  /**
   * Generates an SQL query that returns the foreign key constraint of a given column.
   *
   * @param {string} table Table
   * @param  {string} columnName The name of the column.
   * @returns {string}            The generated sql query.
   * @private
   */
  getForeignKeyQuery(table, columnName) {
    const tableName = table.tableName || table;
    const schemaName = table.schema;

    return `SELECT ${this._getForeignKeysQueryFields()
    } FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
      + ` WHERE (REFERENCED_TABLE_NAME = ${wrapSingleQuote(tableName)
      }${schemaName ? ` AND REFERENCED_TABLE_SCHEMA = ${wrapSingleQuote(schemaName)}` : ''
      } AND REFERENCED_COLUMN_NAME = ${wrapSingleQuote(columnName)
      }) OR (TABLE_NAME = ${wrapSingleQuote(tableName)
      }${schemaName ? ` AND TABLE_SCHEMA = ${wrapSingleQuote(schemaName)}` : ''
      } AND COLUMN_NAME = ${wrapSingleQuote(columnName)
      } AND REFERENCED_TABLE_NAME IS NOT NULL`
      + ')';
  }

  /**
   * Generates an SQL query that removes a foreign key from a table.
   *
   * @param  {string} tableName  The name of the table.
   * @param  {string} foreignKey The name of the foreign key constraint.
   * @returns {string}            The generated sql query.
   * @private
   */
  dropForeignKeyQuery(tableName, foreignKey) {
    return `ALTER TABLE ${this.quoteTable(tableName)} DROP FOREIGN KEY ${this.quoteIdentifier(foreignKey)};`;
  }
}

function wrapSingleQuote(identifier) {
  return Utils.addTicks(identifier, '\'');
}

module.exports = ClickHouseQueryGenerator;