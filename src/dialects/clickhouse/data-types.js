'use strict';

const _ = require('lodash');
const moment = require('moment-timezone');

module.exports = BaseTypes => {
  BaseTypes.ABSTRACT.prototype.dialectTypes = 'https://clickhouse.yandex/docs/en/data_types';

  BaseTypes.DATE.types.clickhouse = ['DateTime'];
  BaseTypes.STRING.types.clickhouse = ['String'];
  BaseTypes.CHAR.types.clickhouse = ['String'];
  BaseTypes.TEXT.types.clickhouse = ['String'];
  BaseTypes.TINYINT.types.clickhouse = ['Int8'];
  BaseTypes.SMALLINT.types.clickhouse = ['Int16'];
  BaseTypes.MEDIUMINT.types.clickhouse = ['Int64'];
  BaseTypes.INTEGER.types.clickhouse = ['Int32'];
  BaseTypes.BIGINT.types.clickhouse = ['Int128'];
  BaseTypes.FLOAT.types.clickhouse = ['Float32'];
  BaseTypes.TIME.types.clickhouse = false;
  BaseTypes.DATEONLY.types.clickhouse = ['Date'];
  BaseTypes.BOOLEAN.types.clickhouse = ['Int8'];
  BaseTypes.BLOB.types.clickhouse = ['String'];
  BaseTypes.DECIMAL.types.clickhouse = ['Decimal'];
  BaseTypes.UUID.types.clickhouse = true;
  BaseTypes.ENUM.types.clickhouse = true;
  BaseTypes.REAL.types.clickhouse = false;
  BaseTypes.DOUBLE.types.clickhouse = false;
  BaseTypes.GEOMETRY.types.clickhouse = false;
  BaseTypes.JSON.types.clickhouse = ['String'];
  BaseTypes.ARRAY.types.clickhouse = ['Array'];

  const StringDefaultFn = () => '';
  const NumberDefaultFn = () => 0;

  class STRING extends BaseTypes.STRING {
    constructor(length, binary) {
      super(length, binary);

      if (!(this instanceof STRING)) return new STRING(length, binary);
      BaseTypes.STRING.apply(this, arguments);
    }
    toSql() {
      return 'String';
    }

    getDefaultValue = StringDefaultFn;
  }

  BaseTypes.STRING.types.clickhouse = {
    oids: [1043],
    /* eslint-disable-next-line  camelcase */
    array_oids: [1015]
  };

  class CHAR extends BaseTypes.CHAR {
    constructor(length, binary) {
      super(length, binary);
      if (!(this instanceof CHAR)) return new CHAR(length, binary);
      BaseTypes.CHAR.apply(this, arguments);
    }
    toSql() {
      return 'FixedString(255)';
    }

    getDefaultValue = StringDefaultFn;
  }

  class TEXT extends BaseTypes.TEXT {
    constructor(length, binary) {
      super(length, binary);
      if (!(this instanceof TEXT)) return new TEXT(length, binary);
      BaseTypes.TEXT.apply(this, arguments);
    }
    toSql() {
      return BaseTypes.TEXT.prototype.toSql.call(this);
    }

    getDefaultValue = StringDefaultFn;
  }

  class BLOB extends BaseTypes.BLOB {
    constructor(length) {
      super(length);
      if (!(this instanceof BLOB)) return new BLOB(length);
      BaseTypes.BLOB.apply(this, arguments);
    }
    toSql() {
      return 'String';
    }
    _stringify(blob, _options) {
      return blob ? JSON.stringify(blob) : '\'\'';
    }

    getDefaultValue = StringDefaultFn;
  }

  class TINYINT extends BaseTypes.TINYINT {
    constructor(length) {
      super(length);
      if (!(this instanceof TINYINT)) return new TINYINT(length);
      BaseTypes.TINYINT.apply(this, arguments);
    }
    toSql() {
      return 'Int8';
    }
    getDefaultValue() { return 0; }
  }

  class SMALLINT extends BaseTypes.SMALLINT {
    constructor(length) {
      super(length);
      if (!(this instanceof SMALLINT)) return new SMALLINT(length);
      BaseTypes.SMALLINT.apply(this, arguments);
    }
    toSql() {
      return 'Int16';
    }

    getDefaultValue = NumberDefaultFn;
  }

  class BIGINT extends BaseTypes.BIGINT {
    constructor(length) {
      super(length);

      if (!(this instanceof BIGINT)) return new BIGINT(length);
      BaseTypes.BIGINT.apply(this, arguments);
    }

    toSql() {
      return 'Int128';
    }

    getDefaultValue = NumberDefaultFn;
  }

  class INTEGER extends BaseTypes.INTEGER {
    constructor(length) {
      super(length);
      if (!(this instanceof INTEGER)) return new INTEGER(length);
      BaseTypes.INTEGER.apply(this, arguments);
    }
    toSql() {
      return 'Int32';
    }

    getDefaultValue = NumberDefaultFn;
  }

  class BOOLEAN extends BaseTypes.BOOLEAN {
    constructor(length) {
      super(length);
      if (!(this instanceof BOOLEAN)) return new BOOLEAN(length);
      BaseTypes.BOOLEAN.apply(this, arguments);
    }
    toSql() {
      return 'Int8';
    }
    _stringify(bool, options) {
      return bool === true ? 1 : 0;
    }

    getDefaultValue = NumberDefaultFn;
  }

  class MEDIUMINT extends BaseTypes.MEDIUMINT {
    constructor(length) {
      super(length);
      if (!(this instanceof MEDIUMINT)) return new MEDIUMINT(length);
      BaseTypes.MEDIUMINT.apply(this, arguments);
    }
    toSql() {
      return 'Int64';
    }

    getDefaultValue = NumberDefaultFn;
  }

  class DECIMAL extends BaseTypes.DECIMAL {
    constructor(precision, scale) {
      super(precision, scale);
      if (!(this instanceof DECIMAL)) return new DECIMAL(precision, scale);
      BaseTypes.DECIMAL.apply(this, arguments);
    }
    toSql() {
      let definition = BaseTypes.DECIMAL.prototype.toSql.apply(this);

      if (this._unsigned) {
        definition = `U${definition}`;
      }

      return definition;
    }

    getDefaultValue = NumberDefaultFn;
  }

  class DATE extends BaseTypes.DATE {
    constructor(length) {
      super(length);
      if (!(this instanceof DATE)) return new DATE(length);
      BaseTypes.DATE.apply(this, arguments);
    }
    static parse(value, options) {
      value = value.string();

      if (value === null) {
        return value;
      }

      if (moment.tz.zone(options.timezone)) {
        value = moment.tz(value, options.timezone).toDate();
      } else {
        value = new Date(`${value} ${options.timezone}`);
      }

      return value;
    }
    toSql() {
      return 'DateTime';
    }
    _stringify(date, options) {
      date = BaseTypes.DATE.prototype._applyTimezone(date, options);

      return date.format('YYYY-MM-DD HH:mm:ss');
    }
    getDefaultValue() { return '0000-00-00 00:00:00'; }
  }

  class DATEONLY extends BaseTypes.DATEONLY {
    constructor() {
      super();
      if (!(this instanceof DATEONLY)) return new DATEONLY();
      BaseTypes.DATEONLY.apply(this, arguments);
    }
    static parse(value) {
      return value.string();
    }
    getDefaultValue() { return '0000-00-00'; }
    toSql() {
      return 'Date';
    }
  }

  class UUID extends BaseTypes.UUID {
    constructor() {
      super();
      if (!(this instanceof UUID)) return new UUID();
      BaseTypes.UUID.apply(this, arguments);
    }
    toSql() {
      return 'UUID';
    }
    getDefaultValue() { return '00000000-0000-0000-0000-000000000000'; }
  }

  class ENUM extends BaseTypes.ENUM {
    constructor() {
      super();
      if (!(this instanceof ENUM)) {
        const obj = Object.create(ENUM.prototype);
        ENUM.apply(obj, arguments);
        return obj;
      }

      BaseTypes.ENUM.apply(this, arguments);
      if (!this.values?.length) this.values = arguments[0]?.values || [];
    }

    toSql(options) {
      return `Enum8(${this.values.map((value, i) => `${options.escape(value)} = ${i + 1}`).join(', ')})`;
    }

    getDefaultValue = NumberDefaultFn;
  }

  class ENUM16 extends BaseTypes.ENUM {
    constructor() {
      super();
      if (!(this instanceof ENUM16)) {
        const obj = Object.create(ENUM16.prototype);
        ENUM.apply(obj, arguments);
        return obj;
      }
      BaseTypes.ENUM16.apply(this, arguments);
    }
    toSql(options) {
      return `Enum16(${_.map(this.values, (value, i) => `${options.escape(value)} = ${i + 1}`).join(', ')})`;
    }
  }


  class JSONTYPE extends BaseTypes.JSON {
    constructor() {
      super();
      if (!(this instanceof JSONTYPE)) return new JSONTYPE();
      BaseTypes.JSON.apply(this, arguments);
    }
    
    _stringify(value, options) {
      return options.operation === 'where' && typeof value === 'string' ? value : JSON.stringify(value);
    }

    getDefaultValue = StringDefaultFn;
  }


  class ARRAY extends BaseTypes.ARRAY {
    toSql() {
      return `Array(${ this.type.toSql() })`;
    }

    _stringify(values, options) {
      const str = `[${ values.map(value => {
        if (this.type && this.type.stringify) {
          value = this.type.stringify(value, options);
    
          if (this.type.escape === false) {
            return value;
          }
        }
        return options.escape(value);
      }, this).join(',') }]`;
    
      return str;
    }

    getDefaultValue = () => [];
  }

  const exports = {
    ARRAY,
    DATE,
    STRING,
    CHAR,
    TEXT,
    TINYINT,
    SMALLINT,
    MEDIUMINT,
    BIGINT,
    INTEGER,
    BOOLEAN,
    ENUM,
    ENUM16,
    DATEONLY,
    UUID,
    DECIMAL,
    BLOB,
    JSON: JSONTYPE
  };

  _.forIn(exports, (DataType, key) => {
    if (!DataType.key) DataType.key = key;
    if (!DataType.extend) {
      DataType.extend = function extend(oldType) {
        return new DataType(oldType.options);
      };
    }
  });

  return exports;
};
