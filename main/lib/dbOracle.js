// load the oracledb library
var oracledb = require('oracledb')

// load the simple oracledb
//var SimpleOracleDB = require('simple-oracledb')
oracledb.autoCommit = true
oracledb.fetchAsString = [oracledb.CLOB];
// modify the original oracledb library
//SimpleOracleDB.extend(oracledb)

function dbOracle() {
  this.dbConfig = global.config
}

// 初始化oracle连接池
dbOracle.prototype.initPool = function () {
  return new Promise((resolve, reject) => {
    let dbConfig = this.dbConfig


    oracledb.createPool({
      user: dbConfig.uid,
      password: dbConfig.dataPwd,
      connectString: `${dbConfig.host}:${dbConfig.port || 1521}/${dbConfig.database}`,
      poolMin: 1,
      poolMax: 5,
      retryCount: 5, // The max amount of retries to get a connection from the pool in case of any error (default to 10 if not provided)
      retryInterval: 500, // The interval in millies between get connection retry attempts (defaults to 250 millies if not provided)
      runValidationSQL: true, // True to ensure the connection returned is valid by running a test validation SQL (defaults to true)
      validationSQL: 'SELECT 1 FROM DUAL', // The test SQL to invoke before returning a connection to validate the connection is open (defaults to 'SELECT 1 FROM DUAL')
      // any other oracledb pool attributes
    }, (error, pool) => {

      if (error) {
        reject(error)
      } else {
        // this.pool = pool
        resolve(pool)
      }
    })
  })
}

dbOracle.prototype.checkConnection = function () {
  return this.query(`SELECT 1 FROM DUAL`)
}

dbOracle.prototype.getTableInfo = function () {
  return this.query(`select table_name as TABLENAME, num_rows from user_tables`)
    .then((arrTables) => {
      let dbTables = arrTables.map((item) => {
        return item.TABLENAME
      })
      global.tables = dbTables
      return dbTables
    })
}

dbOracle.prototype.query = function (sql, paramData, type) {
  return new Promise((resolve, reject) => {
   

    oracledb.getPool().getConnection((err, conn) => {
      if (err) {
        reject(new Error(`GetConnectionFail;${err.message}`))
      } else {
        let queryConfig = { maxRows: 1000, outFormat: oracledb.OBJECT }
        // 执行SQL语句的类型，query,insert,update

        let queryType = type || 'query';
        conn.execute(sql, paramData || [], queryConfig,
          function (err, result) {
            conn.release()
            if (err) {
              reject(err)
            } else {
              resolve(queryType == 'query' ? result.rows : result)
            }
          })


      }
    })
  })
}

/**
 * 本地插入或者更新一条数据
 * @param  {} tablename:本地数据表名
 * @param  {} row:数据记录
 */
dbOracle.prototype.upsert = function (tableObj, row, rowObj) {
  return new Promise((resolve, reject) => {
    let datecols = tableObj.arrdatecols

    //2017-08-09:有可能MYSQL里面的字段是小写。ORACLE全部是大写
    for (var key in row) {
      if (key.toUpperCase() != key) {
        row[key.toUpperCase()] = row[key];
        delete row[key];
      }
    }
    if (datecols && datecols.length > 0) {
      datecols.forEach((colname) => {
        if (row[colname] != null)
          row[colname] = new Date(row[colname])
      })
    }

    let tablename = tableObj.tablename; // .toLowerCase()
    let strValues = ''
    let colStr = ''
    for (var key in row) {
      strValues += `:${key},`
      colStr += `${key},`
    }
    strValues = strValues.substring(0, strValues.length - 1)
    colStr = colStr.substring(0, colStr.length - 1)
    var sql = `INSERT INTO ${tablename}(${colStr}) VALUES(${strValues})`

    // if(row["ANN_CONTE"])
    // row["ANN_CONTE"]="test"
    this.query(sql, row, 'insert').then((dbData) => {

      resolve({ isNew: true })
    }).catch((err) => {
      //插入失败
      if (err.message && err.message.indexOf('ORA-00001') != -1) {
        this.update(tableObj, row).then((dbUpdateRet) => {
          resolve(dbUpdateRet)
        }).catch((dbUpdateError) => {
          reject(dbUpdateError)
        })
      } else {
        global.report.reportLog('inserterror', `${rowObj.keyDataStr},${err.message}`, 'reporterror');
        reject(err)
      }
    })
  })
}
/**
 * 本地更新一条数据，如果插入失败，并且是报数据已经存在，那么要进入这个逻辑
 * @param  {} tablename:本地数据表名
 * @param  {} row:数据记录
 */
dbOracle.prototype.update = function (tableObj, row) {
  return new Promise((resolve, reject) => {
    // 有的数据库大小写敏感
    let tablename = tableObj.tablename; // toLowerCase()

    let whereSql = ''

    // 如果没有ID主键,要靠组合主键判断,有可能有多个组合主键
    let uniqueKeys = tableObj.uniquekeys
    let arrCols
    for (var uKey in uniqueKeys) {
      if (!arrCols)
        arrCols = uniqueKeys[uKey]
    }
    if (arrCols.length > 0) {
      arrCols.forEach((colname, colindex) => {
        if (colindex != 0) {
          whereSql += ' AND '
        }
        whereSql += `  ${colname} =  :${colname}`
      })
    } else {
      reject(new Error(`UpdateError;NoUniqueKeys`))
    }

    // 如果UPDATETIME没有变化，则不更新。尽量不更新数据
    whereSql += `  and UPDATETIME<> :UPDATETIME`

    let updateValues = ''

    for (var key in row) {
      updateValues += `${key}=:${key},`
      // colStr += `${key},`
    }
    updateValues = updateValues.substring(0, updateValues.length - 1)
    // colStr = colStr.substring(0, colStr.length - 1)
    let sql = `UPDATE ${tablename} SET ${updateValues} WHERE ${whereSql}`

    //     console.log('start update',sql)
    this.query(sql, row, 'update').then((dbRet) => {

      resolve({ isUpdate: true, updateRow: dbRet.rowsAffected })
    }).catch((dbErr) => {
      console.log('updateerror', dbErr)
      global.report.reportLog(`updateerror,${whereSql}`, 'reporterror');

      reject(dbErr)
    })
  })
}

exports = module.exports = dbOracle
