const mysql = require('mysql')

const debug = require('debug')('app:client:' + __filename.replace(__dirname, ''))


function dbMysql() {
    var pool = mysql.createPool({
        multipleStatements: true,
        connectionLimit: 2,
        host: config.host,
        user: config.uid,
        password:config.dataPwd,
        database:config.database,
        port: config.port
    })
    this.pool = pool
}

dbMysql.prototype.checkConnection = function() {
    return this.query(`select 1 as a`)
}

dbMysql.prototype.getTableInfo = function() {
    return this.query(`select table_name as tablename,table_rows as rows from information_schema.tables   where table_schema='${config.database}'`)
        .then((arrTables) => {
            let dbTables = arrTables.map((item) => {
                return item.tablename
            })
            global.tables = dbTables
            return dbTables
        })
}

dbMysql.prototype.query = function(sql) {
    return new Promise((resolve, reject) => {

        this.pool.query(sql, function(err, rows, fields) {
            if (err) {
                reject(new Error(`QueryError;${err.message}`))
            } else {
                resolve(rows)
            }
        })
    })
}

/**
 * 本地插入或者更新一条数据
 * @param  {} tablename:本地数据表名
 * @param  {} row:数据记录
 */
dbMysql.prototype.upsert = function(tableObj, row) {
        return new Promise((resolve, reject) => {
             
            let datecols = tableObj.arrdatecols

            if (datecols && datecols.length > 0) {
                datecols.forEach((colname) => {
                    if (row[colname] != null)
                        row[colname] = new Date(row[colname])
                })
            }
           
            let tablename = tableObj.tablename; // .toLowerCase()

            this.pool.query(`INSERT INTO ${tablename} SET ?`, row, (err, result) => {
                if (err) {
                    // ID冲突,说明需要UPDATE数据
                    if (err.code == 'ER_DUP_ENTRY') {
                        this.update(tableObj, row).then((res) => {
                            resolve(res)
                        }).catch((err) => {
                            console.error("errWhenInsert", err)
                            reject(err)
                        })
                    } else {
                        reject(new Error(`InsertError;${err.code};${err.message}`))
                    }
                } else {
                    // 成功插入了一条记录
                    resolve({ isNew: true })
                }
            })
        })
    }
    /**
     * 本地更新一条数据，如果插入失败，并且是报数据已经存在，那么要进入这个逻辑
     * @param  {} tablename:本地数据表名
     * @param  {} row:数据记录
     */
dbMysql.prototype.update = function(tableObj, row) {
    return new Promise((resolve, reject) => {
        // 有的数据库大小写敏感
        let tablename = tableObj.tablename; // toLowerCase()
        let sql = `UPDATE ${tablename} SET `
        var keys = Object.keys(row).sort((a, b) => {
            return a > b
        })
        let dataArr = []

        keys.forEach((key) => {
            if (key.toLowerCase() != 'id') {
                sql += `${key}=?,`
                dataArr.push(row[key])
            }
        })

        // 去掉最后一个，
        sql = sql.substring(0, sql.length - 1)

        // 如果有ID字段，认为这就是唯一主键
        if (row.ID) {
            sql += ` WHERE ID= ? `
            dataArr.push(row.ID)
        } else {
            // 如果没有ID主键,要靠组合主键判断,有可能有多个组合主键
            let uniqueKeys = tableObj.uniquekeys
            let arrCols
            for (var uKey in uniqueKeys) {
                if (!arrCols)
                    arrCols = uniqueKeys[uKey]
            }
            if (arrCols.length > 0) {
                // 时间类型
                let datecols = tableObj.arrdatecols

                sql += ' WHERE '
                arrCols.forEach((colname, colindex) => {
                    if (colindex != 0) {
                        sql += ' AND '
                    }
                    if (typeof row[colname] == 'number') {
                        sql += `  ${colname} =  ${row[colname]}`
                    } else {
                        // 如果组合主键是时间类型
                        if (datecols.indexOf(colname) != -1) {
                            sql += `  ${colname} = '${row[colname].format('yyyy-MM-dd hh:mm:ss')}'`
                        } else {
                            sql += `  ${colname} = '${row[colname]}'`
                        }
                    }
                })
            } else {
                reject(new Error(`UpdateError;NoUniqueKeys`))
            }
        }

        // 如果UPDATETIME没有变化，则不更新。尽量不更新数据
        if (typeof row[tableObj.getUpdateTimeField()] == 'number') {
            sql += `  and ${tableObj.updatetimeField}<> ${row[tableObj.getUpdateTimeField()]}`
        } else {
            // 日期时分秒控制
            let upTimeStr = row[tableObj.getUpdateTimeField()].format('yyyy-MM-dd hh:mm:ss')
            sql += `   and  DATE_FORMAT(${tableObj.updatetimeField},'%Y-%m-%d %H:%i:%s') <> '${upTimeStr}'`
        }

        this.pool.query(sql, dataArr, (err, result) => {
            if (err) {
                console.log('UpdateError', err, sql)
                reject(new Error(`UpdateError;${err.code};${err.message}`))
                global.report.reportLog('SqlError', 'sqlerror:' + sql, 'syncerror')
            } else {
                // 如果有数据更新，上报一条log
                if (result.changedRows > 0) {
                    global.report.reportLog('UpdateSql', sql, 'updatedata')
                }

                resolve({ isUpdate: true, updateRow: result.changedRows })
            }
        })
    })
}

exports = module.exports = dbMysql