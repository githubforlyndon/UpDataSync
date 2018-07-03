// 表数据记录操作类
var rowHelper = {}
const logger = require('./logger.js')

// 当前有几条数据正在处理
var RowsUpserting = 0

function row(data, tableobj) {

    //有可能同步两边数据字段不一致
    let cols = tableobj.cols;

    for (var key in data) {
        //要检查字段是大写还是小写,
        if (tableobj.lowercase) {
            data[key.toLowerCase()] = data[key];
        }

    }

    for (var key in data) {
        if (!cols[key]) {
            delete data[key]
        }
    }

    this.data = data;


    this.tableobj = tableobj; // 这条数据的table对象
    /*
        这条数据最后更新的状态。insert(成功插入了),update（成功更新了）,nochange（数据未变化，不用改db）,fail,pluginfail
    */
    this.pluginupdatetype = ''
    this.dbupdatetype = 'fail'
    this.errmsg = ''; // 在插入或者更新的过程中，出的错误信息。eg：部分图片拉取失败等
    this.keyDataStr = this.getKeyData()
}

// 获取数据的ID，如果是组合主键，那么打出组合主键的值,同时也包含updatetime
row.prototype.getKeyData = function() {
    let uniqueKeys = this.tableobj.uniquekeys
    let updateTime = this.data[this.tableobj.getUpdateTimeField()]
    let keyDataStr = `updatetime:${updateTime};`
    for (var uKey in uniqueKeys) {
        let cols = uniqueKeys[uKey]
        cols.forEach((colname) => {
            keyDataStr += `${colname}:${this.data[colname]};`
        })
    }
    return keyDataStr
}

row.prototype.logUpsertStatus = function() {
        if (this.errmsg == '') {
            global.report.reportLog('row', `name:${this.tableobj.tablename},localcount:${this.tableobj.localcount},${this.dbupdatetype},${this.keyDataStr}, ${this.pluginupdatetype},${this.errmsg}`)
        } else {
            global.report.reportLog('rowerror', `name:${this.tableobj.tablename},localcount:${this.tableobj.localcount},${this.dbupdatetype},${this.keyDataStr}, ${this.pluginupdatetype},${this.errmsg}`, 'reporterror')
        }

        // global.logger.data(this.tableobj.tablename, this.data['ID'], this.dbupdatetype, this.pluginupdatetype,this.errmsg)
    }
    // 开始处理一条数据
row.prototype.startUpsert = function() {
    return this.beforeUpsert().then((ret) => {
        return this.upsertToDB()
    }).then((dbUpdateType) => {
        this.dbupdatetype = dbUpdateType
        this.logUpsertStatus()
        return this
    }).catch((err) => {
        console.log('ERROR START UPSERT', err);
        let eMsg = err.toString();
        if (err.message) {
            eMsg = err.message
        }
        this.errmsg += ';' + eMsg
        this.logUpsertStatus()
        return this
    })
}

row.prototype.beforeUpsert = function() {
    var promise = new Promise((resolve, reject) => {
        let busobj = this.tableobj.busobj
        if (busobj.plugin && busobj.plugin.onBeforeUpdateData) {

            // 插入数据之前要处理
            busobj.plugin.onBeforeUpdateData(this).then((pluginProcessRet) => {
                resolve(1)
            }).catch((err) => {
                reject(err)
            })
        } else {
            resolve(1)
        }
    })

    return promise
}

// 插入或者更新一条数据
row.prototype.upsertToDB = function() {
    // this.tableobj.busobj.db.
    var promise = new Promise((resolve, reject) => {

        let busobj = this.tableobj.busobj

        // db类型在登录时业务已经决定了（t_account表local_db_type字段）
        //
        busobj.db.upsert(this.tableobj, this.data, this).then((ret) => {
            let dbUpdateType = 'fail'
                // 插入了一条记录
            if (ret.isNew) {
                dbUpdateType = 'insert'
            } else if (ret.isUpdate) {
                dbUpdateType = ret.updateRow == 1 ? 'update' : 'nochange'
            }
            // 数据有变更
            if (ret.isNew || ret.isUpdate) {
                if (busobj.plugin && busobj.plugin.onUpdateData) {
                    busobj.plugin.onUpdateData(this, row).then((pluginUpdateRet) => {
                        resolve(dbUpdateType)
                    }).catch((pluginUpdateErr) => {
                        // 插件处理失败
                        // updateType = 'pluginfail'
                        this.pluginupdatetype = 'pluginfail'
                        resolve(dbUpdateType)
                    })
                } else {
                    // 无插件
                    resolve(dbUpdateType)
                }
            }
        }).catch((err) => {
            this.errmsg += err.message
            logger.error('insert sql error', err, this.keyDataStr, this.tableobj.tablename);
            resolve('fail')
        })
    })

    return promise
}

//
rowHelper.upsertRows = function(rows, tableobj) {
    let countinonetime = tableobj.countinonetime
}

exports = module.exports = row