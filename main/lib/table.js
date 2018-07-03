const debug = require('debug')('app:server:' + __filename.replace(__dirname, ''))
const fs = require('fs')
const path = require('path')
const logger = require('./logger.js')
const tafLogger = require('../taf/logger');
const report = require('./report')
const rpc = require('./rpc')
var row = require('./row')
const Q = require('q')

const E_STATUS = {
    '0': '正常同步中，等待下一次同步',
    '1': '准备开始同步', // 要准备一些数据
    '2': '正在拉取数据',
    '3': '正在同步本地数据', // 动态
    '4': '本轮所有数据同步完毕', // 动态(本轮所有数据同步完毕,成功同步xxx条，下次同步xx分钟后)
    '5': '数据表创建成功',
    '6': '等待第一次初始化数据',
    // '8': '本次同步数据全部插入完毕',
    // '10': '为本轮开始拉取准备数据',
    '-1': '本地无数据表，即将自动创建',
    '-2': '创建数据表失败,服务器未正常返回建表SQL',
    '-3': '创建数据表失败,本地执行建表SQL失败',
    '-4': '用户主动暂停同步',
    '-5': '为同步准备数据失败',
    '-6': '排队中',
    //-7其实是远端服务器超时，之所以不写出来是为了怕用户恐慌(其实超时是正常的)
    '-7': '本轮所有数据同步完成，等待下次同步',
    '-8': '本地数据更新出错',
    '-9': '新授权表',
    '-10': '无权限同步数据'
}

function table(busobj, tablename, table_cname, status, localcount, upcount, interval) {
    this.busobj = busobj

    // 有可能表名被强转成了小写
    if (global.tables.indexOf(tablename) == -1 && global.tables.indexOf(tablename.toLowerCase()) != -1) {
        tablename = tablename.toLowerCase()
    }
    this.tablename = tablename
    this.table_cname = table_cname
    this.status = status
    this.localcount = localcount
    this.upcount = upcount
    this.interval = interval || 60
    this.insertcountthistime = 0; // 本轮同步有多少数据新增
    this.updatecountthistime = 0; // 本轮同步更新了多少数据
    this.errmsg = ''
    // this.isupdating = false

    // 本次第几轮
    this.turnindex = 0
    // 本轮更新了几条
    this.turnupdate = 0
    // 本轮插入了几条
    this.turninsert = 0
    // 本轮几条未变
    this.turnnochange = 0
    this.ismyturn = false
    this.uniquekeys = null
    // 列
    this.cols = {}
    // 时间列
    this.arrdatecols = []

    // 每次同时处理多少条,数据中心一次处理一条，因为如果插入失败，则不处理后面的数据
    this.countinonetime = (this.busobj.busid == 'datacenter' || this.busobj.busid == 'datacenterinit') ? 1 : 10

    this.datamustinsert = this.busobj.busid == 'datacenter' || this.busobj.busid == 'datacenterinit';
    // 每次请求多少条
    this.pagesize = busobj.pagesize || 200


    this.lowercase = false;
    this.updatetimeField = this.getUpdateTimeField();
}

/**
 * @desc 定时去服务器去加载配置，需要更新一下值
 */
table.prototype.setInterval = function (interval) {

    if (interval != undefined) {
        this.interval = interval;

    }

}

table.prototype.getUpdateTimeField = function () {
    //陈平个贱人，updatetime字段非要叫update_time，所以这里只能改成可配置了。
    if (this.busobj.otherConfig.updatetimeField) {
        return this.busobj.otherConfig.updatetimeField
    }
    else {
        //有的用户MYSQL配置里面，所有的字段都是小写，所以要判断是不是小写
        return this.lowercase ? "updatetime" : "UPDATETIME"
    }


}

table.prototype.setStatus = function (status, errmsg) {
    this.statusname = E_STATUS[status] || '未知状态'
    if (errmsg) {
        this.statusname += `(${errmsg})`
    }
    debug(this.tablename, `${this.status}===>${status}`, this.statusname)

    if (this.status != status) {

        this.lastStatus = status;
        this.status = status

        // table.HomeEventObj.sender.send('taskLog', {str:`${this.tablename}`})
        // 告诉taskHelper处理下一个表
        let logStr = `${this.tablename}:${this.statusname}`
        if (status == 4 || status == -2 || status == -3 || status == 5 || status == -4 || status == -5 || status == -7 || status == -8) {
            if (status == 4) {
                logStr += `(新增：${this.insertcountthistime}条，更新:${this.updatecountthistime}条)`
            } else if (status == -4) {
                logStr += `暂停同步(新增：${this.insertcountthistime}条，更新:${this.updatecountthistime}条)`
            }

            logger.l(logStr)
            tafLogger.data.info(`table-136|taskLog: ${logStr}`);
            //global.WIN_HOME.send('taskLog', { str: logStr })
            // 最新一条LOG记录下来，上报心跳的时候带上

            this.ismyturn = false
            this.setNextSyncTaskTime()
            this.E.emit('tableTaskDone', this)
        }
        global.LAST_LOG_STR = `${logStr}:localcount:${this.localcount}:${new Date().format('yyyy-MM-dd:hh:mm:ss')}`
        global.report.reportLog('tablestatus', global.LAST_LOG_STR, 'tablestatus')
        if (status == 4) {
            global.report.reportLog('syncdone', global.LAST_LOG_STR, 'syncdone')
        }
    }
}

// 设置下次同步的时间
table.prototype.setNextSyncTaskTime = function () {
    if (this.status != -4) {
        var interval = this.interval; // XX秒

        var now = new Date().valueOf()
        var nextTaskTime = now + interval * 1000
        if (this.status == 5) {
            // 如果当前状态是5的话，就不要等频率(60秒)那么久了,10秒后启动同步
            nextTaskTime = now + 10000
        }
        this.inexttasktime = nextTaskTime
        // 到分钟就行了，秒级不好控制
        this.nexttasktimestr = new Date(nextTaskTime).format('MM-dd hh:mm:ss')
    } else {
        this.nexttasktime = -1
        this.nexttasktimestr = '-'
    }
}

// 设置本地数据表记录数
table.prototype.setLocalCount = function (count) {
    this.localcount = count

    this.sendLocalCountChangeEvent()

    // 发送事件太快了,可以做个延迟
    // global.WIN_HOME.send('tableStatusChanged', this)
}

// 不能发送太快
var LastSendTime = 0
var IEventTimeout
table.prototype.sendLocalCountChangeEvent = function () {
    let now = Date.now()
    //最多2秒一次
    if (now - LastSendTime > 500) {
        LastSendTime = now
        tafLogger.data.info(`table-184|tableStatusChanged:tablename:${this.tablename},status:${this.status},localcount:${this.localcount},upcount:${this.upcount},interval:${this.interval},insertcountthistime:${this.insertcountthistime},updatecountthistime:${this.updatecountthistime},turnindex:${this.turnindex},turninsert:${this.turninsert},turnupdate:${this.turnupdate},pagesize:${this.pagesize},updatetimeField:${this.updatetimeField},errmsg:${this.errmsg}`);
    } else {

        // 先把之前的延迟事件清掉，再重新加一个
        clearTimeout(IEventTimeout)
        IEventTimeout = setTimeout(() => {
            this.sendLocalCountChangeEvent()
        }, 1000)
    }
}

// 当前数据表的任务
table.prototype.myTask = function () {
    if (this.ismyturn) {
        return
    }
    global.CURRENTTABLE = this
    debug('myTask', this.tablename, this.status)
    if (this.status == -2 || this.status == -3) {
        // 创建本地表失败,先把状态置为-1
        this.setStatus(-1)
    } else if (this.status == -4 || this.status == -8 || this.status == -7 || this.status == -6 || this.status == 6 || this.status == 4 || this.status == 5) {
        // 上次同步的时候出错超过5次，把状态先改为1
        // 从来没有同步过的，也改成1
        this.setStatus(1)
    }
    this.ismyturn = true
    if (this.status == -1) {
        this.createTableForLocal()
    } else if (this.status == 1) {
        this.startASyncTask()
    }
}

table.prototype.createTableForLocal = function () {
    this.getCreateTableSql().then((sql) => {
        return this.createTableAsSql(sql)
    })
}

table.prototype.getCreateTableSql = function () {
    return new Promise((resolve, reject) => {
        rpc.getCreateTableSql(this.tablename, this.busobj.local_db_type).then((serverRet) => {
            global.report.reportLog('createsql', serverRet.sSql, 'sync')

            resolve(serverRet.sSql)
        }).catch((err) => {
            console.log('ERROR:CREATETABLE', err)
            global.report.reportLog(`createsqlError,${this.tablename},${err.message}`, 'syncerror')
            this.setStatus(-2, err.message)
            // this.E.emit('tableCreate', this)
            reject(new Error('-2'))
        })
    })
}

// 根据SQL在本地创建表
table.prototype.createTableAsSql = function (sql) {
    return new Promise((resolve, reject) => {
        // type参数只是oracle才有用(dbOracle.js)
        global.BUS.db.query(sql).then((ret) => {
            this.setStatus(5)
            // this.setStatus(1)
            this.E.emit('tableCreate', this)
            resolve(1)
        }).catch((err) => {

            this.setStatus(-3)
            this.E.emit('tableCreate', this)
            global.report.reportLog('createtableerror', `${sql},${err.message}`, 'reporterror')
            reject(new Error('-3'))
        })
    })
}

table.prototype.setStatusName = function (statusname, statuscolor) {
    this.statusname = statusname
    this.statuscolor = statuscolor || 'black'
}

// 把数据表的状态写入用户本地存储系统
table.prototype.saveCurrentStatusToLocal = function () {
    var tablename = this.tablename
    settings.set('table_' + global.AID + tablename.toLowerCase(), this.status)
}

// 开始新一轮同步任务
table.prototype.startASyncTask = function () {
    // check status

    this.turnindex = 0
    this.insertcountthistime = 0
    this.updatecountthistime = 0
    this.syncerrtimes = 0
    this.syncfetchdone = false
    tafLogger.data.info(`table-281|taskLog: ${this.tablename}:开始同步`)
    this.prepareDataForSync().then((rets) => {


        this.syncData()
    })
}

/*
  开始准备新一轮拉取数据，要准备一些数据，例如列的属性(哪些是日期？本地数据量有多少)，最新的一次UPDATE TIME
*/
table.prototype.prepareDataForSync = function () {
    this.setStatus(1)

    return this.getCols().then(() => {
        let actions = [this.getLocalDataCount(), this.getLastUpdateTime()]

        if (global.BUS.busid == 'datacenter') {
            // actions.push(this.getUniqueKeys())
        }
        actions.push(this.getUniqueKeys())

        // actions.push(this.getCols());
        return Q.all(actions).fail((err) => {
            console.log(err)
            this.setStatus(-5)
        })
    }).catch((e) => {
        console.log(e)
        this.setStatus(-5)
    })



}

// 获取oracle数据量
table.prototype.getLocalDataCount = function () {
    let sql = `SELECT COUNT(*) AS TOTAL FROM  ${this.tablename}`

    return global.BUS.db.query(sql).then((dbRet) => {
        var localcount = dbRet[0].TOTAL
        this.setLocalCount(localcount)
        return localcount
    })
}

// 获取oracle数据量
table.prototype.getLastUpdateTime = function () {
    let sql = `select ${this.getUpdateTimeField()} from ${this.tablename}  order by ${this.getUpdateTimeField()} desc `
    if (this.busobj.local_db_type == 'mysql') {
        sql += ' limit 0,1'
    }

    return this.busobj.db.query(sql).then((dbRet) => {
        var lastTime = 0
        if (dbRet.length > 0) {
            lastTime = dbRet[0][this.getUpdateTimeField()];
            //再次兼容
            if (lastTime == undefined) {
                dbRet[0]["UPDATETIME"];
            }


            // logger.l(`updatetime:${this.tablename},${lastTime}`)
            if (this.busobj.busid == 'datacenter' || this.busobj.busid == 'datacenterinit') {
                // 数据中心是timestamp类型
                var t = new Date(lastTime)
                this.setLastUpdatetime(t, 1)
            } else {
                // 正常要求UPDATETIME是int型
                this.setLastUpdatetime(lastTime, 1)
            }
        } else {
            logger.l(`noupdatetime:${this.tablename}`)
            this.setLastUpdatetime(0, 1)
        }

        return lastTime
    })
}

// 获取组合唯一键，主要是为了在UPDATE的时候，能定位到数据。因为数据中心的表都没有ID主键,只有数据中心的业务，才需要
table.prototype.getUniqueKeys = function () {
    if (this.busobj.local_db_type == 'mysql') {
        let sql = `show keys from ${this.tablename}`
        return global.BUS.db.query(sql).then((dbRet) => {
            let keysObj = {}

            dbRet.forEach((sItem) => {
                let consname = sItem.Key_name
                let colname = sItem.Column_name
                if (keysObj[consname]) {
                    keysObj[consname].push(colname)
                } else {
                    keysObj[consname] = [colname]
                }
            })
            this.uniquekeys = keysObj
            return keysObj
        })
    } else {

        //Oracle的表名是大写的。如果数据源是mysql的话，this.tablename可能是小写
        let sql = `select distinct   a.COLUMN_NAME,a.constraint_name as KEY_NAME
from all_cons_columns a, all_constraints b
where a.constraint_name = b.constraint_name
and b.constraint_type = 'P' and a.TABLE_NAME = '${this.tablename.toUpperCase()}'`
        return global.BUS.db.query(sql).then((dbRet) => {
            let keysObj = {}

            dbRet.forEach((sItem) => {
                let consname = sItem.KEY_NAME
                let colname = sItem.COLUMN_NAME
                if (keysObj[consname]) {
                    keysObj[consname].push(colname)
                } else {
                    keysObj[consname] = [colname]
                }
            })
            this.uniquekeys = keysObj
            return keysObj
        })
    }
}

// 获取表的列属性,主要是需要知道哪些是datetime类型
// 获取一个oracle数据表有哪些字段
table.prototype.getCols = function () {
    if (this.busobj.local_db_type == 'mysql') {
        let sql = `SHOW columns from ${this.tablename}`
        return global.BUS.db.query(sql).then((dbRet) => {
            let colsObj = {}
            let arrDateCols = []
            dbRet.forEach((sItem) => {
                let coltype = sItem.Type.toLowerCase()
                let colname = sItem.Field
                colsObj[colname] = coltype
                if (coltype.indexOf('timestamp') != -1 || coltype.indexOf('date') != -1) {
                    arrDateCols.push(colname)
                }
            })
            this.cols = colsObj;
            if (colsObj["updatetime"]) {
                this.lowercase = true;
            }
            this.arrdatecols = arrDateCols
            return colsObj
        })
    } else {
        // mysql-to-oracle,发现同步到oracle里面的表是大写的，为了避免查询不到数，这里表名统一转换成大写来进行比较
        let sql = `select column_name,data_type,data_length,data_precision,nullable,data_scale from cols 
      where upper(table_name) = upper('${this.tablename}') ORDER BY column_id`
        return global.BUS.db.query(sql).then((dbRet) => {
            let colsObj = {}
            let arrDateCols = []
            dbRet.forEach((sItem) => {
                let coltype = sItem['DATA_TYPE'].toLowerCase()
                let colname = sItem['COLUMN_NAME']
                colsObj[colname] = coltype
                if (coltype.indexOf('timestamp') != -1 || coltype.indexOf('date') != -1) {
                    arrDateCols.push(colname)
                }
            })
            this.cols = colsObj
            this.arrdatecols = arrDateCols
            return colsObj
        })
    }

}

// 同步数据
/**
 * @param {Boolean} isRetry:是否是上次失败了重试,fix bug
 */
table.prototype.syncData = function (isRetry) {
    if (this.isfetching) {
        return
    }

    //看一下有没有提高优先级的。
    if (global.SYNC_NOW_TABLE && global.SYNC_NOW_TABLE != this.tablename) {

        global.report.reportLog('change', `改变了优先级${this.tablename}->${global.SYNC_NOW_TABLE}`, 'tablestatus')
        this.setStatus(-4);
        return;
    }
    this.setStatus(2)

    var lastupdateTime = this.lastupdatetime || 0

    var updatetimestart = 1

    // 如果这次同步数据用的lastUpdateTime和上次一样
    if (this.lastupdatetime != 0 && this.lastupdatetime == this.lastsyncupdatetime && this.getallpagedata) {
        updatetimestart = this.updatetimestart + this.pagesize
        //如果是重试的，那么updatetimestart要用上次的。
        if (isRetry === true) {
            updatetimestart = this.updatetimestart
        }
    }

    this.updatetimestart = updatetimestart
    this.isfetching = true
    // debug('startSyncData', this.tablename, lastupdateTime, currentstart)
    // 如果不是第一轮拉数据，那么要把上次同步的信息上报

    if (this.turnindex > 0) {
        let reportStr = `${this.busobj.busid};${this.busobj.access_id};${this.tablename};turn:${this.turnindex};turninsert:${this.turninsert};turnupdate:${this.turnupdate};turnnochange:${this.turnnochange};localcount:${this.localcount}:lastupdate:${lastupdateTime}`
        global.report.reportLog('turn', reportStr, 'tablestatus')
    }

    if (!!!isRetry)
        this.turnindex++

    this.turninsert = 0
    this.turnupdate = 0
    this.turnnochange = 0

    rpc.syncData(this.tablename, lastupdateTime, updatetimestart).then((ret) => {
        // 只要成功了，那么errortimes重置为0
        // 上次拉取的时候，lastupdateTime要记录下来
        this.lastsyncupdatetime = lastupdateTime
        return this.syncDataBack(ret)
    }).catch((err) => {
        this.isfetching = false
        console.log(err)
        global.report.reportLog('syncerror', `${this.tablename},${lastupdateTime},${this.syncerrtimes},${err.message}`, 'reporterror')
        logger.l('ERRORSYNC', err.toString())
        this.syncerrtimes++
        // 连续出错5次
        if (this.syncerrtimes == 5) {
            this.setStatus(-7)
        } else {
            // 这一次没拉完，继续下一次             
            this.syncData(true)
        }

    })
}

table.prototype.setLastUpdatetime = function (time, start) {

    if (time != 0) {
        if (this.busobj.busid == 'datacenter' || this.busobj.busid == 'datacenterinit') {
            this.lastupdatetime = new Date(time).valueOf()
        } else {
            if (this.busobj.otherConfig.updatetimeType == "datetime") {
                this.lastupdatetime = new Date(time).valueOf();
            }
            else {
                this.lastupdatetime = time;
            }
        }
    } else this.lastupdatetime = 0
    //  this.nextStart = start || 1
}

// 设置同步数
table.prototype.setSyncCount = function (addCount) {
    this.synccount += addCount
}

// 成功拉取到远程数据
table.prototype.syncDataBack = function (ret) {
    this.isfetching = false

    var retList = ret.vSyncDataList
    debug('syncDataback', ret.vSyncDataList.length)
    // 本次是否拉满全页数据
    this.getallpagedata = retList.length == this.busobj.pagesize
    // debug('syncDataBack dataLIst', this.tablename, retList.length)
    if (retList.length < this.busobj.pagesize) {
        this.syncfetchdone = true
    }

    if (retList.length > 0) {
        var dataList = retList.map((rItem) => {
            return JSON.parse(rItem)
        })
        this.syncerrtimes = 0
        // 返回的数据是ROWS_COUNT，说明没有拉完
        if (retList.length == this.busobj.pagesize) {

            // 本次同步返回了ROWS_COUNT条数据，说明还有数据未完成
            var dataLen = retList.length
            var updateTime = dataList[dataList.length - 1][this.updatetimeField]
            this.setLastUpdatetime(updateTime)
            // this.syncData()
        }

        this.addRowsPending(dataList)
    } else {
        this.setStatus(4)
    }
}

// 不断的加数据
table.prototype.addRowsPending = function (dataList) {
    this.setStatus(3)
    // 当前有多少数据正在处理
    this.upsertingrows = 0
    this.rowsPending = dataList
    this.insertRowsToLocal()
}

// 此处串行插入数据，故意导致同步数据慢，让陈平给钱后优化方案，改并行
table.prototype.insertRowsToLocal = function () {

    // debug('insertRowsToLocal,length', this.rowsPending.length)
    if (this.rowsPending.length > 0) {
        while (this.countinonetime > this.upsertingrows && this.rowsPending.length > 0) {
            let row = this.rowsPending.shift()
            this.insertSingleRow(row)
        }
    }
}

//
table.prototype.insertSingleRow = function (rowData) {

    var rowObj = new row(rowData, this)
    this.upsertingrows++

    rowObj.startUpsert().then((updateType) => {
        this.insertOrUpdateComplete(rowObj)
    }).catch((err) => {

        // 理论上不会进入这个逻辑
        rowObj.updatetype += `;error`
        this.insertOrUpdateComplete(rowObj)
    })
}

// 处理完一条数据了
/**
 * @param  {} updateType:这条数据最后更新的状态。insert(成功插入了),update（成功更新了）,nochange（数据未变化，不用改db）,fail,pluginfail
 */
table.prototype.insertOrUpdateComplete = function (rowObj) {
    // 如果不是成功插入数据，打一条log

    this.upsertingrows--
    let dbUpdateType = rowObj.dbupdatetype

    if (dbUpdateType == 'insert') {
        var newcount = this.localcount + 1
        this.insertcountthistime += 1
        this.turninsert++
        this.setLocalCount(newcount)
    } else if (dbUpdateType == 'update') {
        this.updatecountthistime += 1
        this.turnupdate++
    } else if (dbUpdateType == 'nochange') {
        this.turnnochange++
    }

    var recordError = false
    if (rowObj.errmsg != '' || dbUpdateType == 'fail') {
        recordError = true
        // 此次数据更新失败了,后面数据全部丢掉
        let logStr = `${this.busobj.busid}|${this.busobj.access_id}|${this.tablename}|${rowObj.keyDataStr}|${rowObj.errmsg}|${dbUpdateType}`
        report.reportLog('InsertOrUpdateError', logStr, 'reporterror')
    }
    // 先看一下本地数据是不是都插完了
    if (rowObj.tableobj.datamustinsert && recordError) {
        // 数据中心的同步如果有一条数据插入失败，就把后面所有的数据全部丢掉。

        this.setStatus(-8, `${rowObj.keyDataStr}`)
    } else {
        if (this.rowsPending.length == 0) {
            if (this.upsertingrows == 0) {
                if (this.syncfetchdone) {
                    this.setStatus(4)
                } else {
                    this.syncData()
                }
            }
        } else {
            this.insertRowsToLocal()
        }
    }
}

exports = module.exports = table