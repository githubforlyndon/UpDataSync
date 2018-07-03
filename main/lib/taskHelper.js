var taskHelper = {}
var path = require('path')
var Q = require('q')
const rpc = require('./rpc')
const report = require('./report')
var table = require('./table')
const logger = require('./logger')
const tafLogger = require('../taf/logger')
const debug = require('debug')('app:server:' + __filename.replace(__dirname, ''))
var ListObj = {}
var ArrList = [];
const ReloadTableInterval = 3600000;

const EventEmitter = require('events')

class TaskEmitter extends EventEmitter { }

const tEmitter = new TaskEmitter()

// 某个数据表的任务完成了或者失败了，那么可以处理下一个表了
tEmitter.on('tableTaskDone', (t) => {
    debug('tableTaskDone', t.tablename)
    taskHelper.doTask()
})

/*
-1:建表中
0:第一次初始化数据
1：正常同步中
*/
const E_STATUS = {
    '-3': '数据准备中',
    '-2': '初始化建表中',
    '-1': '暂停同步中',
    '0': '第一次初始化数据',
    '1': '正常同步中',
    '2': '正常同步中'

}
var TASK_STATUS = -3

taskHelper.setStatus = function (status) {
    TASK_STATUS = status
    var statusname = E_STATUS[status]
}

//马上同步这个表
taskHelper.syncNow = function (tname) {
    global.SYNC_NOW_TABLE = tname;
    TASK_STATUS = 2;
}
// 执行下一个任务
taskHelper.doTask = function () {
    tafLogger.data.info(`taskHelper-55|TASK_STATUS: now task status is ${TASK_STATUS}`);

    if (TASK_STATUS == -3) {
        ArrList.forEach((item) => {
            item.setStatus(item.status)
        })
        // TASK_STATUS = -2
        taskHelper.setStatus(-2)
        taskHelper.doTask()
    } else if (TASK_STATUS == -2) {
        taskHelper.createAllTableForLocal()
    } else if (TASK_STATUS == 0) {
        // 建表完成，获取所有表的本地记录数
        taskHelper.getAllTableRowCount()
    } else if (TASK_STATUS == 1) {

        taskHelper.startTableSyncForFirstTime()
    } else if (TASK_STATUS == 2) {
        // 所有的数据表，第一次初始化都完毕了，
        taskHelper.startNormalSync()
    }
}

// 获取所有表的本地数据量，1.可以知道哪些表的数据量是0，2：展示用
taskHelper.getAllTableRowCount = function () {

    taskHelper.setStatus(1)
    taskHelper.doTask()
    return;

}



taskHelper.getTableListForRender = function () {
    // render进程getTableList事件对象，这样可以持续发事件响应,就算页面刷新，主进程内存的数据可以复用
    report.reportActive()

    // 内存中没有，那么需要RPC请求远端server
    if (ArrList.length == 0) {
        rpc.getTableList().then((ret) => {
            //debug('getTableListback', ret)
            if (ret.iRet == 0) {
                taskHelper.setAccountTableList(ret.vTableList)
                taskHelper.sendTableList()

            } else {
                taskHelper.sendTableList(ret.sMsg)
            }
            setTimeout(taskHelper.reloadUserTablesFromServer, ReloadTableInterval)
        }).catch((err) => {
            logger.l('getTableList error', err)
            taskHelper.sendTableList(err.message)
        })
    } else {
        taskHelper.sendTableList()
    }
}

/**
 * @param  {String} err_msg,不传就代表正常发送
 */
taskHelper.sendTableList = function (err_msg) {
    if (err_msg) {
        tafLogger.data.info(`taskHelp-121|err_msg: ${err_msg}`)
    } else {
        // 正常发送
        tafLogger.data.info(`taskHelp-124|getTableList:tableNum: ${ArrList.length}`)
        taskHelper.doTask()
    }
}


// 给所有数据表拉取创建表的SQL
taskHelper.createAllTableForLocal = function () {
    var arrNoLocalTableList = ArrList.filter((item) => {
        return item.status == -1
    })


    if (arrNoLocalTableList.length > 0) {
        var item = arrNoLocalTableList[0]
        item.myTask()

    } else {
        taskHelper.setStatus(0)
        taskHelper.doTask()
    }
}

// 开始正常的数据同步 
taskHelper.startNormalSync = function () {


    var arrListSort = null;
    var tObjSync = null;
    if (global.SYNC_NOW_TABLE) {
        for (var i = 0; i < ArrList.length; i++) {
            if (ArrList[i].tablename == global.SYNC_NOW_TABLE) {
                tObjSync = ArrList[i];
            }

        }


        global.SYNC_NOW_TABLE = null;
    } else {
        arrListSort = ArrList.filter((rItem) => {
            return (rItem.inexttasktime > 0 && rItem.inexttasktime < Date.now().valueOf()) || rItem.status == -4||rItem.status==-6
        }).sort((a, b) => {

            return a.inexttasktime - b.inexttasktime
        })

        if (arrListSort.length > 0) {
            tObjSync = arrListSort[0]
        }
    }



    if (tObjSync) {
        tObjSync.myTask()
    } else {
        // 15秒后自动执行
        tafLogger.data.info(`taskHelper-183|notask: No task now,A new round of synchronization tasks after 15 seconds`)
        setTimeout(taskHelper.startNormalSync, 15000)
    }
}
// 应用启动后从来没有同步过，开始同步数据
taskHelper.startTableSyncForFirstTime = function () {
    debug('startTableSyncForFirstTime')
    // 先把记录数为0的并且还在排队的，状态置为  '6': '等待第一次初始化数据',
    ArrList.forEach((tItem) => {
        if ((tItem.status == -6 && tItem.localcount == 0) || tItem.status == 5) {
            tItem.setStatus(6)
        }
    })

    var has6 = taskHelper.existStatus(6)
    var has_6 = taskHelper.existStatus(-6)
    // 先处理从来没有拉过数据的。
    if (has6)
        taskHelper.processAsTableStatus(6)
    // 处理排队中的
    else if (has_6)
        taskHelper.processAsTableStatus(-6)
    else {
        // 6,-6都没有了。
        taskHelper.setStatus(2)
        taskHelper.doTask()
    }
}

taskHelper.existStatus = function (status) {
    var arrQuene = ArrList.filter((item) => {
        return item.status == status
    })

    return arrQuene.length > 0
}

taskHelper.processAsTableStatus = function (status) {
    var arrQuene = ArrList.filter((item) => {
        return item.status == status
    })

    if (arrQuene.length > 0) {
        var t = arrQuene[0]
        t.myTask()
    } else {
        taskHelper.doTask()
    }
}


/**
 * @desc 根据item增加一个table实例
 */
taskHelper.addTable = function (item,status) {

    var status = status||-1; // -1:本地没有数据表，0：本地有数据表
    var localCount = 0; // 本地数据表条数
    var tablename = item.sTableName;
    var interval = parseInt(item.iInterval, 10) || 60

    let arrLocalTables = global.tables;
    // mysql-to-oracle时，源mysql表一般是小写，目的oracle表默认是大写，因此增加toUpperCase的判断
    let hasLocalTable = arrLocalTables.indexOf(tablename) != -1 || arrLocalTables.indexOf(tablename.toLowerCase()) != -1 ||
       arrLocalTables.indexOf(tablename.toUpperCase()) != -1;
    if (hasLocalTable) {

        status = -6; // 排队中
        localCount = 0
    }
    let bus = global.BUS
    var t = new table(bus, tablename, item.sTableCName, status, localCount, 0, interval)
    t.E = tEmitter;

    ListObj[tablename] = t

    ArrList.push(t)
}

taskHelper.setAccountTableList = function (accountTableList) {
    ListObj = {}
    ArrList = []
    accountTableList.forEach((item, index) => {
        taskHelper.addTable(item)

    })
}

/**
 * @desc 重新加载远端数据表。因为用户的表可以会发生变化(特别是按月分表的那种),或者是配置发生变化（例如频率）
 */
taskHelper.reloadUserTablesFromServer = () => {
    rpc.getTableList().then((ret) => {
        if (ret.iRet == 0) {
            let isTableListChanged = false;

            const arrNewTables = ret.vTableList;
            let setNewTables = new Set();
            arrNewTables.forEach((item) => {
                setNewTables.add(item.sTableName);
            });

            //先遍历一下新拉回来的授权数据表，看一下哪些有更新频率或者新增加的表
            arrNewTables.forEach((aItem) => {
                var tablename = aItem.sTableName;
                //表已经存在
                if (ListObj[tablename]) {
                    let interval = parseInt(aItem.iInterval, 10) || 60;
                    if (interval != ListObj[tablename].interval) {
                        tafLogger.data.info(`taskHelper-293|taskLog: ${tablename}表频率由${ListObj[tablename].interval}秒变为${interval}秒`);
                        ListObj[tablename].setInterval(interval);
                    }
                }
                else {
                    //本地没有这个表的权限，说明是给账号新授权的表
                    tafLogger.data.info(`taskHelper-298|taskLog: 新授权了一张表:${tablename}`);
                    // aItem.status = -9;
                    taskHelper.addTable(aItem,-9)
                    isTableListChanged = true;
                }
            })


            //遍历本地
            for (var tablename in ListObj) {
                if (setNewTables.has(tablename) === false) {
                    isTableListChanged = true;
                    tafLogger.data.info(`taskHelper-311|taskLog: 服务器停止了${tablename}的同步` );
                    //从数组中删除掉这个元素
                    for (var i = 0; i < ArrList.length; i++) {
                        if (tablename == ArrList[i].tablename) {
                            ArrList.splice(i, 1);
                            break;
                        }
                    }
                    delete ListObj[tablename]
                }
            }

            if (isTableListChanged) {
                tafLogger.data.info(`taskHelper-324|taskLog: tableNum改变了,现在数据 ${ArrList.length}`)
            }



        } else {

        }
        setTimeout(taskHelper.reloadUserTablesFromServer, ReloadTableInterval)

    }).catch((err) => {
        setTimeout(taskHelper.reloadUserTablesFromServer, ReloadTableInterval)
    })
}

// 服务器的数据返回后的处理
taskHelper.processServerData = function (dataList) {
    var dfd = Q.defer()

    return dfd.promise
}

taskHelper.getList = function () {
    return ArrList
}

exports = module.exports = taskHelper