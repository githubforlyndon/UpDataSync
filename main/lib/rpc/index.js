/**
 * RPC请求一定只能用main process来发出，然后以IPC的形式发送给render process
 * 
 */

var rpc = {}
var Taf = require('@taf/taf-rpc').client
Taf.setProperty('timeout', 30000)

var SyncProxy = require('./proxy/SyncProxy.js').DataSync
var Q = require('q')

const debug = require('debug')('app:rpc')

var ProxyObj


rpc.init = function () {

  rpc.setServerAddress();

}

rpc.setServerAddress = function()
{
  var strAddress =tafServant;
  ProxyObj = Taf.stringToProxy(SyncProxy.DBDataSyncProxy, strAddress)

}

/**
 * 通用rpc请求发送
 * @param  {String} funName
 * @param  {arguments} args
 * @param  {String} retKeyField,jce里面的out变量
 * @param  {} logInfo
 */
rpc.invoke = function (funName, args, retKeyField) {
    if (!ProxyObj) {
    rpc.init()
  }

 // var dfd = Q.defer()
  var promise = new Promise((resolve, reject) => {
    ProxyObj[funName].apply(ProxyObj, args).then(function (rpcRet) {

      // rpcRet.response.return是服务器端current.sendResponse的第一个参数
      var retValue = rpcRet.response.arguments[retKeyField].toObject();
      if (retValue.iRet == 0) {
        resolve(retValue)
      }else {
        //global.logger.l('rpc invoke iRet!=0', funName, retValue)
        reject(new Error(retValue.sMsg || '-1'))
      }
    }).fail(function (err) {
      var msg = ''
      try {
        msg = err.message || err.response.error
        if (typeof msg == 'object') {
          msg = msg.message || JSON.stringify(msg)
        }
      } catch(e) {
        msg = err;
        reject(new Error("RpcException"))
      }

      global.logger.l('rpc error', funName, msg)

      // TODO:不要返回null,通用error对象
      reject(new Error(msg))
    })
  })

  return promise

}

/**
 * 用户输入我们分配的accessid和secret登录到UP服务器
 * @param  {String} accessid
 * @param  {String} secret
 * 
 *  struct LoginReq
  {
  	0 optional string 	sAccessId;		 //access id,类似微信的appid
  	1 optional string		sSecret;		 //密钥
  	2 optional string sIP; //来源IP
	3 optional string sUA;//客户端信息
  }
 */
rpc.login = function (accessid, secret) {
  let loginReq = new SyncProxy.LoginReq()
  loginReq.readFromObject(
    {
      sAccessId: accessid,
      sSecret: secret,
      sUA: global.UA
    }
  )
  //var dfd = Q.defer()
 
  return rpc.invoke('login', [loginReq], 'rsp'); // .then(accountHelper.setAccountInfo)
 
}
 

/*
获取当前账号的tablelist
*/
rpc.getTableList = function () {
  debug('start getTableList')
  var token = global.TOKEN || ''
  return rpc.invoke('getTableList', [token], 'rsp')
}

/*
获取生成创建表的SQL
*/
rpc.getCreateTableSql = function (tablename,sTargetDbType,oracleDbUser,oracleTableSpace) {
  debug('start getCreateTableSql', tablename)
  var token = global.TOKEN || ''
  var req = new SyncProxy.GetCreateTableSqlReq()
 
  req.readFromObject(
    {
      sToken: token,
      sTableName: tablename,
      sAccountDbUser: oracleDbUser || '',
      sAccountDbTablespace: oracleTableSpace || '',
      sTargetDbType:sTargetDbType
    }
  )
  return rpc.invoke('getCreateTableSql', [req], 'rsp')
}

/*
获取生成创建表的SQL
*/
rpc.syncData = function (tablename, updateTime, start) {
  debug('start syncData',tablename,updateTime,start)

  var token = global.TOKEN || ''
  var req = new SyncProxy.SyncTableReq()
  
  req.readFromObject(
    {
      sToken: token,
      sTableName: tablename,
      iUpdatetime: updateTime,
      iStart: start
    }
  )
  return rpc.invoke('syncData', [req], 'rsp')
}
/*
上报心跳
*/
rpc.reportActive = function (action, log) {
  // debug('start syncData',tablename,updateTime,start)
  var token = global.TOKEN || ''
  let req = new SyncProxy.ReportActiveReq();
  let lastLogStr = global.LAST_LOG_STR||"";
  //当前正在处理的table
  let currentTable = global.CURRENTTABLE;
  let lastLogWithTable = lastLogStr;
  if(currentTable)
  {
    lastLogWithTable +=`,${currentTable.tablename},${currentTable.statusname},localcount:${currentTable.localcount},insert:${currentTable.insertcountthistime},update:${currentTable.updatecountthistime}`;

  }
 
  req.readFromObject(
    {
      sUA:global.UA,
      sAccessId:global.AID,
      sLastLog:`${lastLogWithTable}`
    }
  )

  return rpc.invoke('reportActiveNew', [token,req], 'rsp')
}
/*
上报日志
*/
rpc.reportLog = function (action, log,logFileName) {
  // debug('start syncData',tablename,updateTime,start)
  var token = global.TOKEN || ''
  var req = new SyncProxy.ReportLogReq()
  req.readFromObject(
    {
      sAction: action,
      sLog: log,
      sAccessId:global.AID,
      sLogFileName:logFileName||""
    }
  )
  return rpc.invoke('reportLog', [token, req], 'rsp')
}

exports = module.exports = rpc
