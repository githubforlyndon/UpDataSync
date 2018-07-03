// 业务类
const Q = require('q')
const debug = require('debug')('app:client:' + __filename.replace(__dirname, ''))
const path = require('path')
const fs = require('fs')
var dbMysql = require('./dbMysql')
var busHelper = {}
var BusObjs = {}

function bus(access_id, busid, token, desc, server_db_type, local_db_type, attach_type, pagesize, otherConfig) {
  this.access_id = access_id
  this.busid = busid
  this.token = token
  this.desc = desc
  this.serverDbType = server_db_type
  this.local_db_type = local_db_type
  this.attachType = attach_type
  this.pagesize = pagesize; // 服务器一次返回多少条数据

  //对应TAF配置项：let otherConfigObj = copyFields(aObj.bus_obj, ["splitTables", "updatetimeField", "idField", "updateTimeType", "imgFields", "imgContentFields"])

  this.otherConfig = otherConfig;

  this.loadPlugins()
  this.initDb()
}

bus.prototype.initDb = function () {
  if (this.db) {
    return Promise.resolve(1);
  } else {
    if (this.local_db_type == 'mysql') {
      this.db = new dbMysql()
      return this.db.checkConnection()
    } else {
      this.db = new global.DbOracle()
      return this.db.initPool().then(() => {
        return this.db.checkConnection()
      })
      // return this.db.checkConnection()
    }
  }
}


// 检查本地db配置项是否是合理的
bus.prototype.checkDbOption = function () {
  return this.getUserOption().then((userOption) => {
    return this.initDb(userOption)
  })
}


/*
每个业务在某些场景有特殊的逻辑，这个需要通过加载自己业务的插件去实现。
eg:资讯同步业务，同步完数据后，要看BODY字段里面有没有图片，如果有，需要把图片拉取到本地,并且改变数据
*/

bus.prototype.loadPlugins = function () {

  // 如果本地是oracle的话，还要require oracledb模块
  if (this.local_db_type == 'oracle') {
    global.DbOracle = require(path.resolve(__dirname, `./dbOracle.js`))
  }

  // eg:plugins/zixun/是否存在
  let busPluginPath = path.resolve(__dirname, `../plugins/${this.busid}/index.js`)

  if (fs.existsSync(busPluginPath)) {
    this.plugin = require(busPluginPath)
    //如果插件暴露了init方法，那么就调用一下
    if (this.plugin.init) {
      this.plugin.init(this);
    }
  }
}

//为当前登录账号加一个业务实例
busHelper.addBus = (access_id, busid, token, desc, server_db_type, local_db_type, attach_type, pagesize, otherConfig) => {
  var busObj = new bus(access_id, busid, token, desc, server_db_type, local_db_type, attach_type, pagesize, otherConfig)
  BusObjs[busid] = busObj
  return busObj
}

// 获取某个业务的实例
busHelper.getBus = function (busid) {
  return BusObjs[busid]
}

exports = module.exports = busHelper
