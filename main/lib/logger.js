const fs = require('fs')
var logger = {}
const debug = require('debug')('app:server:' + __filename.replace(__dirname, ''))
const path = require('path')

function getLogStr (arrArgs) {
  var logStr = Array.prototype.map.call(arrArgs, (item) => {
    var str = item
    if (typeof item == 'object') {
      try {
        str = JSON.stringify(item)
      } catch(e) {
        str = 'unkown object'
      }
    }
    return str
  }).join('|')
  return logStr
}
logger.l = function () {
  var filepath = path.resolve('log', new Date().format('yyyy-MM-dd') + '.log')
  var time = new Date().format('yyyy-MM-dd:hh:mm:ss')
  var logStr = Array.prototype.map.call(arguments, (item) => {
    var str = item
    if (typeof item == 'object') {
      try {
        str = JSON.stringify(item)
      } catch(e) {
        str = 'unkown object'
      }
    }
    return str
  }).join('|')

  fs.appendFileSync(filepath, `${time}|${logStr}\r\n`)
}
logger.log = function () {
  logger.writeLog(getLogStr(arguments), 'log')
}
logger.data = function () {
  logger.writeLog(getLogStr(arguments), 'data')
}
logger.error = function () {
  logger.writeLog(getLogStr(arguments), 'error')
}

logger.writeLog = function (logStr, filename) {
  var time = new Date().format('yyyy-MM-dd:hh:mm:ss')


  var filepath = path.resolve(global.appDataPath, 'log', `${new Date().format('yyyy_MM_dd')}_${filename}.log`)
  fse.ensureFileSync(filepath)
  fs.appendFileSync(filepath, `${time}|${logStr}\r\n`)
}

function checkPath () {
  var logpath = path.resolve(process.cwd(), 'log')
  if (!fs.existsSync(logpath)) {
    fs.mkdirSync(logpath)
  }
}

checkPath()

global.logger = logger

exports = module.exports = logger

Date.prototype.format = function (_fmt) {
  var fmt = _fmt || 'yyyy-MM-dd'
  var t = this
  var o = {
    'M+': t.getMonth() + 1, // 月份
    'd+': t.getDate(), // 日
    'h+': t.getHours(), // 小时
    'm+': t.getMinutes(), // 分
    's+': t.getSeconds(), // 秒
    'q+': Math.floor((t.getMonth() + 3) / 3), // 季度
    'S': t.getMilliseconds() // 毫秒
  }
  if (/(y+)/.test(fmt))
    fmt = fmt.replace(RegExp.$1, (t.getFullYear() + '').substr(4 - RegExp.$1.length))
  for (var k in o)
    if (new RegExp('(' + k + ')').test(fmt))
      if (k == 'S') {
        fmt = fmt.replace(RegExp.$1, ('000' + o[k]).substr(('' + o[k]).length))
      }else {
        fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (('00' + o[k]).substr(('' + o[k]).length)))
      }
  return fmt
}
