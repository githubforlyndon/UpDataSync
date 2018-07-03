/*
各种上报
*/
const rpc = require('./rpc')
var report = {}
report.init = function () {}

// 心跳上报
report.reportActive = function () {
  rpc.reportActive().then((ret) => {

    setTimeout(report.reportActive, 60000)
  }).catch((e) => {
    setTimeout(report.reportActive, 60000)
  })
}


// 日志上报
report.reportLog = function (action, log,logFileName) {
  if (arguments.length > 1) {

  }
  return rpc.reportLog(action, `${global.hostname}|${log}`,logFileName||"report").then((ret) => {
 
    return 1;
 
  })
}

function getStr(s)
{

}

global.report = report;
exports = module.exports = report
