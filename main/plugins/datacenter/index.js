 
const path = require('path')
const url = require('url')

const fs = require('fs')
const fse = require('fs-extra')
const request = require('request')
var datacenter = {
  "version":"20161230001"
}
 

// 数据更新前，要做一些操作,暴露给row对象使用(app/main/lib/row.js)
datacenter.onBeforeUpdateData = function (rowObj) {
  return new Promise((resolve, reject) => {
    //表示配置了附件存储路径（服务器端配置了attachtype=1，客户端才会出现附件存储路径配置界面），附件下载插件才起作用；
    //否则跳过附件下载直接同步数据
    if(global.USERCONFIG.filepath){//filepath是客户端配置的存储路径
      downloadAllFilesForRowData(rowObj).then((processRet) => {
        resolve(rowObj)
      }).catch((err) => {
        console.log('GetImgError', err)
        resolve(rowObj)
      })
    }else{
      resolve(rowObj)
    }
  })
}

// 获取所有需要下载的附件URL
function getAllFilesUrl (rowObj) {
  let rowData = rowObj.data
  let arrImgs = []

//动态读取附件字段，用正则表达式匹配（这里匹配以'/'开头的字段内容）
  for (let prop in rowData) {
    let reg=new RegExp("^\/.{0,}");
    let val = rowData[prop]
    let match = reg.exec(val)
    if(match){
      let orgImgSrc = match[0]
      arrImgs.push({field: prop,orgImgSrc: orgImgSrc})
    }
   //console.log("rowData." + prop + " = " + rowData[prop]);
  }

  return arrImgs
}

// 根据附件字段对应的目录将远端附件下载到本地
function downloadAllFilesForRowData (rowObj) {
  var promise = new Promise((resolve, reject) => {
    let imgs = getAllFilesUrl(rowObj)

    if (imgs.length > 0) {
      let actions = []
      imgs.forEach((imgItem) => {
        actions.push(saveFileToPath(imgItem,rowObj))
      })
      Promise.all(actions).then((rets) => {
        rets.forEach((downloadInfo) => {
          if (downloadInfo.errmsg) {
            //如果出错了，不替换HTML
            rowObj.pluginupdatetype = 'pluginerror'
            rowObj.errmsg += downloadInfo.errmsg + ';'
          }else {
            // 看是否需要替换html里面的img src
            if (downloadInfo.replacedImgPath != downloadInfo.orgImgSrc) {
              let html = rowObj.data[downloadInfo.field]
              html = html.replace(downloadInfo.orgImgSrc, downloadInfo.replacedImgPath)
              rowObj.data[downloadInfo.field] = html
            }
          }
        })
        resolve(rowObj)
      }).catch((err) => {
        console.log('err', err)
        resolve(rowObj)
      })
    }else {
      resolve(rowObj)
    }
  })

  return promise
}

// 远端的图片URL，要转换为本地的相对文件路径，存储到用户设置的filepath里面去
function getDownloadFileInfo (imgItem) {
  let orgImgSrc = imgItem.orgImgSrc
  let urlObj = url.parse(orgImgSrc)
  // 如果orgImgSrc='/upload/aaa/aa.png'这种相对地址，说明是 www.upchina.com
  let downloadUrl = orgImgSrc
  let host = urlObj.host
  let replacedImgPath
  if (host) {
    replacedImgPath = urlObj.path
  }else {
    replacedImgPath = orgImgSrc
    host = 'attach.upchinapro.com'
    downloadUrl = `http://${host}${orgImgSrc}`
  }
  
  let localSavePath = path.resolve(global.USERCONFIG.filepath, replacedImgPath.replace(/^\//, ''))
  return {
    'field': imgItem.field, // 数据表对应的字段
    'orgImgSrc': orgImgSrc, // 原始HTML里面的url
    'downloadUrl': downloadUrl, // 图片完整的URL，带host的
    'localSavePath': localSavePath, // 本地保存的完整路径
    'replacedImgPath': replacedImgPath // 最后替换的本地地址
  }
}

// function saveFileToPath (imgItem,rowObj) {
//   var promise = new Promise((resolve, reject) => {
//     let downloadInfo = getDownloadFileInfo(imgItem)
//     let imgDownloadUrl = downloadInfo.downloadUrl

//     request.get(imgDownloadUrl, {timeout: 20000,encoding: null}, (err, body, rsp) => {
//       //判断远端附件服务器上是否有该附件（返回状态码404表示附件没找到）
//       if(body.statusCode == 404){
//         console.log('远程服务器上没有相对应的附件!',body.statusCode,body.statusMessage);
//       }else{
//         if (err) {
//           console.log('GetFileError', imgDownloadUrl)
//           let errmsg = `GetImageError:${err.message}:${imgDownloadUrl}`
//           downloadInfo.errmsg = errmsg
//         }else {
//           console.log('write path', imgDownloadUrl, downloadInfo.localSavePath)
//           // 如果本地文件存在，就不用处理了
//           try {
//             let fileFolder = path.dirname(downloadInfo.localSavePath)
//             fse.ensureDirSync(fileFolder)
//             if (fs.existsSync(downloadInfo.localSavePath) == false) {
//               fs.writeFileSync(downloadInfo.localSavePath, rsp)
//             }
//             global.report.reportLog("fetchimg1",rowObj.keyDataStr,imgDownloadUrl)
//           } catch(e) {
//             downloadInfo.errmsg = e.message
//           }
        
//         }
//       }
//         resolve(downloadInfo)
      
//     })
//   })

//   return promise
// }

function saveFileToPath (imgItem, rowObj) {
  var promise = new Promise((resolve, reject) => {
    let downloadInfo = getDownloadFileInfo(imgItem)
    let imgDownloadUrl = downloadInfo.downloadUrl

    let fileFolder = path.dirname(downloadInfo.localSavePath)
    fse.ensureDirSync(fileFolder)
    if (fs.existsSync(downloadInfo.localSavePath)) {
      global.report.reportLog('fileexists', rowObj.tableobj.tablename + ',' + rowObj.keyDataStr + ',' + imgDownloadUrl)
      resolve(downloadInfo)
    }else {
      request.get(imgDownloadUrl, {timeout: 20000,encoding: null}, (err, body, rsp) => {
        
          if (err) {
            let errmsg = `GetFileError:${err.message}:${imgDownloadUrl}`
            downloadInfo.errmsg = errmsg
          }else {
            if(body.statusCode == 404){//判断远端附件服务器上是否有该附件（返回状态码404表示附件没找到）
              console.log('远程服务器上没有相对应的附件!',body.statusCode,body.statusMessage);
            }else{
              // 如果本地文件存在，就不用处理了
              try {
                let fileFolder = path.dirname(downloadInfo.localSavePath)
                fse.ensureDirSync(fileFolder)
                fs.writeFileSync(downloadInfo.localSavePath, rsp)
                global.report.reportLog('fetchFile', rowObj.keyDataStrr + ',' + imgDownloadUrl)
              } catch(e) {
                downloadInfo.errmsg = e.message
              }
            }
         }
        resolve(downloadInfo)
      })
    }
  })

  return promise
}

exports = module.exports = datacenter
