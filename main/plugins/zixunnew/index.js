const path = require('path')
const url = require('url')

const fs = require('fs')
const fse = require('fs-extra')
const request = require('request')
var zixunnew = {
  'version': '20170718'
}
var BusObj;
//字段是一张图片的URL
var ImgFields = [];

//字段是HTML，里面有图片需要拉取
var ImgHtmlContentFields = []

var IdField = "id";

//初始化
zixunnew.init = function (busObj) {
  BusObj = busObj;
  if (busObj.otherConfig.imgFields) {
    ImgFields = busObj.otherConfig.imgFields.split(",")
  }
  if (busObj.otherConfig.htmlContentFields) {
    ImgHtmlContentFields = busObj.otherConfig.htmlContentFields.split(",")
  }
  if (busObj.otherConfig.idField) {
    IdField = busObj.otherConfig.idField
  }

  console.log("BusObj", BusObj)

}

// 数据更新前，要做一些操作,暴露给row对象使用(app/main/lib/row.js)
zixunnew.onBeforeUpdateData = function (rowObj) {
  return new Promise((resolve, reject) => {

    downloadAllImgForRowData(rowObj).then((processRet) => {
      resolve(rowObj)
    }).catch((err) => {
      console.log('GetImgError', err)
      resolve(rowObj)
    })
    // 

    // resolve(1)
  })
}

// 获取所有需要下载的图片URL
function getAllImgs(rowObj) {
  let rowData = rowObj.data;
  let arrImgs = [];

  if (ImgHtmlContentFields.length > 0) {
    ImgHtmlContentFields.forEach((htmlField) => {

      if (rowData[htmlField]) {
        let html = rowData[htmlField]
        let reg = /<img\s+src.*=\n*[\\]*["']([\d\w\/\.\:\-\%\?]+?)[\\]*['"]/gi
        var match = reg.exec(html)
        while (match) {
          let orgImgSrc = match[1]
          arrImgs.push({
            field: htmlField,
            orgImgSrc: orgImgSrc
          })
          match = reg.exec(html)
        }
      }

    })
  }


  // 文章的封面
  if (ImgFields.length > 0) {
    ImgFields.forEach((imgField) => {

      if (rowData[imgField]) {

        arrImgs.push({
          field: imgField,
          orgImgSrc: rowData[imgField]
        })

      }
    })

  }

  if (arrImgs.length > 0)
    console.log("arrImgs", arrImgs, rowObj.data.id)

  return arrImgs
}

// 直接先把所有的图片拉取到本地
function downloadAllImgForRowData(rowObj) {
  var promise = new Promise((resolve, reject) => {
    let imgs = getAllImgs(rowObj)

    if (imgs.length > 0) {
      let actions = []
      imgs.forEach((imgItem) => {
        actions.push(saveImgToPath(imgItem, rowObj))
      })
      Promise.all(actions).then((rets) => {
        rets.forEach((downloadInfo) => {
          if (downloadInfo.errmsg) {
            // 如果出错了，不替换HTML
            rowObj.pluginupdatetype = 'pluginerror'
            rowObj.errmsg += downloadInfo.errmsg + ';'

            // 2017-03-21 增加判断 如果出错了(比如超时导致下载资讯图片失败)，分两种情况：
            // 1.如果源路径是绝对路径，不做处理 2.如果源路径是相对路径，修改成绝对路径
            if (downloadInfo.orgImgSrc == downloadInfo.replacedImgPath) { // 表示源路径是相对路径
              let html = rowObj.data[downloadInfo.field]
              html = html.replace(downloadInfo.orgImgSrc, downloadInfo.downloadUrl)
              rowObj.data[downloadInfo.field] = html
            }
          } else {
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
    } else {
      resolve(rowObj)
    }
  })

  return promise
}

// 远端的图片URL，要转换为本地的相对文件路径，存储到用户设置的filepath里面去
function getDownloadImgInfo(imgItem) {
  let orgImgSrc = imgItem.orgImgSrc
  let urlObj = url.parse(orgImgSrc)
  // 如果orgImgSrc='/upload/aaa/aa.png'这种相对地址，说明是 www.upchina.com
  let downloadUrl = orgImgSrc
  let host = urlObj.host
  let replacedImgPath
  if (host) {
    replacedImgPath = urlObj.path
  } else {
    replacedImgPath = orgImgSrc
    host = 'www.upchina.com'
    downloadUrl = `http://${host}${orgImgSrc}`
  }

  let localSavePath = path.resolve(global.USERCONFIG.filepath, replacedImgPath.replace(/^\//, ''))

  //有的图片地址有问号，需要replace掉E:\syncfiles3\News\201710\02dd4dd94b41fbf624e090a3c1a58a58.jpg?__aliup_image_quality_template__
  let posQue = localSavePath.indexOf("?")
  if (posQue != -1) {
    localSavePath = localSavePath.substring(0, posQue)
  }
  return {
    'field': imgItem.field, // 数据表对应的字段
    'orgImgSrc': orgImgSrc, // 原始HTML里面的url
    'downloadUrl': downloadUrl, // 图片完整的URL，带host的
    'localSavePath': localSavePath, // 本地保存的完整路径
    'replacedImgPath': replacedImgPath // 最后替换的本地地址
  }
}

function saveImgToPath(imgItem, rowObj) {
  var promise = new Promise((resolve, reject) => {
    let downloadInfo = getDownloadImgInfo(imgItem)
    let imgDownloadUrl = downloadInfo.downloadUrl

    let fileFolder = path.dirname(downloadInfo.localSavePath)
    try {
      fse.ensureDirSync(fileFolder)
    } catch (e) {

    }

    if (fs.existsSync(downloadInfo.localSavePath)) {
      global.report.reportLog('imgexists', rowObj.tableobj.tablename + ',' + rowObj.keyDataStr + ',' + imgDownloadUrl)
      resolve(downloadInfo)
    } else {
      request.get(imgDownloadUrl, {
        timeout: 20000,
        encoding: null
      }, (err, body, rsp) => {
        if (err || !body) {
          let errmsg = `GetImageError:${err.message}:${imgDownloadUrl}`
          downloadInfo.errmsg = errmsg
        } else {

          if (body.statusCode != 200) {
            downloadInfo.errmsg = `GetImageStatusError:${body.statusCode}:${imgDownloadUrl}`
            // downloadInfo.errmsg = errmsg
          } else {
            // 如果本地文件存在，就不用处理了
            try {
              let fileFolder = path.dirname(downloadInfo.localSavePath)
              fse.ensureDirSync(fileFolder)
              fs.writeFileSync(downloadInfo.localSavePath, rsp)
              global.report.reportLog('fetchimg', rowObj[IdField] + ',' + imgDownloadUrl)
            } catch (e) {
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

exports = module.exports = zixunnew;