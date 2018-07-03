
var rpc = require("./rpc");
var accountHelper = {};
var busHelper = require("./busHelper");
const debug = require('debug')('app:client:' + __filename.replace(__dirname, ''))
const tafLogger = require('../taf/logger')

/*
 *理论上一个account可以有多个业务
 */

accountHelper.login = function (access_id, secret) {

    return rpc.login(access_id, secret).then((acccountInfo) => {
        debug("acccountInfo", acccountInfo);
        let busid = acccountInfo.stBus.sBusId;
        let server_db_type = acccountInfo.stBus.sDbType;
        let local_db_type = acccountInfo.stAccount.sLocalDbType;
        let attach_type = acccountInfo.stBus.iAttachType;
        let token = acccountInfo.sToken;
        let desc = acccountInfo.stBus.sDesc;
        let pagesize = acccountInfo.stBus.iPageSize;
        let otherConfig = {};
        try {
            otherConfig = JSON.parse(acccountInfo.stBus.sOtherBusConfig);
        } catch (err) {
            tafLogger.data.info(err);
        }


        let busObj = busHelper.addBus(access_id, busid, token, desc, server_db_type, local_db_type, attach_type, pagesize, otherConfig);
        //暂时只支持一个业务,放到global下面，后面也可以比较方便的支持多业务
        global.BUS = busObj;

        global.TOKEN = token;

        return acccountInfo;
    })

}


exports = module.exports = accountHelper
