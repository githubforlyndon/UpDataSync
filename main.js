
const accountHelper = require('./main/lib/accountHelper');
const paramHelper = require('./main/lib/paramHelper');
const rpc = require('./main/lib/rpc');
//const dbMysql = require('./main/lib/dbMysql');
const taskHelper = require('./main/lib/taskHelper');
const logger = require('./main/taf/logger');

// 校验参数
let res=paramHelper.paramCheck();

if(!res) {
    return;
}

// 服务初始化
rpc.init();

// 校验用户
accountHelper.login(config.name, config.pwd).then((acccountInfo) => {
    logger.data.info(`main-21|checkout: login success:${acccountInfo.stAccount.sAccountDesc}  expireTime:${acccountInfo.stAccount.sExpireTime}`)
    // 校验本地数据库
    if (global.BUS.local_db_type === 'oracle') {
        setTimeout(checkout, 10000);
    } else {
        checkout();
    }
}).catch((err) => {
    logger.error.error(err);
})

// 校验客户端数据库后同步优品数据
function checkout(){
    // let mysql=new dbMysql();
    global.BUS.db.checkConnection().then(()=>{
        logger.data.info(`main-36|checkout: Local database connection is passed `);
        // 获取本地数据信息
        global.BUS.db.getTableInfo().then((arrTables) => {
            logger.data.info(`main-38|checkout: The number of local database tables: ${arrTables.length}`);
            // 获取优品同步数据表
            taskHelper.getTableListForRender();
        }).catch((err) => {
            logger.error.error(err);
        })
    });

}




