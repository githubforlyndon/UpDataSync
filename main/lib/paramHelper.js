
const logger = require('../taf/logger');

var paramHelper = {};


/*
 * 校验启动参数
 * -n用户 -m密码 -h数据库ip  -u数据库用户名  -p数据库密码
 * -o端口号（mysql默认3306）  -d数据库名
 * -f文件地址
 */

paramHelper.paramCheck=function () {
    let paramObj = global.config;
    if(paramObj.name===undefined||paramObj.name===''){
        logger.data.info('paramHelper-17|warning: UpUser name is required, please make sure' );
        return false;
    }
    if(paramObj.pwd===undefined||paramObj.pwd===''){
        logger.data.info('paramHelper-21|warning: UpUser password is required, please make sure');
        return false;
    }
    if(paramObj.host===undefined||paramObj.host===''){
        logger.data.info('paramHelper-25|warning: Database host is required, please make sure');
        return false;
    }
    if(paramObj.uid===undefined||paramObj.uid===''){
        logger.data.info('paramHelper-29|warning: Database uid is required, please make sure');
        return false;
    }
    if(paramObj.dataPwd===undefined||paramObj.dataPwd===''){
        logger.data.info('paramHelper-33|warning: User password is required, please make sure');
        return false;
    }
    if(paramObj.database===undefined||paramObj.database===''){
        logger.data.info('paramHelper-37|warning: Database password is required, please make sure');
        return false;
    }
    if(paramObj.filepath===undefined||paramObj.filepath===''){
        logger.data.info('paramHelper-41|warning: Filepath is required, please make sure');
        return false;
    }
    if(paramObj.port===undefined||paramObj.port===''){
        logger.data.info('paramHelper-45|warning: Port is required, please make sure');
        return false;
    }

    return true;


}



exports = module.exports = paramHelper