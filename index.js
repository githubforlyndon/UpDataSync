
const co = require('co'),
    path = require('path'),
    configHelper = require('@taf/taf-config-helper').Helper,
    logger = require('./main/taf/logger');

const fileName = 'UpDataSync.conf';

co(function* () {
    const conf = yield configHelper.getConfig({
        fileName: fileName,
        path: path.resolve(__dirname, './' + fileName),
    });
    if (conf.iRet === configHelper.E_RET.OK) {
        const configJson = JSON.parse(conf.data);
        global.config = configJson.options;
        global.tafServant = configJson.server;
        logger.data.info('成功获取配置文件', conf.data);
    } else {
        logger.error.error('获取配置文件失败', conf);
    }
    require('./main');
}).catch(err => {
    logger.error.error('获取配置文件失败', err);
});