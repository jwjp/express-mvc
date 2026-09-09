// modules/logger.js
const winston = require('winston');
const winstonDaily = require('winston-daily-rotate-file');
const path = require('path');
const loggerCache = new Map();

// logger
const logger = winston.createLogger({
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.printf(info => `${info.timestamp} ${info.level.toUpperCase()}: ${info.message}`),
  ),
  transports: [
    // console logging (for PM2)
    new winston.transports.Console(),

    // normal log setting
    new winstonDaily({
      level: 'info',
      datePattern: 'YYYY-MM-DD',
      dirname: 'logs',
      filename: '%DATE%.log',
      maxFiles: '14d',      // 14 days
      zippedArchive: true   // older files are compressed
    }),

    // error log setting
    new winstonDaily({
      level: 'error',
      datePattern: 'YYYY-MM-DD',
      dirname: 'logs',
      filename: '%DATE%.error.log',
      maxFiles: '14d',
      zippedArchive: true
    })
  ]
});

// create logger with file
const createLogger = (folder) => {
  if (loggerCache.has(folder)) {
    return loggerCache.get(folder);
  }

  const scopedLogger = winston.createLogger({
    format: winston.format.combine(
      winston.format.timestamp({
        format: 'YYYY-MM-DD HH:mm:ss'
      }),
      winston.format.printf(info => `[${info.timestamp}] ${info.message}`)
    ),
    transports: [
      new winstonDaily({
        dirname: path.join(process.cwd(), 'logs', folder),
        filename: `${folder}_%DATE%.log`,
        datePattern: 'YYYYMMDD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d'
      })
    ]
  });

  loggerCache.set(folder, scopedLogger);
  return scopedLogger;
};

module.exports = { logger, createLogger };
