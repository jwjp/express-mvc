const express = require('express');
const path = require('path');
const cookieParser = require('cookie-parser');
const session = require('express-session');
const FileStore = require('session-file-store')(session);
const helmet = require("helmet");
const cors = require('cors');
const { accessLogger } = require("./middleware/accessLogger");
const {logger} = require("./modules/logger");
const routerLoader = require("./middleware/routerLoader");
const { rateLimit } = require('express-rate-limit');
const { slowDown } = require('express-slow-down');
const compression = require("compression");
const responseHandler = require("./middleware/responseHandler");
require('dotenv').config();

const app = express();
const isProduction = process.env.NODE_ENV === 'production';
const sessionSecret = process.env.SESSION_SECRET;
const weakSessionSecrets = new Set([
  'YOUR_SESSION_SECRET',
  'change-me-to-a-long-random-secret'
]);

if (isProduction && (!sessionSecret || weakSessionSecrets.has(sessionSecret))) {
  throw new Error('SESSION_SECRET must be set to a strong value in production.');
}

const parseList = (value) => (value || '')
  .split(',')
  .map(item => item.trim())
  .filter(Boolean);

const toPositiveInteger = (value, fallback) => {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
};

const parseTrustProxy = (value) => {
  if (!value) return undefined;
  if (value === 'true') return true;
  if (value === 'false') return false;

  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : value;
};

const allowedOrigins = parseList(process.env.CORS_ORIGINS);
const speedLimiter = slowDown({
  windowMs: 15 * 60 * 1000,
  delayAfter: toPositiveInteger(process.env.SLOW_DOWN_AFTER, 50),
  delayMs: () => toPositiveInteger(process.env.SLOW_DOWN_DELAY_MS, 2000)
});
const rateLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: toPositiveInteger(process.env.RATE_LIMIT, 100),
  standardHeaders: true,
  legacyHeaders: false
});

const trustProxy = parseTrustProxy(process.env.TRUST_PROXY);
if (trustProxy !== undefined) {
  app.set('trust proxy', trustProxy);
}

// 0. Compression
app.use(compression());

// 1. Security
app.use(helmet());
app.use(cors({
  origin: (origin, callback) => {
    if (!origin || allowedOrigins.includes(origin) || (!isProduction && allowedOrigins.length === 0)) {
      return callback(null, true);
    }
    const error = new Error('Not allowed by CORS');
    error.statusCode = 403;
    return callback(error);
  },
  credentials: true
}));

// 2. Parsing
app.use(express.json({ limit: process.env.JSON_LIMIT || '1mb' }));
app.use(express.urlencoded({ extended: false, limit: process.env.FORM_LIMIT || '1mb' }));
app.use(cookieParser());

// 3. Session
app.use(session({
  store: new FileStore({
    retries: 0,
    ttl: 12 * 60 * 60, // 12 hours
  }),
  secret: sessionSecret || 'development-only-session-secret',
  name: process.env.SESSION_NAME || 'sid',
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    secure: isProduction,
    sameSite: isProduction ? 'lax' : 'strict',
    maxAge: 60 * 60 * 1000 * 12, // 12 hours
  }
}));

// TODO: Passport
// app.use(passport.initialize());
// app.use(passport.session());
// TODO: or custom auth
// app.use(authentication);
// app.use(authorization);

// 4. Logger
app.use(accessLogger);

// 5. Limiter
app.use(speedLimiter);
app.use(rateLimiter);

// 6. Response Handler
app.use(responseHandler());

// 7. Static
app.use(express.static(path.join(__dirname, 'view'), { extensions: ['html'] }));

// 8. Controller
routerLoader(path.join(__dirname, 'controller'))(app);

// TODO: - https: nginx
//       - passport
//       - cache

// 9. 404 Error
app.use((req, res, next) => {
  logger.error(`404 Not Found: ${req.originalUrl}`);
  res.error('404 Not Found', 404);
});

// 10. Error
app.use((err, req, res, next) => {
  logger.error(err?.stack);
  const status = err?.statusCode || err?.status || 500;
  const message = status >= 500 && isProduction
    ? 'Internal Server Error'
    : err?.message || 'Internal Server Error';

  if (typeof res.error === 'function') {
    return res.error(message, status);
  }

  return res.status(status).json({
    success: false,
    message
  });
});

module.exports = app;
